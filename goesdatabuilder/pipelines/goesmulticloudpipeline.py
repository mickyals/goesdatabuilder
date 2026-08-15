import logging
import shutil
import warnings
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm import tqdm

from goesdatabuilder.data.goes import multicloudconstants as multicloudconstants
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
from goesdatabuilder.store.datasets import GOESZarrStore
from goesdatabuilder.store.zarrstore import ArrayPresetLike
from goesdatabuilder.utils.config import ConfigDefault, ConfigMixin

logger = logging.getLogger(__name__)


class GOESPipelineOrchestrator(ConfigMixin):
    """
    Orchestrates end-to-end GOES processing pipeline from raw files to CF-compliant Zarr.

    Pipeline Flow:
    --------------
    1. [Optional] Catalog: Index files offline (GOESMetadataCatalog)
    2. Load: Open files with Dask (GOESMultiCloudObservation)
    3. Regrid: Transform to lat/lon grid (GeostationaryRegridder)
    4. Store: Write to Zarr with CF metadata (GOESZarrStore)

    Configuration:
    --------------
    - config:
        - GOESMultiCloudObservation setup (data access, regridding)
        - GOESZarrStore setup (storage, compression, metadata, platforms, bands)
        - Orchestration (catalog, error handling, checkpoints, logging)

    Usage:
    ------
        from goesdatabuilder import set_config, GOESPipelineOrchestrator
        set_config("/path/to/config/file.yaml")
        pipeline = GOESPipelineOrchestrator()
        pipeline.process_all()
        pipeline.finalize()
    """

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(self, lazy_init: bool = False) -> None:
        """
        Initialize pipeline orchestrator.

        Parameters
        ----------
            data_access: dictionary containing configuration settings for data access
            regridding: dictionary containing configuration settings for regridding
            store: dictionary containing configuration settings for zarr storage
            goes: dictionary containing configuration settings for goes data
            pipeline: dictionary containing configuration settings for pipeline execution
        """
        # Setup logging first
        self._setup_logging()
        # Components (initialized lazily)
        self._observation = None
        self._regridder = None
        self._store = None

        if not lazy_init:
            # pre-initialize components (recommended)
            self.observation
            self.regridder
            self.store

        # Processing state
        self._processed_count = 0
        self._skipped_count = 0
        self._failed_count = 0
        self._failed_indices = []
        self._last_processed_idx = -1
        self._start_time = None

        logger.debug("Pipeline orchestrator initialized")

    ############################################################################################
    # PROPERTIES
    ############################################################################################

    @property
    def observation(self) -> GOESMultiCloudObservation:
        """Lazily load and return the observation."""
        if self._observation is None:
            self.initialize_observation()
        return self._observation

    @property
    def regridder(self) -> GeostationaryRegridder:
        """Lazily load and return the regridder."""
        if self._regridder is None:
            self.initialize_regridder()
        return self._regridder

    @property
    def store(self) -> GOESZarrStore:
        """Lazily load and return store."""
        if self._store is None:
            self.initialize_store()
        return self._store

    @property
    def is_initialized(self) -> bool:
        """True if all core components are initialized."""
        return self._observation is not None and self._regridder is not None and self._store is not None

    @property
    def total_observations(self) -> int:
        """Total number of timesteps in dataset."""
        return len(self.observation.nc_files)

    @property
    def processed_count(self) -> int:
        """Number of successfully processed observations."""
        return self._processed_count

    @property
    def failed_count(self) -> int:
        """Number of failed observations."""
        return self._failed_count

    @property
    def skipped_count(self) -> int:
        """Number of skipped observations."""
        return self._skipped_count

    @property
    def success_rate(self) -> float:
        """Fraction of unique observations processed successfully."""
        total_unique = self._processed_count + len(set(self._failed_indices))
        if total_unique == 0:
            return 0.0
        return self._processed_count / total_unique

    @property
    def processing_state(self) -> dict[str, Any]:
        """
        Comprehensive processing state for checkpointing.

        Returns
        -------
            Dictionary with processing statistics
        """
        state = {
            "processed_count": self._processed_count,
            "failed_count": self._failed_count,
            "failed_indices": self._failed_indices.copy(),
            "last_processed_idx": self._last_processed_idx,
            "start_time": self._start_time.isoformat() if self._start_time else None,
            "success_rate": self.success_rate,
        }

        # Add elapsed time if processing has started
        if self._start_time:
            elapsed = datetime.now(UTC) - self._start_time
            state["elapsed_seconds"] = elapsed.total_seconds()

        return state

    ############################################################################################
    # COMPONENT INITIALIZATION
    ############################################################################################

    def initialize_observation(
        self,
    ) -> GOESMultiCloudObservation:
        """
        Initialize GOESMultiCloudObservation.

        Returns
        -------
            GOESMultiCloudObservation instance
        """
        logger.info("Initializing observation...")

        # Note the chunk_size is automatically set to -1 for the pipeline code because the
        # regridder will have to un-chunk it later on anyway.
        kwargs = {**self._config["data_access"], "chunk_size": -1}
        self._observation = GOESMultiCloudObservation(**kwargs)

        # Determine available bands by checking which CMI variables exist

        logger.info(f"Observation initialized: {len(self._observation.nc_files)} timesteps")

        return self._observation

    def initialize_regridder(
        self, weights_dir: str | Path | None = ConfigDefault("regridding", "weights_dir")
    ) -> GeostationaryRegridder:
        """
        Initialize GeostationaryRegridder.

        Parameters
        ----------
            target_grid: Optional explicit {'lat': array, 'lon': array}

        Returns
        -------
            GeostationaryRegridder instance
        """
        logger.info("Initializing regridder...")

        if weights_dir:
            try:
                self._regridder = GeostationaryRegridder.from_weights(weights_dir)
            except FileNotFoundError as e:
                raise ValueError(
                    "No cached weights found for the regridder. Try rebuilding the weights or specify a different location"
                ) from e
        else:
            self._regridder = GeostationaryRegridder.from_observation(self.observation.first)

        logger.info(
            f"Regridder initialized: "
            f"source={self._regridder.source_shape}, "
            f"target={self._regridder.target_shape}, "
            f"coverage={self._regridder.coverage_fraction:.1%}, "
            f"cached={self._regridder.has_cached_weights}"
        )

        return self._regridder

    def initialize_store(
        self,
        bands: list[int] = multicloudconstants.ALL_BANDS,
        presets: dict[str, ArrayPresetLike] = ConfigDefault("store", "presets"),
        global_metadata: dict[str, Any] = ConfigDefault("store", "global_metadata"),
    ) -> GOESZarrStore:
        """
        Initialize GOESZarrStore.

        Store path resolution and type dispatch are handled entirely by
        ZarrStoreBuilder._resolve_store. The orchestrator only resolves
        semantic parameters (region, bands) and validates inputs before
        constructing anything.

        Parameters
        ----------
            store_path: Path or URL to Zarr store (overrides store_config).
                        Env vars expanded by _resolve_store.
            overwrite: Overwrite existing store
            region: Region to initialize (overrides default from platforms)
            bands: Bands to initialize (overrides store_config bands)
            presets: array storage configuration.

        Returns
        -------
            GOESZarrStore instance
        """
        logger.info("Initializing Zarr store...")

        # --- Resolve all parameters before constructing anything ---

        region = self.observation.first.ds.attrs["orbital_slot"]

        # Guard against silent replacement of an open store
        if self._store is not None:
            logger.warning(f"Reinitializing store (previous store at {self._store.store_path} will no longer be used)")
            self._store.close_store()

        # --- All inputs resolved, construct and initialize ---

        self._store = GOESZarrStore(
            store_path=self._config["store"]["path"],
            store_type=self._config["store"]["type"],
            object_store_backend=self._config["store"]["object_store_backend"],
            storage_options=self._config["store"]["storage_options"],
        )
        global_attrs = {k: v[0] for k, v in self.observation.to_computed_attrs().items()}
        self._store.initialize_store(global_metadata={**global_attrs, **global_metadata})

        if not self._store.group_exists(region):
            self._store.initialize_region(
                lat=self.regridder.target_lat,
                lon=self.regridder.target_lon,
                presets=presets,
                bands=bands,
                include_dqf=True,
                provenance=self.regridder.regridding_provenance(),
                observation=self.observation,
            )

        logger.info(f"Store initialized at {self._store.store_path}, region={region}, bands={bands}")

        return self._store

    ############################################################################################
    # PROCESSING - SINGLE OBSERVATION
    ############################################################################################

    def _should_store_data(self, path: str, store_idx: int, overwrite: bool, timestamp: np.datetime64) -> bool:
        """Return True if data should be stored."""
        if overwrite or self.store.is_empty(path, store_idx):
            return True
        logger.debug(f"Data already exists at {path} for index {store_idx}. Skipping ...")
        return False

    def _preallocate_zarr_space_for_single_observation(
        self, store_idx: int | None, region: str, bands: list[int]
    ) -> None:
        arrays = {band_name for band in bands for band_name in (f"CMI_C{band:02d}", f"DQF_C{band:02d}")} | {
            "time",
            "platform_id",
            "scan_mode",
        }
        for name, array in self.store.get_group(region).arrays():
            if name in arrays and (store_idx is None or store_idx >= array.shape[0]):
                new_shape = (array.shape[0] + 1,) + array.shape[1:]
                logger.debug(f"Resizing array {name} to {new_shape}")
                array.resize(new_shape)

    def process_single_observation(
        self,
        time_idx: int,
        bands: list[int] = multicloudconstants.ALL_BANDS,
        workers: int = ConfigDefault("pipeline", "worker_threads"),
        max_retries: int = ConfigDefault("pipeline", "error_handling", "max_retries"),
        overwrite: bool = ConfigDefault("pipeline", "overwrite"),
    ) -> int:
        """
        Process single observation (one timestep).

        Uses get_cmi(band) and get_dqf(band) directly rather than the stateful
        .band setter, which is cleaner in a loop. Uses isel_time(idx) which is
        the actual GOESMultiCloudObservation API (not .isel()).

        Parameters
        ----------
            time_idx: Index into observation.time
            bands: Bands to process (default from store_config)
            region: Target region (default from platforms[0])
            workers: Number of simultaneous threads used to process the bands (maximum is the number of bands)

        Returns
        -------
            Store time index where data was written
        """
        observation = self.observation[time_idx]

        region = observation.ds.attrs["orbital_slot"]

        # Extract metadata first (cheap, fails fast if dataset is malformed)
        timestamp = observation.time.isel(time=0).values

        store_idx = self.store.get_time_index(region, timestamp)

        with warnings.catch_warnings():
            # ignore warning that U3 and U10 are unsupported zarr dtypes
            warnings.simplefilter("ignore")
            # pre-allocate the space so that if anything fails during the storage then all
            # arrays should still have the same size for axis=0
            self._preallocate_zarr_space_for_single_observation(store_idx, region, bands)

        if store_idx is None:
            store_idx = -1  # now refers to the last index on axis=0

        # Define some helper functions to simplify checking if/how data should be written to arrays
        store_data = partial(self.store.write_array, selection=(store_idx,))
        should_store_data = partial(
            self._should_store_data, store_idx=store_idx, overwrite=overwrite, timestamp=timestamp
        )

        any_processed = [False]  # mutable type so it can be updated from within one of the functions below

        def store_func(path: str, data: Any) -> None:
            if should_store_data(path):
                store_data(path, data)
                any_processed[0] = True

        platform_id = str(observation.ds["platform_id"].values[0])
        scan_mode = str(observation.ds["scan_mode"].values[0]) if "scan_mode" in observation.ds else "unknown"

        store_func(f"{region}/time", timestamp)

        with warnings.catch_warnings():
            # ignore warning that U3 and U10 are unsupported zarr dtypes
            warnings.simplefilter("ignore")
            store_func(f"{region}/platform_id", platform_id)
            store_func(f"{region}/scan_mode", scan_mode)

        # Regrid CMI and DQF for each band

        def regrid(band: str, retries_remaining: int = max_retries) -> tuple[str, np.ndarray, np.ndarray]:
            logger.debug("processing band %s of timestep %s", band, time_idx)
            try:
                cmi_path = f"{region}/CMI_C{band:02d}"
                if should_store_data(cmi_path):
                    cmi_2d = observation.get_cmi(band).isel(time=0)
                    cmi_regridded_3d = self.regridder.regrid(cmi_2d).values
                    store_data(cmi_path, cmi_regridded_3d)
                    del cmi_2d, cmi_regridded_3d
                    any_processed[0] = True

                dqf_path = f"{region}/DQF_C{band:02d}"
                if should_store_data(dqf_path):
                    dqf_2d = observation.get_dqf(band).isel(time=0)
                    dqf_regridded_3d = self.regridder.regrid_dqf(dqf_2d).values
                    store_data(dqf_path, dqf_regridded_3d)
                    del dqf_2d, dqf_regridded_3d
                    any_processed[0] = True

            except Exception as e:
                if retries_remaining > 0:
                    retries_remaining -= 1
                    logger.info(f"retrying regridding and storing band {band}. {retries_remaining} attempts remaining.")
                    return regrid(band, retries_remaining=retries_remaining)
                else:
                    raise Exception(f"error regridding and storing band {band}") from e

            return band

        if workers == 1:
            for band in bands:
                regrid(band)
                logger.debug("finished processing band %s of timestep %s", band, time_idx)
        else:
            workers = min(workers, len(bands))
            with ThreadPoolExecutor(max_workers=workers) as exe:
                futures = []
                for band in bands:
                    logger.debug("enqueuing band %s of timestep %s", band, time_idx)
                    futures.append(exe.submit(regrid, band))
                try:
                    for future in as_completed(futures):
                        logger.debug("finished processing band %s of timestep %s", future.result(), time_idx)
                except Exception:
                    exe.shutdown(wait=False, cancel_futures=True)
                    raise

        self._last_processed_idx = time_idx
        self._increment_processed(not any_processed[0])

        return store_idx

    ############################################################################################
    # PROCESSING - BATCH
    ############################################################################################

    def process_batch(
        self,
        start_idx: int,
        end_idx: int,
        bands: list[int] = multicloudconstants.ALL_BANDS,
        show_progress: bool = ConfigDefault("pipeline", "progress", "show_progress"),
        continue_on_error: bool = ConfigDefault("pipeline", "error_handling", "continue_on_error"),
        workers: int = ConfigDefault("pipeline", "worker_threads"),
        overwrite: bool = ConfigDefault("pipeline", "overwrite"),
    ) -> None:
        """
        Process batch of observations.

        Parameters
        ----------
            start_idx: Starting time index
            end_idx: Ending time index (exclusive)
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs (default from pipeline config)
            workers: Number of simultaneous threads used to process the bands (maximum is the number of bands)

        Returns
        -------
            None
        """
        if self._start_time is None:
            self._start_time = datetime.now(UTC)

        logger.info(f"Processing batch: timesteps {start_idx} to {end_idx}")

        self._process_loop(
            indices=range(start_idx, end_idx),
            bands=bands,
            show_progress=show_progress,
            continue_on_error=continue_on_error,
            workers=workers,
            overwrite=overwrite,
        )

        self.store.update_temporal_coverage(self.observation.first.ds.attrs["orbital_slot"])

        logger.info(
            f"Batch complete: {self._processed_count} processed, {self._skipped_count} skipped, {self._failed_count} failed"
        )

    def process_all(
        self,
        bands: list[int] = multicloudconstants.ALL_BANDS,
        show_progress: bool = ConfigDefault("pipeline", "progress", "show_progress"),
        continue_on_error: bool = ConfigDefault("pipeline", "error_handling", "continue_on_error"),
        workers: int = ConfigDefault("pipeline", "worker_threads"),
        overwrite: bool = ConfigDefault("pipeline", "overwrite"),
    ) -> None:
        """
        Process all observations in dataset.

        Parameters
        ----------
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs
            workers: Number of simultaneous threads used to process the bands (maximum is the number of bands)

        Returns
        -------
            None
        """
        self.process_batch(
            start_idx=0,
            end_idx=len(self.observation.nc_files),
            bands=bands,
            show_progress=show_progress,
            continue_on_error=continue_on_error,
            workers=workers,
            overwrite=overwrite,
        )

    def process_time_range(
        self,
        start_time: str | datetime | np.datetime64,
        end_time: str | datetime | np.datetime64,
        bands: list[int] = multicloudconstants.ALL_BANDS,
        show_progress: bool = ConfigDefault("pipeline", "progress", "show_progress"),
        continue_on_error: bool = ConfigDefault("pipeline", "error_handling", "continue_on_error"),
        workers: int = ConfigDefault("pipeline", "worker_threads"),
        overwrite: bool = ConfigDefault("pipeline", "overwrite"),
    ) -> None:
        """
        Process observations within time range.

        Parameters
        ----------
            start_time: Start time
            end_time: End time
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs
            workers: Number of simultaneous threads used to process the bands (maximum is the number of bands)

        Returns
        -------
            None
        """
        # Convert to datetime64
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)

        indices = [i for i, obs in self.observation if start_dt <= obs.time_range_from_filename()[0] >= end_dt]

        logger.info(f"Found {len(indices)} observations in time range")

        if len(indices) == 0:
            logger.warning("No observations found in specified time range")
            return

        if self._start_time is None:
            self._start_time = datetime.now(UTC)

        self._process_loop(
            indices=indices.tolist(),
            bands=bands,
            show_progress=show_progress,
            continue_on_error=continue_on_error,
            progress_desc="Processing time range",
            workers=workers,
            overwrite=overwrite,
        )

        self.store.update_temporal_coverage(self.observation.first.attrs["orbital_slot"])

        logger.info(f"Time range complete: {self._processed_count} processed, {self._failed_count} failed")

    ############################################################################################
    # VALIDATION & DIAGNOSTICS
    ############################################################################################

    def validate_setup(
        self, check_disk_space: bool = True, expected_store_size_gb: int | None = None
    ) -> dict[str, bool]:
        """
        Validate pipeline setup.

        Parameters
        ----------
            check_disk_space: also validate whether there is enough space on disk to store the output zarr store
            expected_store_size_gb: expected size of the of the output zarr store (in GB), if None then this will
                                    be estimated based on input size and compression heuristics.

        Returns
        -------
            Dictionary with validation results
        """
        results = {
            "observation_initialized": self._observation is not None,
            "regridder_initialized": self._regridder is not None,
            "store_initialized": self._store is not None,
        }

        # Check disk space if requested
        if check_disk_space:
            if expected_store_size_gb is None:
                expected_store_size_gb = self.estimate_output_size()["compressed_gb"]
            results["sufficient_disk_space"] = self._check_disk_space(expected_store_size_gb)

        # Log results
        for check, passed in results.items():
            status = "+" if passed else "x"
            logger.info(f"[{status}] {check}: {passed}")

        return results

    def _check_disk_space(self, required_gb: float) -> bool:
        """Check if sufficient disk space available."""
        try:
            store_path = self._config["store"]["path"]

            stats = shutil.disk_usage(store_path)
            available_gb = stats.free / (1024**3)

            logger.info(f"Available disk space: {available_gb:.1f} GB")

            return available_gb >= required_gb
        except Exception as e:
            logger.warning(f"Could not check disk space: {e}")
            return True  # Don't fail validation on error

    def estimate_output_size(self) -> dict[str, float]:
        """
        Estimate output Zarr size.

        Returns
        -------
            Dictionary with size estimates in GB
        """
        if not self.is_initialized:
            logger.warning("Pipeline not initialized, estimates may be inaccurate")

        # Get dimensions
        n_timesteps = self.total_observations
        n_bands = len(multicloudconstants.ALL_BANDS)
        lat_size, lon_size = self.regridder.target_shape

        # Estimate per-band size (float32 = 4 bytes, uint8 = 1 byte)
        cmi_size_per_timestep = lat_size * lon_size * 4  # float32
        dqf_size_per_timestep = lat_size * lon_size * 1  # uint8

        # Total uncompressed
        total_cmi = n_timesteps * n_bands * cmi_size_per_timestep
        total_dqf = n_timesteps * n_bands * dqf_size_per_timestep
        total_uncompressed = total_cmi + total_dqf

        # Estimate compression (typical ratio 3-5x for satellite data)
        compression_ratio = 4.0
        total_compressed = total_uncompressed / compression_ratio

        # Convert to GB
        estimates = {
            "uncompressed_gb": total_uncompressed / (1024**3),
            "compressed_gb": total_compressed / (1024**3),
            "compression_ratio": compression_ratio,
            "per_band_gb": (total_compressed / n_bands) / (1024**3) if n_bands > 0 else 0.0,
        }

        logger.info(f"Estimated output size: {estimates['compressed_gb']:.1f} GB compressed")

        return estimates

    def summary(self) -> dict[str, Any]:
        """
        Get comprehensive processing summary.

        Returns
        -------
            Dictionary with processing statistics and configuration
        """
        summary = {
            "status": {
                "initialized": self.is_initialized,
            },
            "processing": {
                "total_observations": self.total_observations,
                "processed_count": self._processed_count,
                "failed_count": self._failed_count,
                "success_rate": self.success_rate,
                "last_processed_idx": self._last_processed_idx,
            },
        }

        # Add timing if started
        if self._start_time:
            elapsed = datetime.now(UTC) - self._start_time
            summary["processing"]["elapsed_seconds"] = elapsed.total_seconds()  # type: ignore[assignment]
            summary["processing"]["start_time"] = self._start_time.isoformat()  # type: ignore[assignment]

        # Add component info if initialized
        if self.is_initialized:
            summary["components"] = {
                "observation": {
                    "timesteps": len(self.observation.nc_files),
                },
                "regridder": {
                    "source_shape": self.regridder.source_shape,
                    "target_shape": self.regridder.target_shape,
                    "coverage_fraction": self.regridder.coverage_fraction,
                },
            }

        return summary

    def print_summary(self) -> None:
        """Pretty-print processing summary."""
        s = self.summary()

        print("\n" + "=" * 70)
        print("GOES PIPELINE ORCHESTRATOR SUMMARY")
        print("=" * 70)

        # Status
        print("\nSTATUS:")
        for key, value in s["status"].items():
            status = "+" if value else "x"
            print(f"  [{status}] {key}: {value}")

        # Configuration
        print("\nCONFIGURATION:")
        print(f"  Region: {s['configuration']['default_region']}")
        print(f"  Bands: {s['configuration']['default_bands']}")

        # Processing
        print("\nPROCESSING:")
        proc = s["processing"]
        print(f"  Total observations: {proc['total_observations']}")
        print(f"  Processed: {proc['processed_count']}")
        print(f"  Failed: {proc['failed_count']}")
        print(f"  Success rate: {proc['success_rate']:.1%}")

        if "elapsed_seconds" in proc:
            elapsed_hrs = proc["elapsed_seconds"] / 3600
            print(f"  Elapsed time: {elapsed_hrs:.2f} hours")

        # Components
        if "components" in s:
            print("\nCOMPONENTS:")
            obs = s["components"]["observation"]
            print(f"  Observation: {obs['timesteps']} timesteps")

            reg = s["components"]["regridder"]
            print(f"  Regridder: {reg['source_shape']} -> {reg['target_shape']}")
            print(f"  Coverage: {reg['coverage_fraction']:.1%}")

        print("=" * 70 + "\n")

    ############################################################################################
    # FINALIZATION & CLEANUP
    ############################################################################################

    def finalize_store(self) -> None:
        """Finalize Zarr store with temporal coverage updates and history."""
        if self._store is not None:
            logger.info("Finalizing store...")
            self.store.finalize_dataset()
            logger.info("Store finalized")

    def finalize(self) -> None:
        """Finalize pipeline and cleanup resources."""
        logger.info("Finalizing pipeline...")

        try:
            self.finalize_store()
        except Exception as e:
            logger.error(f"Error finalizing store: {e}")

        if self._store is not None:
            try:
                self._store.close_store()
            except Exception as e:
                logger.error(f"Error closing store: {e}")

        logger.info("Pipeline finalized")

    ############################################################################################
    # UTILITIES (PRIVATE)
    ############################################################################################

    def _setup_logging(self) -> None:
        """Configure root logger based on pipeline config."""
        log_config = self._config["pipeline"]["logging"]

        log_level = log_config["level"].upper()
        logger = logging.getLogger(__name__.split(".")[0])
        logger.setLevel(log_level)

        # Console handler (only add if no handlers exist)
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)

            log_config["format"]
            formatter = logging.Formatter(log_config["format"], datefmt=log_config["date_format"])

            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # File handler (optional, avoid duplicates)
        log_file = log_config["log_file"]
        if log_file:
            log_file = Path(log_file)
            log_file.parent.mkdir(parents=True, exist_ok=True)

            existing_file_handlers = [
                h
                for h in logger.handlers
                if isinstance(h, logging.FileHandler) and h.baseFilename == str(log_file.resolve())
            ]
            if not existing_file_handlers:
                file_formatter = logging.Formatter(log_config["format"], datefmt=log_config["date_format"])

                file_handler = logging.FileHandler(log_file)
                file_handler.setLevel(log_level)
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)

    def _increment_processed(self, skipped: bool = False) -> None:
        """Increment processed counters and log milestones."""
        self._processed_count += 1
        if skipped:
            self._skipped_count += 1
        # Log milestones
        log_interval = self._config["pipeline"]["progress"]["log_interval"]
        if self._processed_count % log_interval == 0:
            logger.info(f"Processed {self._processed_count} observations ({self.success_rate:.1%} success rate)")

    def _increment_failed(self, time_idx: int, error: Exception) -> None:
        """Record failed observation."""
        self._failed_count += 1
        self._failed_indices.append(time_idx)

        logger.exception(f"Failed to process observation at time index {time_idx}: {error}")

    def _process_loop(
        self,
        indices: Iterable[int],
        bands: list[int],
        show_progress: bool,
        continue_on_error: bool,
        progress_desc: str = "Processing observations",
        workers: int = ConfigDefault("pipeline", "worker_threads"),
        overwrite: bool = ConfigDefault("pipeline", "overwrite"),
    ) -> None:
        """
        Core processing loop shared by process_batch and process_time_range.

        Parameters
        ----------
            indices: Iterable of time indices to process
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs
            progress_desc: Description for progress bar
        """
        if show_progress:
            indices = tqdm(indices, desc=progress_desc)

        for time_idx in indices:
            try:
                self.process_single_observation(time_idx, bands, workers, overwrite)
            except Exception as e:
                self._increment_failed(time_idx, e)
                if continue_on_error:
                    continue
                else:
                    raise

    ############################################################################################
    # DUNDER METHODS
    ############################################################################################

    def __enter__(self) -> "GOESPipelineOrchestrator":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Context manager exit."""
        self.finalize()

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        status = "initialized" if self.is_initialized else "not initialized"

        if self.is_initialized:
            return (
                f"GOESPipelineOrchestrator(\n"
                f"    status={status},\n"
                f"    observations={self.total_observations},\n"
                f"    processed={self._processed_count},\n"
                f"    failed={self._failed_count},\n"
                f"    success_rate={self.success_rate:.1%}\n"
                f")"
            )
        else:
            return f"GOESPipelineOrchestrator(status={status})"
