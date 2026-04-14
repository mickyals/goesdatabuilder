import json
import logging
import os
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import warnings

import numpy as np
import pandas as pd
from tqdm import tqdm

from goesdatabuilder.data.goes import multicloudconstants as multicloudconstants
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
from goesdatabuilder.store.datasets import GOESZarrStore
from goesdatabuilder.utils.config import ConfigDefault, ConfigMixin
from goesdatabuilder.utils.grid_utils import build_longitude_array

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
        - pipeline_config: Orchestration (catalog, Dask, batching, checkpoints, logging)

    Usage:
    ------
        pipeline = GOESPipelineOrchestrator(
            config={...}
        ) # or GOESPipelineOrchestrator.from_configs("config_file.yaml")
        pipeline.initialize_all(store_path='output.zarr')
        pipeline.process_all()
        pipeline.finalize()
    """

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(
        self,
        data_access: dict[str, Any] = ConfigDefault("data_access"),
        regridding: dict[str, Any] = ConfigDefault("regridding"),
        store: dict[str, Any] = ConfigDefault("store"),
        zarr: dict[str, Any] = ConfigDefault("zarr"),
        goes: dict[str, Any] = ConfigDefault("goes"),
        pipeline: dict[str, Any] = ConfigDefault("pipeline"),
    ) -> None:
        """
        Initialize pipeline orchestrator.

        Parameters
        ----------
            config: Path to YAML or JSON config
            catalog: Optional pre-built GOESMetadataCatalog
        """
        # Setup logging first
        self._setup_logging()

        # Components (initialized lazily)
        self._observation = None
        self._regridder = None
        self._store = None
        self._dask_client = None

        # Processing state
        self._processed_count = 0
        self._failed_count = 0
        self._failed_indices = []
        self._last_processed_idx = -1
        self._start_time = None

        # Config shortcuts (computed from configs)
        self._configured_regions = self._config["goes"]["orbital_slots"]
        self._default_region = self._configured_regions[0]
        self._default_bands = self._config["goes"]["bands"]

        logger.info("Pipeline orchestrator initialized")

    ############################################################################################
    # PROPERTIES
    ############################################################################################

    @property
    def is_initialized(self) -> bool:
        """True if all core components are initialized."""
        return self._observation is not None and self._regridder is not None and self._store is not None

    @property
    def has_catalog(self) -> bool:
        """True if catalog is available."""
        return self._catalog is not None

    @property
    def has_dask_client(self) -> bool:
        """True if Dask distributed client is active."""
        if self._dask_client is None:
            return False

        try:
            # Check if client is still alive
            self._dask_client.scheduler_info()
            return True
        except Exception:
            return False

    @property
    def total_observations(self) -> int:
        """Total number of timesteps in dataset."""
        if self._observation is None:
            return 0
        return len(self._observation.nc_files)

    @property
    def processed_count(self) -> int:
        """Number of successfully processed observations."""
        return self._processed_count

    @property
    def failed_count(self) -> int:
        """Number of failed observations."""
        return self._failed_count

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

        Parameters
        ----------
            file_list: Explicit file list (overrides catalog filtering)
            time_range: (start, end) datetime tuple for filtering catalog

        Returns
        -------
            GOESMultiCloudObservation instance
        """
        logger.info("Initializing observation...")

        # Note the chunk_size is automatically set to -1 for the pipeline code because the
        # regridder will have to un-chunk it later on anyway.
        kwargs = {**self._config["data_access"], "chunk_size": -1}
        # Create observation
        self._observation = GOESMultiCloudObservation(valid_orbital_slots=self._configured_regions, **kwargs)

        # Determine available bands by checking which CMI variables exist

        logger.info(f"Observation initialized: {len(self._observation.nc_files)} timesteps")

        return self._observation

    def initialize_regridder(
        self,
        target_grid: dict | None = None,
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

        # Ensure observation is initialized
        if self._observation is None:
            self.initialize_observation()

        if self._config["regridding"]["load_cached"]:
            try:
                self._regridder = GeostationaryRegridder.from_weights(self._config["regridding"]["weights_dir"])
            except FileNotFoundError as e:
                raise ValueError(
                    "No cached weights found for the regridder. Try rebuilding the weights or specify a different location"
                ) from e
        else:
            # Gets the first file of the first batch of files
            single_file_observation = self._observation[0]

            # Set observation band to reference for coordinate extraction
            single_file_observation.band = self._config["regridding"]["reference_band"]

            # Get source coordinates from observation
            source_x = single_file_observation.x.values
            source_y = single_file_observation.y.values
            satellite_projection = single_file_observation.satellite_projection

            # Common kwargs
            regridder_kwargs = dict(
                source_x=source_x,
                source_y=source_y,
                projection=satellite_projection,
                weights_dir=self._config["regridding"]["weights_dir"],
                decimals=self._config["regridding"]["decimals"],
                reference_band=single_file_observation.band,
            )

            if target_grid is not None:
                regridder_kwargs["target_lat"] = target_grid["lat"]
                regridder_kwargs["target_lon"] = target_grid["lon"]

            else:
                target_config = self._config["regridding"]["target"]

                if "lat_min" in target_config and "lon_min" in target_config:
                    res = target_config["resolution"]
                    lat_res = target_config.get("lat_resolution", res)
                    lon_res = target_config.get("lon_resolution", res)

                    regridder_kwargs["target_lat"] = np.arange(
                        target_config["lat_min"],
                        target_config["lat_max"] + lat_res,
                        lat_res,
                    )
                    regridder_kwargs["target_lon"] = build_longitude_array(
                        target_config["lon_min"],
                        target_config["lon_max"],
                        lon_res,
                        decimals=self._config["regridding"]["decimals"],
                    )
                else:
                    regridder_kwargs["target_resolution"] = target_config["resolution"]

            self._regridder = GeostationaryRegridder(**regridder_kwargs)

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
        store_path: str | os.PathLike = ConfigDefault("store", "path"),
        overwrite: bool = False,
        region: str | None = None,
        bands: list[int] | None = None,
        lat_preset: str = "coordinate",
        lon_preset: str = "coordinate",
        time_preset: str = "coordinate",
        aux_preset: str = "coordinate",
        cmi_preset: str = "field",
        dqf_preset: str = "field",
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
            lat_preset: Zarr preset name for latitude coordinate arrays
            lon_preset: Zarr preset name for longitude coordinate arrays
            time_preset: Zarr preset name for time coordinate arrays
            aux_preset: Zarr preset name for auxiliary coordinate arrays
            cmi_preset: Zarr preset name for CMI data arrays
            dqf_preset: Zarr preset name for DQF data arrays

        Returns
        -------
            GOESZarrStore instance
        """
        logger.info("Initializing Zarr store...")

        # --- Resolve all parameters before constructing anything ---

        if self._regridder is None:
            self.initialize_regridder()

        region = region if region is not None else self._default_region
        bands = bands if bands is not None else self._default_bands

        # Guard against silent replacement of an open store
        if self._store is not None:
            logger.warning(f"Reinitializing store (previous store at {self._store.store_path} will be replaced)")
            self._store.close_store()

        # --- All inputs resolved, construct and initialize ---

        self._store = GOESZarrStore(store=self._config["store"], zarr=self._config["zarr"])
        self._store.initialize_store(store_path, overwrite=overwrite)

        self._store.initialize_region(
            region=region,
            lat=self._regridder.target_lat,
            lon=self._regridder.target_lon,
            lat_preset=lat_preset,
            lon_preset=lon_preset,
            time_preset=time_preset,
            aux_preset=aux_preset,
            cmi_preset=cmi_preset,
            dqf_preset=dqf_preset,
            bands=bands,
            include_dqf=True,
            regridder=self._regridder,
        )

        logger.info(f"Store initialized at {self._store.store_path}, region={region}, bands={bands}")

        return self._store

    def initialize_dask_client(
        self,
        n_workers: int = ConfigDefault("pipeline", "dask", "local", "n_workers"),
        threads_per_worker: int = ConfigDefault("pipeline", "dask", "local", "threads_per_worker"),
        memory_limit: str = ConfigDefault("pipeline", "dask", "local", "memory_limit"),
        scheduler_address: str = ConfigDefault("pipeline", "dask", "scheduler_address"),
    ) -> None:
        """
        Initialize Dask distributed client.

        Parameters
        ----------
            n_workers: Number of workers (overrides pipeline_config)
            threads_per_worker: Threads per worker (overrides pipeline_config)
            memory_limit: Memory limit per worker (overrides pipeline_config)
            scheduler_address: Connect to existing cluster (overrides pipeline_config)

        Returns
        -------
            None (sets self._dask_client)
        """
        logger.info("Initializing Dask client...")

        try:
            from dask.distributed import Client, LocalCluster
        except ImportError:
            logger.warning("dask.distributed not available. Install with: pip install dask[distributed]")
            return

        # Connect to remote cluster or create local
        if scheduler_address:
            logger.info(f"Connecting to remote Dask cluster at {scheduler_address}")
            self._dask_client = Client(scheduler_address)

        else:
            logger.info(
                f"Creating local Dask cluster: workers={n_workers}, threads={threads_per_worker}, memory={memory_limit}"
            )

            cluster = LocalCluster(
                n_workers=n_workers, threads_per_worker=threads_per_worker, memory_limit=memory_limit
            )

            self._dask_client = Client(cluster)

        # Apply Dask config overrides
        config_overrides = self._config["pipeline"]["dask"]["config"]
        if config_overrides:
            import dask

            for key, value in config_overrides.items():
                dask.config.set({key: value})

        logger.info(f"Dask client initialized: {self._dask_client}")
        logger.info(f"Dashboard available at: {self._dask_client.dashboard_link}")

    def initialize_all(
        self,
        store_path: str | os.PathLike,
        overwrite: bool = False,
        region: str | None = None,
        bands: list[int] | None = None,
        use_dask_client: bool = ConfigDefault("pipeline", "dask", "enabled"),
        lat_preset: str = "coordinate",
        lon_preset: str = "coordinate",
        time_preset: str = "coordinate",
        aux_preset: str = "coordinate",
        cmi_preset: str = "field",
        dqf_preset: str = "field",
    ) -> None:
        """
        Initialize all pipeline components.

        Parameters
        ----------
            store_path: Path to Zarr store
            overwrite: Overwrite existing store
            region: Region to initialize
            bands: Bands to initialize
            use_catalog: Build/load catalog for file discovery
            use_dask_client: Initialize Dask client
            lat_preset: Zarr preset name for latitude coordinate arrays
            lon_preset: Zarr preset name for longitude coordinate arrays
            time_preset: Zarr preset name for time coordinate arrays
            aux_preset: Zarr preset name for auxiliary coordinate arrays
            cmi_preset: Zarr preset name for CMI data arrays
            dqf_preset: Zarr preset name for DQF data arrays

        Returns
        -------
            None
        """
        logger.info("Initializing all pipeline components...")

        # 2. Observation (required)
        self.initialize_observation()

        # 3. Regridder (required)
        self.initialize_regridder()

        # 4. Store (required)
        self.initialize_store(
            store_path,
            overwrite,
            region,
            bands,
            lat_preset=lat_preset,
            lon_preset=lon_preset,
            time_preset=time_preset,
            aux_preset=aux_preset,
            cmi_preset=cmi_preset,
            dqf_preset=dqf_preset,
        )

        if use_dask_client:
            self.initialize_dask_client()

        logger.info("All components initialized successfully")

    ############################################################################################
    # PROCESSING - SINGLE OBSERVATION
    ############################################################################################

    def process_single_observation(self, time_idx: int, bands: list[int] = None, region: str = None) -> int:
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

        Returns
        -------
            Store time index where data was written
        """
        if not self.is_initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_all() first.")

        observation = self._observation[time_idx]

        bands = bands if bands is not None else self._default_bands
        region = region if region is not None else self._default_region

        # Extract metadata first (cheap, fails fast if dataset is malformed)
        timestamp = observation.time.isel(time=0).values
        obs_ds = observation.isel_time(0)
        platform_id = str(obs_ds["platform_id"].values)
        scan_mode = str(obs_ds["scan_mode"].values) if "scan_mode" in obs_ds else None

        with warnings.catch_warnings():
            # ignore warning that U3 and U10 are unsupported zarr dtypes
            warnings.simplefilter("ignore")
            self._store.append_array(f"{region}/platform_id", np.array([platform_id]))
            self._store.append_array(f"{region}/scan_mode", np.array([scan_mode or "unknown"]))
        store_idx = self._store.append_array(f"{region}/time", np.array([timestamp]), axis=0, return_location=True)[0]

        # Regrid CMI and DQF for each band

        def regrid(band: str) -> tuple[str, np.ndarray, np.ndarray]:
            cmi_3d = observation.get_cmi(band)
            cmi_2d = cmi_3d.isel(time=0)

            cmi_regridded_3d = self._regridder.regrid(cmi_2d).values[np.newaxis, :, :]
            self._store.append_array(f"{region}/CMI_C{band:02d}", cmi_regridded_3d, axis=0)
            del cmi_3d, cmi_2d, cmi_regridded_3d

            dqf_3d = observation.get_dqf(band)
            dqf_2d = dqf_3d.isel(time=0)

            dqf_regridded_3d = self._regridder.regrid(dqf_2d).values[np.newaxis, :, :]
            self._store.append_array(f"{region}/CMI_C{band:02d}", dqf_regridded_3d, axis=0)
            del dqf_3d, dqf_2d, dqf_regridded_3d

            return band


        workers = int(os.getenv("GOES_MAX_WORKERS", 16)) # TODO: make this settable in the config
        with ThreadPoolExecutor(max_workers=workers) as exe:
            futures = []
            for band in bands:
                logger.debug("processing band %s of timestep %s", band, time_idx)

                futures.append(exe.submit(regrid, band))
            try:
                for future in as_completed(futures):
                    logger.debug("finished processing band %s of timestep %s", future.result(), time_idx)
            except Exception:
                exe.shutdown(wait=False, cancel_futures=True)

        self._last_processed_idx = time_idx
        self._increment_processed()

        return store_idx

    ############################################################################################
    # PROCESSING - BATCH
    ############################################################################################

    def process_batch(
        self,
        start_idx: int = 0,
        end_idx: int = None,
        bands: list[int] = None,
        region: str | None = None,
        show_progress: bool = ConfigDefault("pipeline", "progress", "show_progress"),
        continue_on_error: bool = ConfigDefault("pipeline", "batching", "continue_on_error"),
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

        Returns
        -------
            None
        """
        if not self.is_initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_all() first.")

        # Set defaults
        bands, region, end_idx = self._set_processing_defaults(bands, region, end_idx)

        if self._start_time is None:
            self._start_time = datetime.now(UTC)

        logger.info(f"Processing batch: timesteps {start_idx} to {end_idx}")

        self._process_loop(
            indices=range(start_idx, end_idx),
            bands=bands,
            region=region,
            show_progress=show_progress,
            continue_on_error=continue_on_error,
        )

        self._store.update_temporal_coverage(region)

        logger.info(f"Batch complete: {self._processed_count} processed, {self._failed_count} failed")

    def process_all(
        self,
        bands: list[int] = None,
        region: str | None = None,
        show_progress: bool = ConfigDefault("pipeline", "progress", "show_progress"),
        continue_on_error: bool = ConfigDefault("pipeline", "batching", "continue_on_error"),
    ) -> None:
        """
        Process all observations in dataset.

        Parameters
        ----------
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs

        Returns
        -------
            None
        """
        self.process_batch(
            start_idx=0,
            end_idx=None,
            bands=bands,
            region=region,
            show_progress=show_progress,
            continue_on_error=continue_on_error,
        )

    # TODO: remove this function because it requires loading everything into memory which isn't feasible for large batches
    def process_time_range(
        self,
        start_time: str | datetime | np.datetime64,
        end_time: str | datetime | np.datetime64,
        bands: list[int] = None,
        region: str | None = None,
        show_progress: bool = ConfigDefault("pipeline", "progress", "show_progress"),
        continue_on_error: bool = ConfigDefault("pipeline", "batching", "continue_on_error"),
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

        Returns
        -------
            None
        """
        if not self.is_initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_all() first.")

        # Convert to datetime64
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)

        # Find time indices
        time_values = pd.to_datetime(self._observation.time.values)
        mask = (time_values >= start_dt) & (time_values <= end_dt)
        indices = np.where(mask)[0]

        logger.info(f"Found {len(indices)} observations in time range")

        if len(indices) == 0:
            logger.warning("No observations found in specified time range")
            return

        # Set defaults
        bands, region, _ = self._set_processing_defaults(bands, region)

        if self._start_time is None:
            self._start_time = datetime.now(UTC)

        self._process_loop(
            indices=indices.tolist(),
            bands=bands,
            region=region,
            show_progress=show_progress,
            continue_on_error=continue_on_error,
            progress_desc="Processing time range",
        )

        self._store.update_temporal_coverage(region)

        logger.info(f"Time range complete: {self._processed_count} processed, {self._failed_count} failed")

    ############################################################################################
    # ERROR RECOVERY
    ############################################################################################

    def retry_failed(
        self, bands: list[int] = None, region: str = None, show_progress: bool | None = None, max_retries: int = None
    ) -> None:
        """
        Retry processing failed observations.

        Parameters
        ----------
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            max_retries: Max retries per observation (default from pipeline config)

        Returns
        -------
            None
        """
        if not self._failed_indices:
            logger.info("No failed observations to retry")
            return

        if max_retries is None:
            max_retries = self._config["pipeline"]["batching"]["max_retries"]

        if show_progress is None:
            show_progress = self._config["pipeline"]["progress"]["show_progress"]

        # Set defaults
        bands, region, _ = self._set_processing_defaults(bands, region)

        logger.info(f"Retrying {len(self._failed_indices)} failed observations")

        # Deduplicate: count prior failures per index to enforce max_retries
        from collections import Counter

        failure_counts = Counter(self._failed_indices)

        to_retry = []
        still_failed = []
        for idx, count in failure_counts.items():
            if count <= max_retries:
                to_retry.append(idx)
            else:
                logger.warning(f"Max retries exceeded for time index {idx} ({count} prior failures)")
                still_failed.append(idx)

        # Reset failed indices (repopulated by any new failures below)
        self._failed_indices = still_failed
        retry_failed_count = 0

        iterator = sorted(to_retry)

        if show_progress:
            iterator = tqdm(iterator, desc="Retrying failed")

        for time_idx in iterator:
            try:
                self.process_single_observation(time_idx, bands, region)
            except Exception as e:
                retry_failed_count += 1
                self._failed_indices.append(time_idx)
                logger.error(f"Retry failed for time index {time_idx}: {e}")

        succeeded = len(to_retry) - retry_failed_count
        logger.info(
            f"Retry complete: {succeeded} succeeded, "
            f"{retry_failed_count} still failed, "
            f"{len(still_failed)} skipped (max retries exceeded)"
        )

    def skip_failed(self) -> None:
        """Clear failed indices list (mark as intentionally skipped)."""
        skipped_count = len(self._failed_indices)
        self._failed_indices = []
        logger.info(f"Skipped {skipped_count} failed observations")

    def export_failed_indices(self, output_path: str | Path) -> None:
        """Export failed indices to JSON."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump({"failed_indices": self._failed_indices}, f, indent=2)

        logger.info(f"Exported {len(self._failed_indices)} failed indices to {output_path}")

    def import_failed_indices(self, input_path: str | Path) -> None:
        """Import failed indices from JSON."""
        input_path = Path(input_path)

        with open(input_path) as f:
            data = json.load(f)

        self._failed_indices = data[
            "failed_indices"
        ]  # NOTE TO SELF: CONSIDER OPTION TO APPEND FAILED INDICES ALREADY STORED WITH JSON
        logger.info(f"Imported {len(self._failed_indices)} failed indices from {input_path}")

    ############################################################################################
    # CHECKPOINTING & STATE MANAGEMENT
    ############################################################################################

    def save_checkpoint(self, checkpoint_path: str | Path) -> None:
        """Save processing state to JSON checkpoint."""
        checkpoint_path = Path(checkpoint_path)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        state = self.processing_state
        state["timestamp"] = datetime.now(UTC).isoformat()

        with open(checkpoint_path, "w") as f:
            json.dump(state, f, indent=2)

        logger.info(f"Checkpoint saved to {checkpoint_path}")

    def load_checkpoint(self, checkpoint_path: str | Path) -> None:
        """Restore processing state from JSON checkpoint."""
        checkpoint_path = Path(checkpoint_path)
        if not checkpoint_path.exists():
            raise FileNotFoundError(f"Checkpoint file not found: {checkpoint_path}")

        with open(checkpoint_path) as f:
            state = json.load(f)

        self._processed_count = state["processed_count"]
        self._failed_count = state["failed_count"]
        self._failed_indices = state["failed_indices"]
        self._last_processed_idx = state["last_processed_idx"]

        if state.get("start_time"):
            self._start_time = datetime.fromisoformat(state["start_time"])

        logger.info(f"Checkpoint loaded: {self._processed_count} processed, {self._failed_count} failed")

    def resume_from_checkpoint(
        self, checkpoint_path: str | Path, store_path: str | Path, continue_processing: bool = True
    ) -> None:
        """
        Resume processing from a saved checkpoint.

        Opens the existing Zarr store (does not recreate it) and continues
        processing from the last successfully processed index.

        Parameters
        ----------
            checkpoint_path: Path to checkpoint JSON
            store_path: Path to existing Zarr store
            continue_processing: If True, continue processing after loading
        """
        logger.info("Resuming from checkpoint...")

        self.load_checkpoint(checkpoint_path)

        self.initialize_observation()
        self.initialize_regridder()

        # Open existing store (not create new)
        self._store = GOESZarrStore.from_existing(store_path=store_path, mode="r+", **self._config)
        self._store.rebuild_region_cache(self._default_region)

        if continue_processing:
            start_idx = self._last_processed_idx + 1
            logger.info(f"Continuing processing from index {start_idx}")
            self.process_batch(start_idx=start_idx)

    def _auto_checkpoint(self) -> None:
        """Automatically save checkpoint if enabled in config."""
        checkpoint_config = self._config["pipeline"]["checkpoints"]

        if not checkpoint_config["enabled"]:
            return

        checkpoint_dir = checkpoint_config["directory"]
        checkpoint_dir = Path(os.path.expandvars(checkpoint_dir))
        checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Create checkpoint filename with timestamp
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        checkpoint_path = checkpoint_dir / f"checkpoint_{timestamp}.json"

        self.save_checkpoint(checkpoint_path)

        # Clean up old checkpoints
        keep_last_n = checkpoint_config.get("keep_last_n", 5)
        if keep_last_n:
            self._cleanup_old_checkpoints(checkpoint_dir, keep_last_n)

    @staticmethod
    def _cleanup_old_checkpoints(checkpoint_dir: Path, keep_last_n: int) -> None:
        """Remove old checkpoints, keeping only the last N."""
        checkpoints = sorted(checkpoint_dir.glob("checkpoint_*.json"))

        if len(checkpoints) > keep_last_n:
            for old_checkpoint in checkpoints[:-keep_last_n]:
                old_checkpoint.unlink()
                logger.debug(f"Removed old checkpoint: {old_checkpoint}")

    ############################################################################################
    # VALIDATION & DIAGNOSTICS
    ############################################################################################

    def validate_setup(self) -> dict[str, bool]:
        """
        Validate pipeline setup.

        Returns
        -------
            Dictionary with validation results
        """
        results = {
            "observation_initialized": self._observation is not None,
            "regridder_initialized": self._regridder is not None,
            "store_initialized": self._store is not None,
            "catalog_available": self._catalog is not None,
            "dask_client_active": self.has_dask_client,
        }

        # Check disk space if requested
        validation_config = self._config["pipeline"]["validation"]
        if validation_config["check_disk_space"]:
            required_gb = validation_config["required_free_space_gb"]
            results["sufficient_disk_space"] = self._check_disk_space(required_gb)

        # Log results
        for check, passed in results.items():
            status = "+" if passed else "x"
            logger.info(f"[{status}] {check}: {passed}")

        return results

    def _check_disk_space(self, required_gb: float) -> bool:
        """Check if sufficient disk space available."""
        try:
            import shutil

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
        n_bands = len(self._default_bands)
        lat_size, lon_size = self._regridder.target_shape if self._regridder else (1000, 1000)

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
                "has_catalog": self.has_catalog,
                "has_dask_client": self.has_dask_client,
            },
            "configuration": {
                "default_region": self._default_region,
                "default_bands": self._default_bands,
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
                    "timesteps": len(self._observation.nc_files),
                },
                "regridder": {
                    "source_shape": self._regridder.source_shape,
                    "target_shape": self._regridder.target_shape,
                    "coverage_fraction": self._regridder.coverage_fraction,
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
            print(f"  Observation: {obs['timesteps']} timesteps, bands {obs['available_bands']}")

            reg = s["components"]["regridder"]
            print(f"  Regridder: {reg['source_shape']} -> {reg['target_shape']}")
            print(f"  Coverage: {reg['coverage_fraction']:.1%}")

        print("=" * 70 + "\n")

    ############################################################################################
    # HELPER METHODS
    ############################################################################################

    # TODO: do this better with ConfigDefault
    def _set_processing_defaults(
        self, bands: list[int] | None = None, region: str | None = None, end_idx: int | None = None
    ) -> tuple[list[int], str, int]:
        """
        Set default values for processing parameters.

        Parameters
        ----------
            bands: Optional list of bands to process
            region: Optional region to process
            end_idx: Optional end index (only used by process_batch)

        Returns
        -------
            tuple: (bands, region, end_idx) with defaults applied
        """
        bands = bands if bands is not None else self._default_bands
        region = region if region is not None else self._default_region
        end_idx = end_idx if end_idx is not None else self.total_observations

        return bands, region, end_idx

    ############################################################################################
    # FINALIZATION & CLEANUP
    ############################################################################################

    def finalize_store(self) -> None:
        """Finalize Zarr store with temporal coverage updates and history."""
        if self._store:
            logger.info("Finalizing store...")
            self._store.finalize_dataset()
            logger.info("Store finalized")

    def close_dask_client(self) -> None:
        """Shutdown Dask client."""
        if self._dask_client:
            logger.info("Closing Dask client...")
            try:
                self._dask_client.close()
            except Exception as e:
                logger.error(f"Error closing Dask client: {e}")
            finally:
                self._dask_client = None
            logger.info("Dask client closed")

    def finalize(self) -> None:
        """Finalize pipeline and cleanup resources."""
        logger.info("Finalizing pipeline...")

        try:
            self.finalize_store()
        except Exception as e:
            logger.error(f"Error finalizing store: {e}")

        try:
            self.close_dask_client()
        except Exception as e:
            logger.error(f"Error closing Dask client: {e}")

        if self._store:
            try:
                self._store.close_store()
            except Exception as e:
                logger.error(f"Error closing store: {e}")

        logger.info("Pipeline finalized")

    ############################################################################################
    # UTILITIES (PRIVATE)
    ############################################################################################

    def _setup_logging(self) -> None:
        """Configure logging based on pipeline config."""
        log_config = self._config["pipeline"]["logging"]

        log_level = log_config["level"].upper()
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

    # def _get_batch_size(self) -> int:
    #     """Get batch size from pipeline config or auto-calculate."""
    #     batching = self._pipeline_config.get('batching', {})
    #     batch_size = batching.get('batch_size')
    #
    #     if batch_size is not None:
    #         return batch_size
    #
    #     # Auto-calculate based on available memory
    #     try:
    #         import psutil # type: ignore
    #     except ImportError:
    #         logger.warning("psutil not available, using default batch size")
    #         return 100
    #     else:
    #         available_gb = psutil.virtual_memory().available / (1024 ** 3)
    #         # Use 50% of available memory, ~100MB per observation
    #         batch_size = int((available_gb * 0.5 * 1024) / 100)
    #         batch_size = max(10, min(batch_size, 1000))
    #         logger.info(f"Auto-calculated batch size: {batch_size}")
    #         return batch_size

    def _increment_processed(self) -> None:
        """Increment processed counter and log milestones."""
        self._processed_count += 1

        # Log milestones
        log_interval = self._config["pipeline"]["progress"]["log_interval"]
        if self._processed_count % log_interval == 0:
            logger.info(f"Processed {self._processed_count} observations ({self.success_rate:.1%} success rate)")

    def _increment_failed(self, time_idx: int, error: Exception) -> None:
        """Record failed observation."""
        self._failed_count += 1
        self._failed_indices.append(time_idx)

        logger.exception(f"Failed to process observation at time index {time_idx}: {error}")

    def _should_checkpoint(self, current_idx: int) -> bool:
        """Check if checkpoint should be saved."""
        if not self._config["pipeline"]["checkpoints"]["enabled"]:
            return False

        interval = self._config["pipeline"]["batching"]["checkpoint_interval"]

        if interval is None:
            return False

        return (current_idx + 1) % interval == 0

    def _get_files_from_catalog(
        self,
        time_range: tuple[datetime, datetime] = None,
    ) -> list[Path]:
        """
        Get file paths from the catalog, optionally filtered by time range.

        The GOESMetadataCatalog observations DataFrame stores absolute file paths
        in the 'file_path' column (set by scan_file). There is no 'filename' or
        'band_id' column. MCMIP files contain all 16 bands per file, so band-level
        filtering at the file level is not applicable.

        Catalog filters for orbital_slot and scene_id can be applied from
        the pipeline config's catalog section.

        Parameters
        ----------
            time_range: Optional (start, end) datetime tuple

        Returns
        -------
            List of Path objects
        """
        if self._catalog is None:
            raise RuntimeError("Catalog not initialized")

        if time_range is None:
            args = {}
        else:
            args = {"start": time_range[0], "end": time_range[1]}

        args["orbital_slot"] = self._config["pipeline"]["catalog"]["orbital_slot"]
        args["scene_id"] = self._config["pipeline"]["catalog"]["scene_id"]

        return self._catalog.get_files_for_period(**{k: v for k, v in args.items() if v is not None})

    def _process_loop(
        self,
        indices: Iterable[int],
        bands: list[int],
        region: str,
        show_progress: bool,
        continue_on_error: bool,
        progress_desc: str = "Processing observations",
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
                self.process_single_observation(time_idx, bands, region)

                if self._should_checkpoint(time_idx):
                    try:
                        self._auto_checkpoint()
                    except Exception as e:
                        logger.error(f"Checkpoint failed at index {time_idx}: {e}")

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
