from pathlib import Path
from typing import Optional, Union, List, Dict, Any
from datetime import datetime, timezone
import logging
import numpy as np
import pandas as pd
import yaml
import json
import os
import copy

from ..data.goes.multicloudcatalog import GOESMetadataCatalog
from  ..data.goes import multicloudconstants as multicloudconstants
from ..data.goes.multicloud import GOESMultiCloudObservation
from ..regrid.geostationary import GeostationaryRegridder
from ..store.datasets import GOESZarrStore
from ..utils.grid_utils import build_longitude_array

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration validation error"""
    pass


class GOESPipelineOrchestrator:
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
    - obs_config: GOESMultiCloudObservation setup (data access, regridding)
    - store_config: GOESZarrStore setup (storage, compression, metadata, platforms, bands)
        NOTE: Must be a file path (str or Path), not a dict, because GOESZarrStore
        and ZarrStoreBuilder require a config file path for initialization.
    - pipeline_config: Orchestration (catalog, Dask, batching, checkpoints, logging)

    Usage:
    ------
        pipeline = GOESPipelineOrchestrator.from_configs(
            obs_config='obs.yaml',
            store_config='store.yaml',
            pipeline_config='pipeline.yaml'
        )
        pipeline.initialize_all(store_path='output.zarr')
        pipeline.process_all()
        pipeline.finalize()
    """

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(
            self,
            obs_config: Union[str, Path, dict],
            store_config: Union[str, Path],
            pipeline_config: Union[str, Path, dict] = None,
            catalog: Optional[GOESMetadataCatalog] = None
                ):

        """
        Initialize pipeline orchestrator.

        Parameters:
            obs_config: Path to YAML or dict with GOESMultiCloudObservation config and regridder configs
            store_config: Path to YAML/JSON config file for GOESZarrStore.
                          Must be a file path because ZarrStoreBuilder._load_config requires it.
            pipeline_config: Path to YAML or dict with pipeline orchestration config
            catalog: Optional pre-built GOESMetadataCatalog
        """
        # Load configurations
        self._obs_config = self._load_config(obs_config)
        # store_config must remain a path for GOESZarrStore constructor
        self._store_config_path = Path(store_config)
        if not self._store_config_path.exists():
            raise FileNotFoundError(f"Store config file not found: {self._store_config_path}")
        self._store_config = self._load_config(store_config)
        self._pipeline_config = self._load_config(pipeline_config) if pipeline_config else {}

        # Setup logging first
        self._setup_logging()

        # Components (initialized lazily)
        self._catalog = catalog
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
        self._configured_regions = self._get_configured_regions()
        self._default_region = self._configured_regions[0]
        self._default_bands = self._get_default_bands()

        logger.info("Pipeline orchestrator initialized")

    @classmethod
    def from_configs(
            cls,
            obs_config: Union[str, Path],
            store_config: Union[str, Path],
            pipeline_config: Union[str, Path] = None
    ) -> 'GOESPipelineOrchestrator':
        """
        Create pipeline from config file paths.

        Parameters:
            obs_config: Path to observation YAML config
            store_config: Path to store YAML config
            pipeline_config: Path to pipeline YAML config

        Returns:
            GOESPipelineOrchestrator instance
        """
        return cls(
            obs_config=obs_config,
            store_config=store_config,
            pipeline_config=pipeline_config
        )

    ############################################################################################
    # PROPERTIES
    ############################################################################################

    @property
    def is_initialized(self) -> bool:
        """True if all core components are initialized."""
        return (
                self._observation is not None and
                self._regridder is not None and
                self._store is not None
        )

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
        return len(self._observation.time)

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
    def processing_state(self) -> Dict[str, Any]:
        """
        Comprehensive processing state for checkpointing.

        Returns:
            Dictionary with processing statistics
        """
        state = {
            'processed_count': self._processed_count,
            'failed_count': self._failed_count,
            'failed_indices': self._failed_indices.copy(),
            'last_processed_idx': self._last_processed_idx,
            'start_time': self._start_time.isoformat() if self._start_time else None,
            'success_rate': self.success_rate,
        }

        # Add elapsed time if processing has started
        if self._start_time:
            elapsed = datetime.now(timezone.utc) - self._start_time
            state['elapsed_seconds'] = elapsed.total_seconds()

        return state

    @property
    def obs_config(self) -> dict:
        """Observation configuration (read-only copy)."""
        return copy.deepcopy(self._obs_config)

    @property
    def store_config(self) -> dict:
        """Store configuration (read-only copy)."""
        return copy.deepcopy(self._store_config)

    @property
    def pipeline_config(self) -> dict:
        """Pipeline configuration (read-only copy)."""
        return copy.deepcopy(self._pipeline_config)

    ############################################################################################
    # COMPONENT INITIALIZATION
    ############################################################################################

    def initialize_catalog(
            self,
            force_rebuild: bool = False,
    ) -> GOESMetadataCatalog:
        """
        Initialize or load metadata catalog.

        The GOESMetadataCatalog constructor takes only output_dir. File scanning
        is done via scan_directory(directory, pattern) or scan_files(file_paths).

        Parameters:
            force_rebuild: Rebuild catalog even if CSV exists

        Returns:
            GOESMetadataCatalog instance
        """
        logger.info("Initializing metadata catalog...")

        # Get data access config (from observation config)
        data_config = self._obs_config.get('data_access')
        if data_config is None:
            raise ConfigError("Missing Key: 'data_access' is missing from the provided in Observation configuration. ")

        if 'file_dir' not in data_config:
            raise ConfigError("obs_config['data_access'] must contain 'file_dir' key")

        file_dir = Path(os.path.expandvars(data_config['file_dir']))

        if not file_dir.exists():
            raise ConfigError(f"file_dir not found: {file_dir}")

        # Determine catalog output directory (from pipeline config, not obs config)
        catalog_config = self._pipeline_config.get('catalog')
        if catalog_config is None:
            raise ConfigError("Missing Key: 'catalog' is missing from the provided in Pipeline configuration. ")
        catalog_dir = catalog_config.get('output_dir', str(file_dir / 'catalog'))
        catalog_dir = Path(os.path.expandvars(str(catalog_dir)))

        observations_csv = catalog_dir / 'observations.csv'

        # Load existing or build new
        # GOESMetadataCatalog(output_dir) creates the instance
        # .from_csv() is an instance method that loads CSVs into internal DataFrames
        if observations_csv.exists() and not force_rebuild:
            logger.info(f"Loading existing catalog from {catalog_dir}")
            self._catalog = GOESMetadataCatalog(output_dir=catalog_dir).from_csv()
        else:
            logger.info(f"Building new catalog from {file_dir}")

            # Create catalog (constructor only takes output_dir)
            self._catalog = GOESMetadataCatalog(output_dir=catalog_dir)



            # Get glob pattern from data config
            pattern = data_config.get('pattern', '**/*.nc')

            # scan_directory finds files matching pattern and calls scan_files internally
            # scan_files accepts parallel and max_workers kwargs
            self._catalog.scan_directory(
                directory=file_dir,
                pattern=pattern,
            )

            # Export to CSV
            self._catalog.to_csv()
            logger.info(f"Catalog saved to {catalog_dir}")


        logger.info(f"Catalog ready: {len(self._catalog.observations)} observations")

        return self._catalog

    def initialize_observation(
            self,
            file_list: Optional[List[Path]] = None,
            time_range: Optional[tuple] = None,
    ) -> GOESMultiCloudObservation:
        """
        Initialize GOESMultiCloudObservation.

        GOESMultiCloudObservation._validate_and_load_config expects either
        'files' (list of paths) or 'file_dir' (directory path) under data_access.

        Parameters:
            file_list: Explicit file list (overrides catalog filtering)
            time_range: (start, end) datetime tuple for filtering catalog

        Returns:
            GOESMultiCloudObservation instance
        """
        logger.info("Initializing observation...")

        # Determine file list
        if file_list is None:
            # Get files from catalog with filters
            if self._catalog is None:
                logger.info("No catalog available, initializing...")
                self.initialize_catalog()

            file_list = self._get_files_from_catalog(time_range)

        if not file_list:
            raise ValueError("No files to process. Check catalog filters or provide explicit file_list.")

        logger.info(f"Selected {len(file_list)} files")

        # Build observation config with file list
        # GOESMultiCloudObservation expects 'files' key (not 'file_list')
        obs_config_copy = self.obs_config # this is a deep copy of the og config

        if 'data_access' not in obs_config_copy:
            obs_config_copy['data_access'] = {}

        # Set files list (the key the actual constructor looks for)
        obs_config_copy['data_access']['files'] = [str(f) for f in file_list]

        # Remove file_dir to avoid ambiguity (files takes precedence per the constructor,
        # but cleaner to not have both)
        obs_config_copy['data_access'].pop('file_dir', None)

        # Create observation
        self._observation = GOESMultiCloudObservation(obs_config_copy)

        # Set default region from actual data (not config ordering)
        observed_slot = str(self._observation.orbital_slot.isel(time=0).values)
        if observed_slot not in self._configured_regions:
            raise ConfigError(
                f"Observed orbital slot '{observed_slot}' not in configured regions "
                f"{self._configured_regions}"
            )
        self._default_region = observed_slot

        # Determine available bands by checking which CMI variables exist
        available_bands = self._get_available_bands()

        logger.info(
            f"Observation initialized: "
            f"{len(self._observation.time)} timesteps, "
            f"bands={available_bands}"
        )

        return self._observation

    def initialize_regridder(
            self,
            reference_band: int = None,
            load_cached: bool = None,
            target_grid: Optional[dict] = None
    ) -> GeostationaryRegridder:
        """
        Initialize GeostationaryRegridder.

        Parameters:
            reference_band: Band for weight computation (overrides obs_config)
            load_cached: Load cached weights (overrides obs_config)
            target_grid: Optional explicit {'lat': array, 'lon': array}

        Returns:
            GeostationaryRegridder instance
        """
        logger.info("Initializing regridder...")

        # Ensure observation is initialized
        if self._observation is None:
            self.initialize_observation()

        # Get regridding config from observation config
        regrid_config = self._obs_config.get('regridding')
        if regrid_config is None:
            raise ConfigError("Missing Key: 'regridding' is missing from the provided in Observation configuration. ")

        # Get reference band (override > obs_config > default)
        if reference_band is None:
            reference_band = regrid_config.get('reference_band', 7)

        # Get load_cached (override > obs_config > default)
        if load_cached is None:
            load_cached = regrid_config.get('load_cached', True)

        # Set observation band to reference for coordinate extraction
        self._observation.band = reference_band

        # Get source coordinates from observation
        source_x = self._observation.x.values
        source_y = self._observation.y.values
        satellite_projection = self._observation.satellite_projection

        # Get weights directory from config
        weights_dir = regrid_config.get('weights_dir')

        # Get decimals for np.round in target grid construction (default 4)
        decimals = regrid_config.get('decimals', 4)

        # Common kwargs
        regridder_kwargs = dict(
            source_x=source_x,
            source_y=source_y,
            projection=satellite_projection,
            weights_dir=weights_dir,
            load_cached=load_cached,
            decimals=decimals,
            reference_band=reference_band,
        )

        if target_grid is not None:
            regridder_kwargs['target_lat'] = target_grid['lat']
            regridder_kwargs['target_lon'] = target_grid['lon']

        else:
            target_config = regrid_config.get('target', {})

            if 'lat_min' in target_config and 'lon_min' in target_config:
                res = target_config.get('resolution', 0.02)
                lat_res = target_config.get('lat_resolution', res)
                lon_res = target_config.get('lon_resolution', res)

                regridder_kwargs['target_lat'] = np.arange(
                    target_config['lat_min'],
                    target_config['lat_max'] + lat_res,
                    lat_res,
                )
                regridder_kwargs['target_lon'] = build_longitude_array(
                    target_config['lon_min'],
                    target_config['lon_max'],
                    lon_res,
                    decimals=decimals,
                )
            else:
                regridder_kwargs['target_resolution'] = target_config.get('resolution', 0.02)

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
            store_path: Union[str, Path] = None,
            overwrite: bool = False,
            region: str = None,
            bands: List[int] = None
    ) -> GOESZarrStore:
        """
        Initialize GOESZarrStore.

        Store path resolution and type dispatch are handled entirely by
        ZarrStoreBuilder._resolve_store. The orchestrator only resolves
        semantic parameters (region, bands) and validates inputs before
        constructing anything.

        Parameters:
            store_path: Path or URL to Zarr store (overrides store_config).
                        Env vars expanded by _resolve_store.
            overwrite: Overwrite existing store
            region: Region to initialize (overrides default from platforms)
            bands: Bands to initialize (overrides store_config bands)

        Returns:
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
            logger.warning(
                "Reinitializing store (previous store at "
                f"{self._store.store_path} will be replaced)"
            )
            self._store.close_store()

        # --- All inputs resolved, construct and initialize ---

        self._store = GOESZarrStore(self._store_config_path)
        self._store.initialize_store(store_path, overwrite=overwrite)

        self._store.initialize_region(
            region=region,
            lat=self._regridder.target_lat,
            lon=self._regridder.target_lon,
            bands=bands,
            include_dqf=True,
            regridder=self._regridder,
        )

        logger.info(
            f"Store initialized at {self._store.store_path}, "
            f"region={region}, bands={bands}"
        )

        return self._store

    def initialize_dask_client(
            self,
            n_workers: int = None,
            threads_per_worker: int = None,
            memory_limit: str = None,
            scheduler_address: str = None
    ):
        """
        Initialize Dask distributed client.

        Parameters:
            n_workers: Number of workers (overrides pipeline_config)
            threads_per_worker: Threads per worker (overrides pipeline_config)
            memory_limit: Memory limit per worker (overrides pipeline_config)
            scheduler_address: Connect to existing cluster (overrides pipeline_config)

        Returns:
            None (sets self._dask_client)
        """
        logger.info("Initializing Dask client...")

        # Get Dask config from pipeline config
        dask_config = self._pipeline_config.get('dask', {})

        if not dask_config.get('enabled', True):
            logger.info("Dask client disabled in pipeline config")
            return

        try:
            from dask.distributed import Client, LocalCluster
        except ImportError:
            logger.warning(
                "dask.distributed not available. "
                "Install with: pip install dask[distributed]"
            )
            return

        # Determine scheduler address (override > config)
        scheduler_address = scheduler_address or dask_config.get('scheduler_address')

        # Connect to remote cluster or create local
        if scheduler_address:
            logger.info(f"Connecting to remote Dask cluster at {scheduler_address}")
            self._dask_client = Client(scheduler_address)

        else:
            local_config = dask_config.get('local', {})

            # Get parameters (override > config > defaults)
            n_workers = n_workers or local_config.get('n_workers', 4)
            threads_per_worker = threads_per_worker or local_config.get('threads_per_worker', 2)
            memory_limit = memory_limit or local_config.get('memory_limit', '4GB')

            logger.info(
                f"Creating local Dask cluster: "
                f"workers={n_workers}, "
                f"threads={threads_per_worker}, "
                f"memory={memory_limit}"
            )

            cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=threads_per_worker,
                memory_limit=memory_limit
            )

            self._dask_client = Client(cluster)

        # Apply Dask config overrides
        config_overrides = dask_config.get('config', {})
        if config_overrides:
            import dask
            for key, value in config_overrides.items():
                dask.config.set({key: value})

        logger.info(f"Dask client initialized: {self._dask_client}")
        logger.info(f"Dashboard available at: {self._dask_client.dashboard_link}")

    def initialize_all(
            self,
            store_path: Union[str, Path] = None,
            overwrite: bool = False,
            region: str = None,
            bands: List[int] = None,
            use_catalog: bool = None,
            use_dask_client: bool = None
    ):
        """
        Initialize all pipeline components.

        Parameters:
            store_path: Path to Zarr store
            overwrite: Overwrite existing store
            region: Region to initialize
            bands: Bands to initialize
            use_catalog: Build/load catalog for file discovery
            use_dask_client: Initialize Dask client

        Returns:
            None
        """
        logger.info("Initializing all pipeline components...")

        # Get defaults from pipeline config
        pipeline_defaults = self._pipeline_config.get('pipeline', {})

        # 1. Catalog (optional)
        if use_catalog is None:
            use_catalog = pipeline_defaults.get('use_catalog', True)

        if use_catalog:
            self.initialize_catalog()

        # 2. Observation (required)
        self.initialize_observation()

        # 3. Regridder (required)
        self.initialize_regridder()

        # 4. Store (required)
        self.initialize_store(store_path, overwrite, region, bands)

        # 5. Dask client (optional)
        if use_dask_client is None:
            dask_config = self._pipeline_config.get('dask', {})
            use_dask_client = dask_config.get('enabled', False)

        if use_dask_client:
            self.initialize_dask_client()

        logger.info("All components initialized successfully")

    ############################################################################################
    # PROCESSING - SINGLE OBSERVATION
    ############################################################################################

    def process_single_observation(
            self,
            time_idx: int,
            bands: List[int] = None,
            region: str = None
    ) -> int:
        """
        Process single observation (one timestep).

        Uses get_cmi(band) and get_dqf(band) directly rather than the stateful
        .band setter, which is cleaner in a loop. Uses isel_time(idx) which is
        the actual GOESMultiCloudObservation API (not .isel()).

        Parameters:
            time_idx: Index into observation.time
            bands: Bands to process (default from store_config)
            region: Target region (default from platforms[0])

        Returns:
            Store time index where data was written
        """
        if not self.is_initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_all() first.")

        bands = bands if bands is not None else self._default_bands
        region = region if region is not None else self._default_region

        # Extract metadata first (cheap, fails fast if dataset is malformed)
        timestamp = self._observation.time.isel(time=time_idx).values
        obs_ds = self._observation.isel_time(time_idx)
        platform_id = str(obs_ds['platform_id'].values)
        scan_mode = str(obs_ds['scan_mode'].values) if 'scan_mode' in obs_ds else None

        # Regrid CMI and DQF for each band
        cmi_data = {}
        dqf_data = {}

        for band in bands:
            cmi_3d = self._observation.get_cmi(band)
            dqf_3d = self._observation.get_dqf(band)

            cmi_2d = cmi_3d.isel(time=time_idx)
            dqf_2d = dqf_3d.isel(time=time_idx)

            cmi_regridded = self._regridder.regrid(cmi_2d)
            dqf_regridded = self._regridder.regrid_dqf(dqf_2d)

            cmi_data[band] = cmi_regridded.values
            dqf_data[band] = dqf_regridded.values

        # Append to store
        store_idx = self._store.append_observation(
            region=region,
            timestamp=timestamp,
            platform_id=platform_id,
            cmi_data=cmi_data,
            dqf_data=dqf_data,
            scan_mode=scan_mode,
        )

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
            bands: List[int] = None,
            region: str = None,
            show_progress: bool = True,
            continue_on_error: bool = None
    ):
        """
        Process batch of observations.

        Parameters:
            start_idx: Starting time index
            end_idx: Ending time index (exclusive)
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs (default from pipeline config)

        Returns:
            None
        """
        if not self.is_initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_all() first.")

        # Set defaults
        bands, region, end_idx = self._set_processing_defaults(bands, region, end_idx)

        if continue_on_error is None:
            batching = self._pipeline_config.get('batching', {})
            continue_on_error = batching.get('continue_on_error', True)

        if self._start_time is None:
            self._start_time = datetime.now(timezone.utc)

        logger.info(f"Processing batch: timesteps {start_idx} to {end_idx}")

        self._process_loop(
            indices=range(start_idx, end_idx),
            bands=bands,
            region=region,
            show_progress=show_progress,
            continue_on_error=continue_on_error,
        )

        self._store.update_temporal_coverage(region)

        logger.info(
            f"Batch complete: {self._processed_count} processed, "
            f"{self._failed_count} failed"
        )

    def process_all(
            self,
            bands: List[int] = None,
            region: str = None,
            show_progress: bool = True,
            continue_on_error: bool = None
    ):
        """
        Process all observations in dataset.

        Parameters:
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs

        Returns:
            None
        """
        self.process_batch(
            start_idx=0,
            end_idx=None,
            bands=bands,
            region=region,
            show_progress=show_progress,
            continue_on_error=continue_on_error
        )

    def process_time_range(
            self,
            start_time: Union[str, datetime, np.datetime64],
            end_time: Union[str, datetime, np.datetime64],
            bands: List[int] = None,
            region: str = None,
            show_progress: bool = True,
            continue_on_error: bool = None
    ):
        """
        Process observations within time range.

        Parameters:
            start_time: Start time
            end_time: End time
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs

        Returns:
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

        if continue_on_error is None:
            batching = self._pipeline_config.get('batching', {})
            continue_on_error = batching.get('continue_on_error', True)

        if self._start_time is None:
            self._start_time = datetime.now(timezone.utc)

        self._process_loop(
            indices=indices.tolist(),
            bands=bands,
            region=region,
            show_progress=show_progress,
            continue_on_error=continue_on_error,
            progress_desc="Processing time range",
        )

        self._store.update_temporal_coverage(region)

        logger.info(
            f"Time range complete: {self._processed_count} processed, "
            f"{self._failed_count} failed"
        )

    ############################################################################################
    # ERROR RECOVERY
    ############################################################################################

    def retry_failed(
            self,
            bands: List[int] = None,
            region: str = None,
            show_progress: bool = True,
            max_retries: int = None
    ):
        """
        Retry processing failed observations.

        Parameters:
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            max_retries: Max retries per observation (default from pipeline config)

        Returns:
            None
        """
        if not self._failed_indices:
            logger.info("No failed observations to retry")
            return

        if max_retries is None:
            batching = self._pipeline_config.get('batching', {})
            max_retries = batching.get('max_retries', 1)

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
            try:
                from tqdm import tqdm
                iterator = tqdm(iterator, desc="Retrying failed")
            except ImportError:
                pass

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

    def skip_failed(self):
        """Clear failed indices list (mark as intentionally skipped)."""
        skipped_count = len(self._failed_indices)
        self._failed_indices = []
        logger.info(f"Skipped {skipped_count} failed observations")

    def export_failed_indices(self, output_path: Union[str, Path]):
        """Export failed indices to JSON."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump({'failed_indices': self._failed_indices}, f, indent=2)

        logger.info(f"Exported {len(self._failed_indices)} failed indices to {output_path}")

    def import_failed_indices(self, input_path: Union[str, Path]):
        """Import failed indices from JSON."""
        input_path = Path(input_path)

        with open(input_path) as f:
            data = json.load(f)

        self._failed_indices = data['failed_indices'] # NOTE TO SELF: CONSIDER OPTION TO APPEND FAILED INDICES ALREADY STORED WITH JSON
        logger.info(f"Imported {len(self._failed_indices)} failed indices from {input_path}")

    ############################################################################################
    # CHECKPOINTING & STATE MANAGEMENT
    ############################################################################################

    def save_checkpoint(self, checkpoint_path: Union[str, Path]):
        """Save processing state to JSON checkpoint."""
        checkpoint_path = Path(checkpoint_path)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        state = self.processing_state
        state['timestamp'] = datetime.now(timezone.utc).isoformat()

        with open(checkpoint_path, 'w') as f:
            json.dump(state, f, indent=2)

        logger.info(f"Checkpoint saved to {checkpoint_path}")

    def load_checkpoint(self, checkpoint_path: Union[str, Path]):
        """Restore processing state from JSON checkpoint."""
        checkpoint_path = Path(checkpoint_path)
        if not checkpoint_path.exists():
            raise FileNotFoundError(f"Checkpoint file not found: {checkpoint_path}")

        with open(checkpoint_path) as f:
            state = json.load(f)

        self._processed_count = state['processed_count']
        self._failed_count = state['failed_count']
        self._failed_indices = state['failed_indices']
        self._last_processed_idx = state['last_processed_idx']

        if state.get('start_time'):
            self._start_time = datetime.fromisoformat(state['start_time'])

        logger.info(
            f"Checkpoint loaded: {self._processed_count} processed, "
            f"{self._failed_count} failed"
        )

    def resume_from_checkpoint(
            self,
            checkpoint_path: Union[str, Path],
            store_path: Union[str, Path],
            continue_processing: bool = True
    ):
        """
        Resume processing from a saved checkpoint.

        Opens the existing Zarr store (does not recreate it) and continues
        processing from the last successfully processed index.

        Parameters:
            checkpoint_path: Path to checkpoint JSON
            store_path: Path to existing Zarr store
            continue_processing: If True, continue processing after loading
        """
        logger.info("Resuming from checkpoint...")

        self.load_checkpoint(checkpoint_path)

        self.initialize_observation()
        self.initialize_regridder()

        # Open existing store (not create new)
        self._store = GOESZarrStore.from_existing(
            store_path=store_path,
            config_path=self._store_config_path,
            mode="r+",
        )
        self._store.rebuild_region_cache(self._default_region)

        if continue_processing:
            start_idx = self._last_processed_idx + 1
            logger.info(f"Continuing processing from index {start_idx}")
            self.process_batch(start_idx=start_idx)

    def _auto_checkpoint(self):
        """Automatically save checkpoint if enabled in config."""
        checkpoint_config = self._pipeline_config.get('checkpoints', {})

        if not checkpoint_config.get('enabled', True):
            return

        checkpoint_dir = checkpoint_config.get('directory', './checkpoints')
        checkpoint_dir = Path(os.path.expandvars(checkpoint_dir))
        checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Create checkpoint filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        checkpoint_path = checkpoint_dir / f"checkpoint_{timestamp}.json"

        self.save_checkpoint(checkpoint_path)

        # Clean up old checkpoints
        keep_last_n = checkpoint_config.get('keep_last_n', 5)
        if keep_last_n:
            self._cleanup_old_checkpoints(checkpoint_dir, keep_last_n)

    def _cleanup_old_checkpoints(self, checkpoint_dir: Path, keep_last_n: int):
        """Remove old checkpoints, keeping only the last N."""
        checkpoints = sorted(checkpoint_dir.glob('checkpoint_*.json'))

        if len(checkpoints) > keep_last_n:
            for old_checkpoint in checkpoints[:-keep_last_n]:
                old_checkpoint.unlink()
                logger.debug(f"Removed old checkpoint: {old_checkpoint}")

    ############################################################################################
    # VALIDATION & DIAGNOSTICS
    ############################################################################################

    def validate_setup(self) -> Dict[str, bool]:
        """
        Validate pipeline setup.

        Returns:
            Dictionary with validation results
        """
        results = {
            'observation_initialized': self._observation is not None,
            'regridder_initialized': self._regridder is not None,
            'store_initialized': self._store is not None,
            'catalog_available': self._catalog is not None,
            'dask_client_active': self.has_dask_client,
        }

        # Check disk space if requested
        validation_config = self._pipeline_config.get('validation', {})
        if validation_config.get('check_disk_space', False):
            required_gb = validation_config.get('required_free_space_gb', 100)
            results['sufficient_disk_space'] = self._check_disk_space(required_gb)

        # Log results
        for check, passed in results.items():
            status = "+" if passed else "x"
            logger.info(f"[{status}] {check}: {passed}")

        return results

    def _check_disk_space(self, required_gb: float) -> bool:
        """Check if sufficient disk space available."""
        try:
            import shutil

            store_path = self._store_config.get('store', {}).get('path', '.')

            stats = shutil.disk_usage(store_path)
            available_gb = stats.free / (1024 ** 3)

            logger.info(f"Available disk space: {available_gb:.1f} GB")

            return available_gb >= required_gb
        except Exception as e:
            logger.warning(f"Could not check disk space: {e}")
            return True  # Don't fail validation on error

    def estimate_output_size(self) -> Dict[str, float]:
        """
        Estimate output Zarr size.

        Returns:
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
            'uncompressed_gb': total_uncompressed / (1024 ** 3),
            'compressed_gb': total_compressed / (1024 ** 3),
            'compression_ratio': compression_ratio,
            'per_band_gb': (total_compressed / n_bands) / (1024 ** 3) if n_bands > 0 else 0.0,
        }

        logger.info(f"Estimated output size: {estimates['compressed_gb']:.1f} GB compressed")

        return estimates

    def _get_available_bands(self) -> List[int]:
        """Get list of band numbers with CMI variables in the observation dataset."""
        if self._observation is None:
            return []
        return [
            b for b in range(1, 17)
            if f'CMI_C{b:02d}' in self._observation.ds
        ]

    def summary(self) -> Dict[str, Any]:
        """
        Get comprehensive processing summary.

        Returns:
            Dictionary with processing statistics and configuration
        """
        summary = {
            'status': {
                'initialized': self.is_initialized,
                'has_catalog': self.has_catalog,
                'has_dask_client': self.has_dask_client,
            },
            'configuration': {
                'default_region': self._default_region,
                'default_bands': self._default_bands,
            },
            'processing': {
                'total_observations': self.total_observations,
                'processed_count': self._processed_count,
                'failed_count': self._failed_count,
                'success_rate': self.success_rate,
                'last_processed_idx': self._last_processed_idx,
            },
        }

        # Add timing if started
        if self._start_time:
            elapsed = datetime.now(timezone.utc) - self._start_time
            summary['processing']['elapsed_seconds'] = elapsed.total_seconds()
            summary['processing']['start_time'] = self._start_time.isoformat()

        # Add component info if initialized
        if self.is_initialized:
            summary['components'] = {
                'observation': {
                    'timesteps': len(self._observation.time),
                    'available_bands': self._get_available_bands(),
                },
                'regridder': {
                    'source_shape': self._regridder.source_shape,
                    'target_shape': self._regridder.target_shape,
                    'coverage_fraction': self._regridder.coverage_fraction,
                },
            }

        return summary

    def print_summary(self):
        """Pretty-print processing summary."""
        s = self.summary()

        print("\n" + "=" * 70)
        print("GOES PIPELINE ORCHESTRATOR SUMMARY")
        print("=" * 70)

        # Status
        print("\nSTATUS:")
        for key, value in s['status'].items():
            status = "+" if value else "x"
            print(f"  [{status}] {key}: {value}")

        # Configuration
        print("\nCONFIGURATION:")
        print(f"  Region: {s['configuration']['default_region']}")
        print(f"  Bands: {s['configuration']['default_bands']}")

        # Processing
        print("\nPROCESSING:")
        proc = s['processing']
        print(f"  Total observations: {proc['total_observations']}")
        print(f"  Processed: {proc['processed_count']}")
        print(f"  Failed: {proc['failed_count']}")
        print(f"  Success rate: {proc['success_rate']:.1%}")

        if 'elapsed_seconds' in proc:
            elapsed_hrs = proc['elapsed_seconds'] / 3600
            print(f"  Elapsed time: {elapsed_hrs:.2f} hours")

        # Components
        if 'components' in s:
            print("\nCOMPONENTS:")
            obs = s['components']['observation']
            print(f"  Observation: {obs['timesteps']} timesteps, bands {obs['available_bands']}")

            reg = s['components']['regridder']
            print(f"  Regridder: {reg['source_shape']} -> {reg['target_shape']}")
            print(f"  Coverage: {reg['coverage_fraction']:.1%}")

        print("=" * 70 + "\n")

    ############################################################################################
    # HELPER METHODS
    ############################################################################################

    def _set_processing_defaults(
            self,
            bands: Optional[List[int]] = None,
            region: Optional[str] = None,
            end_idx: Optional[int] = None
    ) -> tuple[List[int], str, int]:
        """
        Set default values for processing parameters.

        Parameters:
            bands: Optional list of bands to process
            region: Optional region to process
            end_idx: Optional end index (only used by process_batch)

        Returns:
            tuple: (bands, region, end_idx) with defaults applied
        """
        bands = bands if bands is not None else self._default_bands
        region = region if region is not None else self._default_region
        end_idx = end_idx if end_idx is not None else self.total_observations

        return bands, region, end_idx

    ############################################################################################
    # FINALIZATION & CLEANUP
    ############################################################################################

    def finalize_store(self):
        """Finalize Zarr store with temporal coverage updates and history."""
        if self._store:
            logger.info("Finalizing store...")
            self._store.finalize_dataset()
            logger.info("Store finalized")

    def close_dask_client(self):
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

    def finalize(self):
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

    def _load_config(self, config: Union[str, Path, dict]) -> dict:
        """Load configuration from file or dict with env var expansion."""
        if isinstance(config, dict):
            return config

        config_path = Path(config)

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path) as f:
            content = f.read()

        # Expand environment variables
        expanded = os.path.expandvars(content)

        # Parse based on extension
        suffix = config_path.suffix.lower()
        if suffix in {'.yaml', '.yml'}:
            return yaml.safe_load(expanded)
        elif suffix == '.json':
            return json.loads(expanded)
        else:
            # Default to YAML
            logger.warning(f"Unknown config extension '{suffix}', defaulting to YAML")
            return yaml.safe_load(expanded)

    def _setup_logging(self):
        """Configure logging based on pipeline config."""
        log_config = self._pipeline_config.get('logging', {})

        level = log_config.get('level', 'INFO')
        log_level = getattr(logging, level.upper(), logging.INFO)

        logger.setLevel(log_level)

        # Console handler (only add if no handlers exist)
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)

            fmt = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            date_fmt = log_config.get('date_format', None)
            formatter = logging.Formatter(fmt, datefmt=date_fmt)

            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # File handler (optional, avoid duplicates)
        log_file = log_config.get('log_file')
        if log_file:
            log_file = Path(os.path.expandvars(log_file))
            log_file.parent.mkdir(parents=True, exist_ok=True)

            existing_file_handlers = [
                h for h in logger.handlers
                if isinstance(h, logging.FileHandler)
                   and h.baseFilename == str(log_file.resolve())
            ]
            if not existing_file_handlers:
                fmt = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                date_fmt = log_config.get('date_format', None)
                file_formatter = logging.Formatter(fmt, datefmt=date_fmt)

                file_handler = logging.FileHandler(log_file)
                file_handler.setLevel(log_level)
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)

    def _get_configured_regions(self) -> List[str]:
        """Get all configured regions from store config."""
        goes_config = self._store_config.get('goes', {})
        regions = goes_config.get('orbital_slots')

        if not regions:
            raise ValueError(
                "No orbital slots defined in store_config['goes']['orbital_slots']. "
                "Must specify at least one (e.g., 'GOES-East', 'GOES-West')"
            )

        return regions

    def _get_default_bands(self) -> List[int]:
        """Get default bands from store config."""
        goes_config = self._store_config.get('goes', {})
        bands = goes_config.get('bands', multicloudconstants.BANDS)

        if not bands:
            raise ValueError(
                "No bands defined in store_config['goes']['bands']. "
                "Must specify at least one band (1-16)"
            )

        return bands

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

    def _increment_processed(self):
        """Increment processed counter and log milestones."""
        self._processed_count += 1

        # Log milestones
        log_interval = self._pipeline_config.get('progress', {}).get('log_interval', 100)
        if self._processed_count % log_interval == 0:
            logger.info(
                f"Processed {self._processed_count} observations "
                f"({self.success_rate:.1%} success rate)"
            )

    def _increment_failed(self, time_idx: int, error: Exception):
        """Record failed observation."""
        self._failed_count += 1
        self._failed_indices.append(time_idx)

        logger.error(
            f"Failed to process observation at time index {time_idx}: {error}"
        )

    def _should_checkpoint(self, current_idx: int) -> bool:
        """Check if checkpoint should be saved."""
        checkpoint_config = self._pipeline_config.get('checkpoints', {})

        if not checkpoint_config.get('enabled', True):
            return False

        batching = self._pipeline_config.get('batching', {})
        interval = batching.get('checkpoint_interval', 500)

        if interval is None:
            return False

        return (current_idx + 1) % interval == 0

    def _get_files_from_catalog(
            self,
            time_range: Optional[tuple] = None,
    ) -> List[Path]:
        """
        Get file paths from the catalog, optionally filtered by time range.

        The GOESMetadataCatalog observations DataFrame stores absolute file paths
        in the 'file_path' column (set by scan_file). There is no 'filename' or
        'band_id' column. MCMIP files contain all 16 bands per file, so band-level
        filtering at the file level is not applicable.

        Catalog filters for orbital_slot and scene_id can be applied from
        the pipeline config's catalog section.

        Parameters:
            time_range: Optional (start, end) datetime tuple

        Returns:
            List of Path objects
        """
        if self._catalog is None:
            raise RuntimeError("Catalog not initialized")

        # Start with full observations DataFrame
        # .observations property returns a copy of the internal DataFrame
        df = self._catalog.observations

        if df.empty:
            logger.warning("Catalog observations DataFrame is empty")
            return []

        # Apply time range filter
        if time_range is not None:
            start, end = time_range
            start_dt = pd.to_datetime(start)
            end_dt = pd.to_datetime(end)

            df = df[
                (df['time_coverage_start'] >= start_dt) &
                (df['time_coverage_end'] <= end_dt)
            ]
            logger.info(f"Time filter: {len(df)} files in range {start} to {end}")

        # Apply orbital slot filter from pipeline catalog config
        catalog_config = self._pipeline_config.get('catalog', {})

        orbital_slot = catalog_config.get('orbital_slot')
        if orbital_slot is not None:
            df = df[df['orbital_slot'] == orbital_slot]
            logger.info(f"Orbital slot filter ({orbital_slot}): {len(df)} files")

        # Apply scene_id filter from pipeline catalog config
        scene_id = catalog_config.get('scene_id')
        if scene_id is not None:
            df = df[df['scene_id'] == scene_id]
            logger.info(f"Scene filter ({scene_id}): {len(df)} files")

        # The 'file_path' column contains absolute paths (set by scan_file)
        file_list = [Path(fp) for fp in df['file_path'].tolist()]

        logger.info(f"Catalog query returned {len(file_list)} files")

        return file_list

    def _process_loop(
            self,
            indices,
            bands: List[int],
            region: str,
            show_progress: bool,
            continue_on_error: bool,
            progress_desc: str = "Processing observations"
    ):
        """
        Core processing loop shared by process_batch and process_time_range.

        Parameters:
            indices: Iterable of time indices to process
            bands: Bands to process
            region: Target region
            show_progress: Show progress bar
            continue_on_error: Continue if error occurs
            progress_desc: Description for progress bar
        """
        if show_progress:
            try:
                from tqdm import tqdm
                indices = tqdm(indices, desc=progress_desc)
            except ImportError:
                logger.warning("tqdm not available, progress bar disabled")

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

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.finalize()

    def __repr__(self) -> str:
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