from pathlib import Path
from typing import Optional, Union, List, Dict, Any
from datetime import datetime
import logging
import numpy as np
import pandas as pd
import yaml
import json
import os

from ..data.goes.multicloudcatalog import GOESMetadataCatalog
from ..data.goes.multicloud import GOESMultiCloudObservation
from ..regrid.geostationary import GeostationaryRegridder
from ..store.datasets import GOESZarrStore

logger = logging.getLogger(__name__)


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
            store_config: Union[str, Path, dict],
            pipeline_config: Union[str, Path, dict] = None,
            catalog: Optional[GOESMetadataCatalog] = None
    ):
        """
        Initialize pipeline orchestrator.

        Parameters:
            obs_config: Path to YAML or dict with GOESMultiCloudObservation config
            store_config: Path to YAML or dict with GOESZarrStore config
            pipeline_config: Path to YAML or dict with pipeline orchestration config
            catalog: Optional pre-built GOESMetadataCatalog
        """
        # Load configurations
        self._obs_config = self._load_config(obs_config)
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
        self._default_region = self._get_default_region()
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
        """Fraction of observations processed successfully (0.0 to 1.0)."""
        total = self._processed_count + self._failed_count
        if total == 0:
            return 0.0
        return self._processed_count / total

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
            elapsed = datetime.utcnow() - self._start_time
            state['elapsed_seconds'] = elapsed.total_seconds()

        return state

    @property
    def obs_config(self) -> dict:
        """Observation configuration (read-only copy)."""
        return self._obs_config.copy()

    @property
    def store_config(self) -> dict:
        """Store configuration (read-only copy)."""
        return self._store_config.copy()

    @property
    def pipeline_config(self) -> dict:
        """Pipeline configuration (read-only copy)."""
        return self._pipeline_config.copy()

    ############################################################################################
    # COMPONENT INITIALIZATION
    ############################################################################################

    def initialize_catalog(
            self,
            force_rebuild: bool = False,
            parallel: bool = None,
            max_workers: int = None
    ) -> GOESMetadataCatalog:
        """
        Initialize or load metadata catalog.

        Parameters:
            force_rebuild: Rebuild catalog even if CSV exists
            parallel: Use parallel file scanning (default from pipeline config)
            max_workers: Number of workers for parallel scanning (default from pipeline config)

        Returns:
            GOESMetadataCatalog instance
        """
        logger.info("Initializing metadata catalog...")

        # Get data access config (from observation config)
        data_config = self._obs_config.get('data_access', {})
        file_dir = Path(os.path.expandvars(data_config['file_dir']))

        # Determine catalog output directory
        catalog_dir = data_config.get('catalog_dir', file_dir / 'catalog')
        catalog_dir = Path(os.path.expandvars(str(catalog_dir)))

        observations_csv = catalog_dir / 'observations.csv'

        # Load existing or build new
        if observations_csv.exists() and not force_rebuild:
            logger.info(f"Loading existing catalog from {observations_csv}")
            self._catalog = GOESMetadataCatalog.from_csv(observations_csv)
        else:
            logger.info(f"Building new catalog from {file_dir}")

            # Create catalog
            self._catalog = GOESMetadataCatalog(
                file_dir=file_dir,
                output_dir=catalog_dir,
                pattern=data_config.get('pattern', '*.nc'),
                recursive=data_config.get('recursive', True)
            )

            # Get parallel settings from pipeline config if not provided
            if parallel is None:
                catalog_config = self._pipeline_config.get('catalog', {})
                parallel = catalog_config.get('parallel', True)

            if max_workers is None:
                catalog_config = self._pipeline_config.get('catalog', {})
                max_workers = catalog_config.get('max_workers')

            # Scan files
            if parallel and max_workers:
                self._catalog.scan(max_workers=max_workers)
            elif parallel:
                self._catalog.scan()  # Use default workers
            else:
                self._catalog.scan(max_workers=1)

            # Export to CSV
            self._catalog.to_csv()
            logger.info(f"Catalog saved to {catalog_dir}")

        logger.info(f"Catalog loaded: {len(self._catalog.observations)} observations")

        return self._catalog

    def initialize_observation(
            self,
            file_list: Optional[List[Path]] = None,
            time_range: Optional[tuple] = None,
            bands: Optional[List[int]] = None
    ) -> GOESMultiCloudObservation:
        """
        Initialize GOESMultiCloudObservation.

        Parameters:
            file_list: Explicit file list (overrides catalog filtering)
            time_range: (start, end) datetime tuple for filtering catalog
            bands: Bands to filter in catalog

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

            file_list = self._get_files_from_catalog(time_range, bands)

        if not file_list:
            raise ValueError("No files to process. Check catalog filters or provide explicit file_list.")

        logger.info(f"Selected {len(file_list)} files")

        # Create observation config with file list
        obs_config_copy = self._obs_config.copy()

        # Update data_access section with file list
        if 'data_access' not in obs_config_copy:
            obs_config_copy['data_access'] = {}

        obs_config_copy['data_access']['file_list'] = file_list

        # Create observation
        self._observation = GOESMultiCloudObservation(obs_config_copy)

        logger.info(
            f"Observation initialized: "
            f"{len(self._observation.time)} timesteps, "
            f"bands={self._observation.available_bands}"
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
        regrid_config = self._obs_config.get('regridding', {})

        # Get reference band (override > obs_config > default)
        if reference_band is None:
            reference_band = regrid_config.get('reference_band', 7)

        # Get load_cached (override > obs_config > default)
        if load_cached is None:
            load_cached = regrid_config.get('load_cached', True)

        # Set observation band to reference for coordinate extraction
        self._observation.band = reference_band

        # Determine target grid
        if target_grid is not None:
            # Explicit target grid provided
            target_lat = target_grid['lat']
            target_lon = target_grid['lon']

            self._regridder = GeostationaryRegridder(
                source_x=self._observation.x.values,
                source_y=self._observation.y.values,
                projection=self._observation.projection,
                target_lat=target_lat,
                target_lon=target_lon,
                weights_dir=regrid_config.get('weights_dir'),
                load_cached=load_cached,
                reference_band=reference_band
            )

        else:
            # Use target grid from obs_config
            target_config = regrid_config.get('target', {})

            if 'lat_min' in target_config and 'lon_min' in target_config:
                # Explicit bounds in config
                res = target_config.get('resolution', 0.02)

                # Handle separate lat/lon resolutions
                lat_res = target_config.get('lat_resolution', res)
                lon_res = target_config.get('lon_resolution', res)

                target_lat = np.arange(
                    target_config['lat_min'],
                    target_config['lat_max'] + lat_res,
                    lat_res
                )
                target_lon = np.arange(
                    target_config['lon_min'],
                    target_config['lon_max'] + lon_res,
                    lon_res
                )

                self._regridder = GeostationaryRegridder(
                    source_x=self._observation.x.values,
                    source_y=self._observation.y.values,
                    projection=self._observation.projection,
                    target_lat=target_lat,
                    target_lon=target_lon,
                    weights_dir=regrid_config.get('weights_dir'),
                    load_cached=load_cached,
                    reference_band=reference_band
                )

            else:
                # Auto-compute from source bounds
                resolution = target_config.get('resolution', 0.02)

                self._regridder = GeostationaryRegridder(
                    source_x=self._observation.x.values,
                    source_y=self._observation.y.values,
                    projection=self._observation.projection,
                    target_resolution=resolution,
                    weights_dir=regrid_config.get('weights_dir'),
                    load_cached=load_cached,
                    reference_band=reference_band
                )

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

        Parameters:
            store_path: Path to Zarr store (overrides store_config)
            overwrite: Overwrite existing store
            region: Region to initialize (overrides default from platforms)
            bands: Bands to initialize (overrides store_config bands)

        Returns:
            GOESZarrStore instance
        """
        logger.info("Initializing Zarr store...")

        # Ensure regridder is initialized
        if self._regridder is None:
            self.initialize_regridder()

        # Create store from store_config
        self._store = GOESZarrStore(self._store_config)

        # Determine store path (override > store_config > error)
        if store_path is None:
            store_path = self._store_config.get('store', {}).get('path')

        if store_path is None:
            raise ValueError(
                "store_path must be provided or set in store_config['store']['path']"
            )

        store_path = Path(os.path.expandvars(str(store_path)))

        # Initialize store
        self._store.initialize_store(store_path, overwrite=overwrite)

        # Determine region (override > default from platforms)
        if region is None:
            region = self._default_region

        # Determine bands (override > store_config)
        if bands is None:
            bands = self._default_bands

        # Initialize region with regridder metadata
        self._store.initialize_region(
            region=region,
            lat=self._regridder.target_lat,
            lon=self._regridder.target_lon,
            bands=bands,
            include_dqf=True,
            regridder=self._regridder
        )

        logger.info(
            f"Store initialized at {store_path}, "
            f"region={region}, "
            f"bands={bands}"
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
            # Remote cluster
            logger.info(f"Connecting to remote Dask cluster at {scheduler_address}")
            self._dask_client = Client(scheduler_address)

        else:
            # Local cluster
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

            # Create cluster
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

        Parameters:
            time_idx: Index into observation.time
            bands: Bands to process (default from store_config)
            region: Target region (default from platforms[0])

        Returns:
            Store time index where data was written
        """
        if not self.is_initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_all() first.")

        # Set defaults
        if bands is None:
            bands = self._default_bands

        if region is None:
            region = self._default_region

        # Extract observation at time index
        obs_single = self._observation.isel(time=time_idx)

        # Regrid CMI and DQF for each band
        cmi_data = {}
        dqf_data = {}

        for band in bands:
            # Set band on observation
            self._observation.band = band

            # Get data at this timestep (may be Dask or NumPy)
            cmi = self._observation.cmi.isel(time=time_idx)
            dqf = self._observation.dqf.isel(time=time_idx)

            # Regrid (returns xr.DataArray, may have Dask backing)
            cmi_regridded = self._regridder.regrid(cmi)
            dqf_regridded = self._regridder.regrid_dqf(dqf)

            # Store (will auto-compute Dask to NumPy if needed)
            cmi_data[band] = cmi_regridded
            dqf_data[band] = dqf_regridded

        # Append to store
        store_idx = self._store.append_observation(
            region=region,
            timestamp=obs_single.time.values,
            platform_id=obs_single.platform_id.values.item() if hasattr(obs_single.platform_id.values, 'item') else str(
                obs_single.platform_id.values),
            scan_mode=obs_single.scan_mode.values.item() if hasattr(obs_single.scan_mode, 'values') else None,
            cmi_data=cmi_data,
            dqf_data=dqf_data
        )

        # Update state
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
        if end_idx is None:
            end_idx = self.total_observations

        if bands is None:
            bands = self._default_bands

        if region is None:
            region = self._default_region

        if continue_on_error is None:
            batching = self._pipeline_config.get('batching', {})
            continue_on_error = batching.get('continue_on_error', True)

        # Start timing
        if self._start_time is None:
            self._start_time = datetime.utcnow()

        logger.info(f"Processing batch: timesteps {start_idx} to {end_idx}")

        # Create iterator
        iterator = range(start_idx, end_idx)

        # Add progress bar if requested
        if show_progress:
            try:
                from tqdm import tqdm
                iterator = tqdm(iterator, desc="Processing observations")
            except ImportError:
                logger.warning("tqdm not available, progress bar disabled")

        # Process each timestep
        for time_idx in iterator:
            try:
                self.process_single_observation(time_idx, bands, region)

                # Check if checkpoint needed
                if self._should_checkpoint(time_idx):
                    self._auto_checkpoint()

            except Exception as e:
                self._increment_failed(time_idx, e)

                if continue_on_error:
                    continue
                else:
                    raise

        # Update temporal coverage in store
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
        if bands is None:
            bands = self._default_bands

        if region is None:
            region = self._default_region

        if continue_on_error is None:
            batching = self._pipeline_config.get('batching', {})
            continue_on_error = batching.get('continue_on_error', True)

        # Start timing
        if self._start_time is None:
            self._start_time = datetime.utcnow()

        # Create iterator
        iterator = indices

        # Add progress bar if requested
        if show_progress:
            try:
                from tqdm import tqdm
                iterator = tqdm(iterator, desc="Processing time range")
            except ImportError:
                logger.warning("tqdm not available, progress bar disabled")

        # Process each timestep
        for time_idx in iterator:
            try:
                self.process_single_observation(int(time_idx), bands, region)

                # Check if checkpoint needed
                if self._should_checkpoint(time_idx):
                    self._auto_checkpoint()

            except Exception as e:
                self._increment_failed(int(time_idx), e)

                if continue_on_error:
                    continue
                else:
                    raise

        # Update temporal coverage
        self._store.update_temporal_coverage(region)

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

        logger.info(f"Retrying {len(self._failed_indices)} failed observations")

        # Set defaults
        if bands is None:
            bands = self._default_bands

        if region is None:
            region = self._default_region

        # Copy failed indices (will be modified during retry)
        failed_copy = self._failed_indices.copy()
        retry_count = {}

        # Clear failed indices
        self._failed_indices = []
        retry_failed_count = 0

        # Create iterator
        iterator = failed_copy

        if show_progress:
            try:
                from tqdm import tqdm
                iterator = tqdm(iterator, desc="Retrying failed")
            except ImportError:
                pass

        for time_idx in iterator:
            # Track retries
            retry_count[time_idx] = retry_count.get(time_idx, 0) + 1

            if retry_count[time_idx] > max_retries:
                logger.warning(f"Max retries exceeded for time index {time_idx}")
                self._failed_indices.append(time_idx)
                continue

            try:
                self.process_single_observation(time_idx, bands, region)
            except Exception as e:
                retry_failed_count += 1
                self._failed_indices.append(time_idx)
                logger.error(f"Retry failed for time index {time_idx}: {e}")

        logger.info(
            f"Retry complete: {len(failed_copy) - retry_failed_count} succeeded, "
            f"{retry_failed_count} still failed"
        )

    def skip_failed(self):
        """Clear failed indices list (mark as intentionally skipped)."""
        skipped_count = len(self._failed_indices)
        self._failed_indices = []
        logger.info(f"Skipped {skipped_count} failed observations")

    def export_failed_indices(self, output_path: Union[str, Path]):
        """
        Export failed indices to JSON.

        Parameters:
            output_path: Path to save failed indices

        Returns:
            None
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump({'failed_indices': self._failed_indices}, f, indent=2)

        logger.info(f"Exported {len(self._failed_indices)} failed indices to {output_path}")

    def import_failed_indices(self, input_path: Union[str, Path]):
        """
        Import failed indices from JSON.

        Parameters:
            input_path: Path to load failed indices

        Returns:
            None
        """
        input_path = Path(input_path)

        with open(input_path) as f:
            data = json.load(f)

        self._failed_indices = data['failed_indices']
        logger.info(f"Imported {len(self._failed_indices)} failed indices from {input_path}")

    ############################################################################################
    # CHECKPOINTING & STATE MANAGEMENT
    ############################################################################################

    def save_checkpoint(self, checkpoint_path: Union[str, Path]):
        """
        Save processing state to JSON checkpoint.

        Parameters:
            checkpoint_path: Path to save checkpoint

        Returns:
            None
        """
        checkpoint_path = Path(checkpoint_path)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        # Get state
        state = self.processing_state
        state['timestamp'] = datetime.utcnow().isoformat()

        # Save
        with open(checkpoint_path, 'w') as f:
            json.dump(state, f, indent=2)

        logger.info(f"Checkpoint saved to {checkpoint_path}")

    def load_checkpoint(self, checkpoint_path: Union[str, Path]):
        """
        Restore processing state from JSON checkpoint.

        Parameters:
            checkpoint_path: Path to load checkpoint

        Returns:
            None
        """
        checkpoint_path = Path(checkpoint_path)

        with open(checkpoint_path) as f:
            state = json.load(f)

        # Restore state
        self._processed_count = state['processed_count']
        self._failed_count = state['failed_count']
        self._failed_indices = state['failed_indices']
        self._last_processed_idx = state['last_processed_idx']

        if state['start_time']:
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
        Resume from checkpoint.

        Parameters:
            checkpoint_path: Path to checkpoint
            store_path: Path to existing Zarr store
            continue_processing: If True, continue processing after loading

        Returns:
            None
        """
        logger.info("Resuming from checkpoint...")

        # Load checkpoint
        self.load_checkpoint(checkpoint_path)

        # Initialize components without catalog
        self.initialize_observation()
        self.initialize_regridder()

        # Open existing store (don't overwrite)
        self.initialize_store(store_path, overwrite=False)

        if continue_processing:
            # Continue from last processed index
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
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
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
            status = "✓" if passed else "✗"
            logger.info(f"{status} {check}: {passed}")

        return results

    def _check_disk_space(self, required_gb: float) -> bool:
        """Check if sufficient disk space available."""
        try:
            import shutil

            if self._store:
                store_path = self._store._store.path
            else:
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
            'per_band_gb': (total_compressed / n_bands) / (1024 ** 3),
        }

        logger.info(f"Estimated output size: {estimates['compressed_gb']:.1f} GB compressed")

        return estimates

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
            elapsed = datetime.utcnow() - self._start_time
            summary['processing']['elapsed_seconds'] = elapsed.total_seconds()
            summary['processing']['start_time'] = self._start_time.isoformat()

        # Add component info if initialized
        if self.is_initialized:
            summary['components'] = {
                'observation': {
                    'timesteps': len(self._observation.time),
                    'available_bands': self._observation.available_bands,
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
        summary = self.summary()

        print("\n" + "=" * 70)
        print("GOES PIPELINE ORCHESTRATOR SUMMARY")
        print("=" * 70)

        # Status
        print("\nSTATUS:")
        for key, value in summary['status'].items():
            status = "✓" if value else "✗"
            print(f"  {status} {key}: {value}")

        # Configuration
        print("\nCONFIGURATION:")
        print(f"  Region: {summary['configuration']['default_region']}")
        print(f"  Bands: {summary['configuration']['default_bands']}")

        # Processing
        print("\nPROCESSING:")
        proc = summary['processing']
        print(f"  Total observations: {proc['total_observations']}")
        print(f"  Processed: {proc['processed_count']}")
        print(f"  Failed: {proc['failed_count']}")
        print(f"  Success rate: {proc['success_rate']:.1%}")

        if 'elapsed_seconds' in proc:
            elapsed_hrs = proc['elapsed_seconds'] / 3600
            print(f"  Elapsed time: {elapsed_hrs:.2f} hours")

        # Components
        if 'components' in summary:
            print("\nCOMPONENTS:")
            obs = summary['components']['observation']
            print(f"  Observation: {obs['timesteps']} timesteps, bands {obs['available_bands']}")

            reg = summary['components']['regridder']
            print(f"  Regridder: {reg['source_shape']} → {reg['target_shape']}")
            print(f"  Coverage: {reg['coverage_fraction']:.1%}")

        print("=" * 70 + "\n")

    ############################################################################################
    # FINALIZATION & CLEANUP
    ############################################################################################

    def finalize_store(self):
        """Finalize Zarr store."""
        if self._store:
            logger.info("Finalizing store...")
            self._store.finalize_dataset()
            logger.info("Store finalized")

    def close_dask_client(self):
        """Shutdown Dask client."""
        if self._dask_client:
            logger.info("Closing Dask client...")
            self._dask_client.close()
            self._dask_client = None
            logger.info("Dask client closed")

    def finalize(self):
        """Finalize pipeline and cleanup resources."""
        logger.info("Finalizing pipeline...")

        self.finalize_store()
        self.close_dask_client()

        if self._store:
            self._store.close_store()

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

        # Read file
        with open(config_path) as f:
            content = f.read()

        # Expand environment variables
        expanded = os.path.expandvars(content)

        # Parse YAML
        return yaml.safe_load(expanded)

    def _setup_logging(self):
        """Configure logging based on pipeline config."""
        log_config = self._pipeline_config.get('logging', {})

        # Log level
        level = log_config.get('level', 'INFO')
        log_level = getattr(logging, level.upper())

        # Configure logger
        logger.setLevel(log_level)

        # Console handler
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            formatter_ = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter_)
            logger.addHandler(console_handler)

        # File handler (optional)
        log_file = log_config.get('log_file')
        if log_file:
            log_file = Path(os.path.expandvars(log_file))
            log_file.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter_)
            logger.addHandler(file_handler)

    def _get_default_region(self) -> str:
        """Get default region from store config."""
        goes_config = self._store_config.get('goes', {})
        platforms = goes_config.get('platforms', [])

        if not platforms:
            raise ValueError(
                "No platforms defined in store_config['goes']['platforms']. "
                "Must specify at least one platform (e.g., 'GOES-East', 'GOES-West')"
            )

        return platforms[0]

    def _get_default_bands(self) -> List[int]:
        """Get default bands from store config."""
        goes_config = self._store_config.get('goes', {})
        bands = goes_config.get('bands', list(range(1, 17)))

        if not bands:
            raise ValueError(
                "No bands defined in store_config['goes']['bands']. "
                "Must specify at least one band (1-16)"
            )

        return bands

    def _get_batch_size(self) -> int:
        """Get batch size from pipeline config or auto-calculate."""
        batching = self._pipeline_config.get('batching', {})
        batch_size = batching.get('batch_size')

        if batch_size is not None:
            return batch_size

        # Auto-calculate based on available memory
        try:
            import psutil
            available_gb = psutil.virtual_memory().available / (1024 ** 3)
            # Use 50% of available memory, ~100MB per observation
            batch_size = int((available_gb * 0.5 * 1024) / 100)
            batch_size = max(10, min(batch_size, 1000))
            logger.info(f"Auto-calculated batch size: {batch_size}")
            return batch_size
        except ImportError:
            logger.warning("psutil not available, using default batch size")
            return 100

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
            bands: Optional[List[int]] = None
    ) -> List[Path]:
        """
        Get a list of file paths from the catalog, filtered by time range and bands.

        This method takes an optional time range tuple (start, end) and an optional list of bands.
        It returns a list of file paths that match the filters.

        The method first gets the full catalog as a pandas DataFrame.
        It then applies the time range filter, if provided, by selecting rows where the start time is greater than or equal to the start of the time range and the end time is less than or equal to the end of the time range.
        It then applies the orbital slot filter, if provided in the catalog config, by selecting rows where the orbital slot matches the provided value.
        It then applies the band filter, if provided, by selecting rows where the band ID is in the provided list.
        It then applies the scene ID filter, if provided in the catalog config, by selecting rows where the scene ID matches the provided value.
        Finally, it converts the filtered DataFrame to a list of file paths by combining the file directory with the filename column.
        """
        if self._catalog is None:
            raise RuntimeError("Catalog not initialized")

        # Get data config
        data_config = self._obs_config.get('data_access', {})
        file_dir = Path(os.path.expandvars(data_config['file_dir']))

        # Start with full catalog
        df = self._catalog.observations.copy()

        # Apply time range filter
        if time_range is not None:
            start, end = time_range
            start_dt = pd.to_datetime(start)
            end_dt = pd.to_datetime(end)

            # Select rows where start time is greater than or equal to the start of the time range
            # and end time is less than or equal to the end of the time range
            df = df[
                (df['time_coverage_start'] >= start_dt) &
                (df['time_coverage_end'] <= end_dt)
                ]
            logger.info(f"Time filter: {len(df)} files in range {start} to {end}")

        # Apply orbital slot filter (from catalog config)
        catalog_config = self._pipeline_config.get('catalog', {})
        if 'orbital_slot' in catalog_config:
            # Select rows where the orbital slot matches the provided value
            df = df[df['orbital_slot'] == catalog_config['orbital_slot']]
            logger.info(f"Orbital slot filter: {len(df)} files")

        # Apply band filter
        if bands is not None:
            # Select rows where the band ID is in the provided list
            df = df[df['band_id'].isin(bands)]
            logger.info(f"Band filter: {len(df)} files")

        # Apply scene ID filter (from catalog config)
        if 'scene_id' in catalog_config:
            # Select rows where the scene ID matches the provided value
            df = df[df['scene_id'] == catalog_config['scene_id']]
            logger.info(f"Scene filter: {len(df)} files")

        # Convert to file paths
        file_list = [file_dir / filename for filename in df['filename'].tolist()]

        return file_list

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
        """String representation of the GOESPipelineOrchestrator object.

        This method returns a string that represents the object, including its
        status, number of observations, number of processed and failed
        observations, and its success rate (as a percentage).

        The string is formatted to include line breaks and indentation, making it
        easier to read and understand.
        """
        status = "initialized" if self.is_initialized else "not initialized"

        if self.is_initialized:
            return (
                f"GOESPipelineOrchestrator(\n"
                f"    status={status},\n"  # Status: initialized or not
                f"    observations={self.total_observations},\n"  # Total number of observations
                f"    processed={self._processed_count},\n"  # Number of observations processed
                f"    failed={self._failed_count},\n"  # Number of observations failed
                f"    success_rate={self.success_rate:.1%}\n"  # Success rate as a percentage
                f")"
            )
        else:
            return f"GOESPipelineOrchestrator(status={status})"