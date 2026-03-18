from ..zarrstore import ZarrStoreBuilder
from pathlib import Path
import numpy as np
from datetime import datetime, timezone
from typing import Union, Optional, TYPE_CHECKING
import logging
import json


from ...utils.grid_utils import validate_longitude_monotonic

from ...data.goes import multicloudconstants

if TYPE_CHECKING:
    from ...regrid import GeostationaryRegridder

logger = logging.getLogger(__name__)


class GOESZarrStore(ZarrStoreBuilder):
    """
    CF-compliant Zarr store builder for GOES ABI imagery.
    Stores regridded lat/lon data with full CF metadata.
    Fully configurable via YAML - supports ACDD-1.3, provenance tracking, and extended DQF flags.
    """

    ############################################################################################
    # CLASS CONSTANTS (FALLBACK IF NOT IN CONFIG)
    ############################################################################################

    CELL_METHODS = 'time: point latitude,longitude: mean'  # indicates what each pixel means in metadata

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(self, config_path: Union[str, Path]):
        """
        Input: path to config YAML/JSON
        Output: None
        Job: Call parent init, load GOES-specific config (bands, metadata, etc.)
        """
        super().__init__(config_path)

        # Load GOES-specific configuration
        self._load_goes_config()

        # Per-region caches (populated by initialize_region)
        self._region_shapes: dict[str, tuple[int, int]] = {}
        self._region_bands: dict[str, set[int]] = {}

    def _load_goes_config(self):
        """Load and validate GOES-specific configuration"""
        goes_config = self.config.get('goes', {})

        # Load regions (platforms)
        self.REGIONS = multicloudconstants.VALID_ORBITAL_SLOTS

        # Load bands to process
        self.BANDS = goes_config.get('bands', multicloudconstants.ALL_BANDS)

        # Load band metadata (with fallback to defaults)
        config_band_metadata = goes_config.get('band_metadata', multicloudconstants.DEFAULT_BAND_METADATA)
        config_band_metadata = {int(k): v for k, v in config_band_metadata.items()} # JIC someone uses "1" instead of 1 in config

        self.BAND_METADATA = {}

        for band in range(1, 17):
            if band in config_band_metadata:
                # Use config metadata
                self.BAND_METADATA[band] = config_band_metadata[band]
            else:
                # Fallback to default
                self.BAND_METADATA[band] = multicloudconstants.DEFAULT_BAND_METADATA.get(band)

        logger.info(f"Loaded GOES config: regions={self.REGIONS}, bands={self.BANDS}")

    ############################################################################################
    # STORE INITIALIZATION
    ############################################################################################

    def initialize_store(self, store_path: Union[str, Path], overwrite: bool = False):
        """
        Input: store path, overwrite flag
        Output: None
        Job: Create store, root group with CF global attrs from config


        """
        self.create_store(store_path, overwrite=overwrite)

        global_attrs = self._cf_global_attrs()
        self.set_attrs("/", global_attrs, merge=False)

        logger.info(f"Initialized GOES Zarr store at {store_path}")

    def initialize_region(
            self,
            region: str,
            lat: np.ndarray,
            lon: np.ndarray,
            lat_preset: Optional[str],
            lon_preset: Optional[str],
            time_preset: Optional[str],
            aux_preset: Optional[str],
            cmi_preset: Optional[str],
            dqf_preset: Optional[str],
            bands: Optional[list] = None,
            include_dqf: bool = True,
            regridder: Optional['GeostationaryRegridder'] = None
    ):
        """
        Input: region name ('GOES-East' or 'GOES-West' or 'GOES-Test')
               lat - 1D array of latitudes (degrees_north), must be monotonic
               lon - 1D array of longitudes (degrees_east), must be monotonic
               bands - which bands to create (default: from config or all 16)
               include_dqf - whether to create DQF arrays
               regridder - GeostationaryRegridder instance for full provenance
        Output: None
        Job: Create region group with full provenance attrs,
             create dimension coords (lat, lon, time),
             create auxiliary coords (platform_id, scan_mode),
             create all CMI and DQF arrays with CF attrs
        """
        if region not in self.REGIONS:
            raise ValueError(f"Invalid region '{region}'. Must be one of {self.REGIONS}")

        # Use bands from config if not specified
        if bands is None:
            bands = self.BANDS

        # Validate lat/lon are monotonic
        if not (np.all(np.diff(lat) > 0) or np.all(np.diff(lat) < 0)):
            raise ValueError("Latitude array must be monotonic")
        if not validate_longitude_monotonic(lon):
            raise ValueError(
                      "Longitude array must be monotonic "
                      "(checked in 0-360 space for antimeridian-crossing grids)"
                  )

        # Get regridding provenance if available
        regridding_provenance = None
        if regridder is not None:
            regridding_provenance = regridder.regridding_provenance()

        # Create region group with full metadata
        region_attrs = self._cf_region_attrs(lat, lon, regridding_provenance)
        self.create_group(region, attrs=region_attrs)

        logger.info(f"Creating region '{region}' with lat={len(lat)}, lon={len(lon)}, bands={bands}")

        # Create dimension coordinates
        self._create_lat_coord(region, lat, preset = lat_preset)
        self._create_lon_coord(region, lon, preset = lon_preset)
        self._create_time_coord(region, preset = time_preset)

        # Create auxiliary coordinates
        self._create_auxiliary_coords(region, preset = aux_preset)

        # Create CMI and DQF arrays for each band
        for band in bands:
            self._create_cmi_array(region, band, preset = cmi_preset)
            if include_dqf:
                self._create_dqf_array(region, band, preset = dqf_preset)

                # Cache for fast-path validation during append
        self._region_shapes[region] = (len(lat), len(lon))
        self._region_bands[region] = set(bands)
        logger.info(f"Initialized region '{region}' with {len(bands)} bands")

    def rebuild_region_cache(self, region: str):
        """
        Rebuild region caches from an existing store.

        Used when opening a store via from_existing, where initialize_region
        was not called and the caches are empty.

        :param region: Region identifier to rebuild cache for
        :raises KeyError: If region group or coordinate arrays don't exist
        """
        if not self.group_exists(region):
            raise KeyError(f"Region '{region}' not found in store")

        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")
        self._region_shapes[region] = (lat_arr.shape[0], lon_arr.shape[0])
        self._region_bands[region] = set(self.get_bands(region))

        logger.debug(
            f"Rebuilt cache for '{region}': "
            f"shape={self._region_shapes[region]}, "
            f"bands={sorted(self._region_bands[region])}"
        )

    ############################################################################################
    # COORDINATE CREATION (PRIVATE)
    ############################################################################################

    def _create_lat_coord(self, region: str, lat: np.ndarray, preset: str = 'secondary'):
        """
        Create latitude coordinate array for a region.
        
        Creates a CF-compliant latitude coordinate array with proper metadata.
        The coordinate is created with the full latitude extent as a single chunk
        since it's typically accessed in its entirety for spatial operations.
        
        :param region: Region identifier (e.g., 'GOES-East', 'GOES-West')
        :type region: str
        :param lat: Array of latitude values in degrees north
        :type lat: np.ndarray
        :param preset: Array pipeline preset for compression configuration
        :type preset: str
        """
        path = f"{region}/lat"

        attrs = {
            'standard_name': 'latitude',
            'long_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y',
        }

        self.create_array(
            path=path,
            shape=(len(lat),),
            dtype=np.float64,
            attrs=attrs,
            preset=preset,
            dimension_names=['lat'],
            chunks=(len(lat),),
        )

        self.write_array(path, lat)

    def _create_lon_coord(self, region: str, lon: np.ndarray, preset: str = 'secondary'):
        """
        Create longitude coordinate array for a region.
        
        Creates a CF-compliant longitude coordinate array with proper metadata.
        The coordinate is created with the full longitude extent as a single chunk
        since it's typically accessed in its entirety for spatial operations.
        
        :param region: Region identifier (e.g., 'GOES-East', 'GOES-West')
        :type region: str
        :param lon: Array of longitude values in degrees east
        :type lon: np.ndarray
        :param preset: Array pipeline preset for compression configuration
        :type preset: str
        """
        path = f"{region}/lon"

        attrs = {
            'standard_name': 'longitude',
            'long_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X',
        }

        self.create_array(
            path=path,
            shape=(len(lon),),
            dtype=np.float64,
            attrs=attrs,
            preset=preset,
            dimension_names=['lon'],
            chunks=(len(lon),),
        )

        self.write_array(path, lon)

    def _create_time_coord(self, region: str, chunks: tuple = (512,), preset: str = 'secondary'):
        """
        Create extensible time coordinate array for a region.
        
        Creates an empty, extensible time dimension coordinate that can be appended
        to as new observations are added. Uses datetime64[ns] for CF-compliant
        time representation and is configured for efficient time-series operations.
        
        :param region: Region identifier (e.g., 'GOES-East', 'GOES-West')
        :type region: str
        :param chunks: Chunk size for time dimension (default: 512 for efficient append)
        :type chunks: tuple
        :param preset: Array pipeline preset for compression configuration
        :type preset: str
        """
        path = f"{region}/time"

        attrs = {
            'standard_name': 'time',
            'long_name': 'observation time',
            'axis': 'T',
        }

        self.create_array(
            path=path,
            shape=(0,),
            dtype='datetime64[ns]',
            attrs=attrs,
            preset=preset,
            dimension_names=['time'],
            chunks=chunks,
        )

    def _create_auxiliary_coords(self, region: str, preset: str = 'secondary'):
        """
        Create auxiliary coordinate arrays for a region.
        
        Creates empty, extensible auxiliary coordinate arrays that store metadata
        for each observation including platform identifier and scan mode. These
        coordinates are aligned with the time dimension and are populated as
        observations are appended to the store.
        
        Creates two auxiliary arrays:
        - platform_id: Satellite platform identifier (e.g., 'G16', 'G18')
        - scan_mode: ABI scan mode (e.g., '3', '4', '6')
        
        :param region: Region identifier (e.g., 'GOES-East', 'GOES-West')
        :type region: str
        :param preset: Array pipeline preset for compression configuration
        :type preset: str
        """
        platform_attrs = {
            'long_name': 'satellite platform identifier',
            'cf_role': 'auxiliary_coordinate',
        }
        self.create_array(
            path=f"{region}/platform_id",
            shape=(0,),
            dtype='U3',
            attrs=platform_attrs,
            preset=preset,
            dimension_names=['time'],
            chunks=(512,),
        )

        scan_attrs = {
            'long_name': 'ABI scan mode',
            'cf_role': 'auxiliary_coordinate',
        }
        self.create_array(
            path=f"{region}/scan_mode",
            shape=(0,),
            dtype='U10',
            attrs=scan_attrs,
            preset=preset,
            dimension_names=['time'],
            chunks=(512,),
        )

    ############################################################################################
    # ARRAY CREATION (PRIVATE)
    ############################################################################################

    def _create_cmi_array(self, region: str, band: int, preset: str = 'default'):
        """Create CMI_C##(time, lat, lon) float32, empty/extensible on time."""
        if band not in range(1, 17):
            raise ValueError(f"Invalid band {band}. Must be 1-16")

        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")

        path = f"{region}/CMI_C{band:02d}"

        return self.create_array(
            path=path,
            shape=(0, lat_arr.shape[0], lon_arr.shape[0]),
            dtype=np.float32,
            attrs=self._cf_cmi_attrs(band),
            preset=preset,
            dimension_names=["time", "lat", "lon"],
        )

    def _create_dqf_array(self, region: str, band: int, preset: str = 'secondary'):
        """Create DQF_C##(time, lat, lon) uint8, empty/extensible on time."""
        if band not in range(1, 17):
            raise ValueError(f"Invalid band {band}. Must be 1-16")

        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")

        path = f"{region}/DQF_C{band:02d}"

        return self.create_array(
            path=path,
            shape=(0, lat_arr.shape[0], lon_arr.shape[0]),
            dtype=np.uint8,
            attrs=self._cf_dqf_attrs(band),
            preset=preset,
            dimension_names=["time", "lat", "lon"],
        )

    ############################################################################################
    # DATA INSERTION
    ############################################################################################

    def append_observation(
            self,
            region: str,
            timestamp,
            platform_id: str,
            cmi_data: dict,
            dqf_data: Optional[dict] = None,
            scan_mode: Optional[str] = None
    ) -> int:
        """Append single observation to region"""
        # Fast-path validation using cached region metadata (no store lookups)
        expected_shape = self._region_shapes.get(region)
        if expected_shape is None:
            raise KeyError(f"Region '{region}' not initialized in store")

        expected_bands = self._region_bands.get(region, set())

        for band, data in cmi_data.items():
            if data.shape != expected_shape:
                raise ValueError(
                    f"CMI band {band} shape {data.shape} does not match "
                    f"expected {expected_shape}"
                )
            if band not in expected_bands:
                raise KeyError(f"Band {band} CMI array not found in region '{region}'")

        if dqf_data:
            for band, data in dqf_data.items():
                if data.shape != expected_shape:
                    raise ValueError(
                        f"DQF band {band} shape {data.shape} does not match "
                        f"expected {expected_shape}"
                    )

        # Convert timestamp to datetime64
        if not isinstance(timestamp, np.datetime64):
            timestamp = np.datetime64(timestamp)

        # Append to time coordinate
        time_idx = self.append_array(
            f"{region}/time",
            np.array([timestamp]),
            axis=0,
            return_location=True
        )[0]

        # Append to auxiliary coordinates
        self.append_array(f"{region}/platform_id", np.array([platform_id]))
        self.append_array(f"{region}/scan_mode", np.array([scan_mode or 'unknown']))

        # Append CMI data (auto-computes Dask if needed)
        for band, data in cmi_data.items():
            data_3d = data[np.newaxis, :, :]
            self.append_array(f"{region}/CMI_C{band:02d}", data_3d, axis=0)

        # Append DQF data if provided (auto-computes Dask if needed)
        if dqf_data:
            for band, data in dqf_data.items():
                data_3d = data[np.newaxis, :, :]
                self.append_array(f"{region}/DQF_C{band:02d}", data_3d, axis=0)

        logger.debug(f"Appended observation to {region} at time index {time_idx}")

        return time_idx

    def append_batch(self, region: str, observations: list) -> tuple:
        if not observations:
            return 0, 0

        # Fast-path validation using cached region metadata
        expected_shape = self._region_shapes.get(region)
        if expected_shape is None:
            raise KeyError(f"Region '{region}' not initialized in store")

        registered_bands = self._region_bands.get(region, multicloudconstants.ALL_BANDS)
        n_obs = len(observations)

        # Validate all observations
        expected_bands = set(observations[0]['cmi_data'].keys())

        missing = expected_bands - registered_bands
        if missing:
            raise KeyError(f"Bands {missing} not found in region '{region}'")

        for i, obs in enumerate(observations):
            for band, data in obs['cmi_data'].items():
                if data.shape != expected_shape:
                    raise ValueError(
                        f"Observation {i} CMI band {band} shape {data.shape}, "
                        f"expected {expected_shape}"
                    )
            if obs.get('dqf_data'):
                for band, data in obs['dqf_data'].items():
                    if data.shape != expected_shape:
                        raise ValueError(
                            f"Observation {i} DQF band {band} shape {data.shape}, "
                            f"expected {expected_shape}"
                        )
            obs_bands = set(obs['cmi_data'].keys())
            if obs_bands != expected_bands:
                raise ValueError(
                    f"Observation {i} has bands {obs_bands}, expected {expected_bands}"
                )

        # Cache array references once (these are actual writes, not just validation)
        time_arr = self.get_array(f"{region}/time")
        platform_arr = self.get_array(f"{region}/platform_id")
        scan_arr = self.get_array(f"{region}/scan_mode")

        # Single resize
        start_idx = time_arr.shape[0]
        end_idx = start_idx + n_obs

        time_arr.resize((end_idx,))
        platform_arr.resize((end_idx,))
        scan_arr.resize((end_idx,))

        # Write coordinates
        timestamps = np.array([np.datetime64(obs['timestamp']) for obs in observations])
        platform_ids = np.array([obs['platform_id'] for obs in observations])
        scan_modes = np.array([obs.get('scan_mode', '') for obs in observations])

        time_arr[start_idx:end_idx] = timestamps
        platform_arr[start_idx:end_idx] = platform_ids
        scan_arr[start_idx:end_idx] = scan_modes

        # Write bands
        has_dqf = all('dqf_data' in obs for obs in observations)
        for band in expected_bands:
            cmi_arr = self.get_array(f"{region}/CMI_C{band:02d}")
            cmi_arr.resize((end_idx, *cmi_arr.shape[1:]))
            cmi_stack = np.stack([obs['cmi_data'][band] for obs in observations], axis=0)
            cmi_arr[start_idx:end_idx] = self._ensure_numpy(cmi_stack)

            if has_dqf:
                dqf_arr = self.get_array(f"{region}/DQF_C{band:02d}")
                dqf_arr.resize((end_idx, *dqf_arr.shape[1:]))
                dqf_stack = np.stack([obs['dqf_data'][band] for obs in observations], axis=0)
                dqf_arr[start_idx:end_idx] = self._ensure_numpy(dqf_stack)

        logger.info(f"Appended {n_obs} observations to {region} (indices {start_idx}-{end_idx})")
        return start_idx, end_idx

    ############################################################################################
    # VALIDATION
    ############################################################################################

    def _validate_region(self, region: str):
        """Check region is valid and exists in store"""
        if region not in self.REGIONS:
            raise ValueError(f"Invalid region '{region}'. Must be one of {self.REGIONS}")

        if not self.group_exists(region):
            raise KeyError(f"Region '{region}' not initialized in store")

    def _validate_observation_shapes(
            self,
            region: str,
            cmi_data: dict,
            dqf_data: Optional[dict] = None
    ):
        """Check all arrays have shape (lat_size, lon_size)"""
        # Get expected shape
        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")
        expected_shape = (lat_arr.shape[0], lon_arr.shape[0])

        # Check CMI shapes
        for band, data in cmi_data.items():
            if data.shape != expected_shape:
                raise ValueError(
                    f"CMI band {band} shape {data.shape} does not match "
                    f"expected {expected_shape}"
                )

        # Check DQF shapes if provided
        if dqf_data:
            for band, data in dqf_data.items():
                if data.shape != expected_shape:
                    raise ValueError(
                        f"DQF band {band} shape {data.shape} does not match "
                        f"expected {expected_shape}"
                    )

    def _validate_bands_exist(self, region: str, bands: list):
        """Check all bands have CMI arrays in region"""
        for band in bands:
            path = f"{region}/CMI_C{band:02d}"
            if not self.array_exists(path):
                raise KeyError(f"Band {band} CMI array not found at '{path}'")

    ############################################################################################
    # QUERY ZARR STORE
    ############################################################################################

    def get_time_range(self, region: str) -> Optional[tuple]:
        """Get (start, end) as datetime64, or None if no observations"""
        self._validate_region(region)

        time_arr = self.get_array(f"{region}/time")

        if time_arr.shape[0] == 0:
            return None

        return time_arr[0], time_arr[-1]

    def get_observation_count(self, region: str) -> int:
        """Get number of time steps"""
        self._validate_region(region)

        time_arr = self.get_array(f"{region}/time")
        return time_arr.shape[0]

    def get_spatial_extent(self, region: str) -> dict:
        """Get lat/lon bounds"""
        self._validate_region(region)

        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")

        lat_vals = lat_arr[:]
        lon_vals = lon_arr[:]

        return {
            'lat_min': float(lat_vals.min()),
            'lat_max': float(lat_vals.max()),
            'lon_min': float(lon_vals.min()),
            'lon_max': float(lon_vals.max()),
        }

    def get_bands(self, region: str) -> list:
        """Get sorted list of band numbers present"""
        self._validate_region(region)

        arrays = self.array_list(region)

        bands = []
        for name in arrays:
            if name.startswith('CMI_C'):
                try:
                    band_num = int(name[5:7])
                    bands.append(band_num)
                except ValueError:
                    continue

        return sorted(bands)

    def get_platforms(self, region: str) -> list:
        """Get unique platform_id values"""
        self._validate_region(region)

        platform_arr = self.get_array(f"{region}/platform_id")

        if platform_arr.shape[0] == 0:
            return []

        platforms = platform_arr[:]
        return sorted(list(set(platforms)))

    ############################################################################################
    # PROVENANCE & METADATA UPDATES
    ############################################################################################

    def update_temporal_coverage(self, region: str):
        """
        Update global time_coverage_* attributes based on current data
        Should be called after appending observations
        """
        time_range = self.get_time_range(region)

        if time_range is None:
            return

        start, end = time_range

        # Update global attrs
        current_attrs = self.get_attrs('/')
        current_attrs['time_coverage_start'] = str(start)
        current_attrs['time_coverage_end'] = str(end)

        # Calculate duration
        duration = end - start
        current_attrs['time_coverage_duration'] = str(duration)

        # Update modified timestamp
        current_attrs['date_modified'] = datetime.now(timezone.utc).isoformat() + 'Z'

        self.set_attrs('/', current_attrs, merge=True)

        logger.debug(f"Updated temporal coverage for {region}: {start} to {end}")

    def add_processing_history(self, message: str):
        """Append to processing history attribute"""
        current_attrs = self.get_attrs('/')
        current_history = current_attrs.get('history', '')

        timestamp = datetime.now(timezone.utc).isoformat() + 'Z'
        new_entry = f"{timestamp}: {message}"

        if current_history:
            current_attrs['history'] = f"{current_history}\n{new_entry}"
        else:
            current_attrs['history'] = new_entry

        self.set_attrs('/', current_attrs, merge=True)

    def add_source_files(self, region: str, file_paths: list[str]):
        """Track source files used to create this dataset."""
        region_attrs = self.get_attrs(region)

        existing_raw = region_attrs.get('source_files', '')
        existing_sources = existing_raw.split('\n') if existing_raw else []

        all_sources = sorted(set(existing_sources + file_paths))

        self.set_attrs(region, {
            'source_files': '\n'.join(all_sources),
            'source_file_count': len(all_sources),
        }, merge=True)

    def finalize_dataset(self):
        """
        Final metadata updates before closing
        - Update all temporal coverage
        - Add final history entry
        - Optionally validate CF compliance
        """
        for region in self.REGIONS:
            if self.group_exists(region):
                self.update_temporal_coverage(region)
            else:
                logger.warning(f"Configured region '{region}' not found in store, skipping")

        # Add final history entry
        self.add_processing_history("Dataset finalized and ready for distribution")

        logger.info("Dataset finalized")

    ############################################################################################
    # CF METADATA (PRIVATE)
    ############################################################################################

    def _cf_global_attrs(self) -> dict:
        """Return CF global attributes from config with ACDD compliance"""
        goes_config = self.config.get('goes', {})
        global_metadata = goes_config.get('global_metadata', {})
        processing_config = goes_config.get('processing', {})

        # Default values
        defaults = {
            'Conventions': 'CF-1.13, ACDD-1.3',
            'title': 'GOES ABI L2+ Cloud and Moisture Imagery',
            'summary': 'Regridded GOES ABI imagery on regular lat/lon grid',
            'institution': 'University of Toronto',
            'source': 'GOES-R Series Advanced Baseline Imager',
            'processing_level': 'L2+',
            'creator_name': 'Marble Platform',
            'creator_type': 'institution',
            'references': 'https://www.goes-r.gov/products/baseline-cloud-moisture-imagery.html',
            'comment': 'Regridded from native geostationary projection to geographic lat/lon using barycentric interpolation',
            'license': 'CC BY 4.0',
            'standard_name_vocabulary': 'CF Standard Name Table v92',
            'keywords': 'ATMOSPHERE > ATMOSPHERIC RADIATION > REFLECTANCE, SPECTRAL/ENGINEERING > INFRARED WAVELENGTHS > BRIGHTNESS TEMPERATURE',
        }

        # Merge config with defaults (config takes precedence)
        attrs = {**defaults, **global_metadata}

        # Add processing metadata if available
        if processing_config:
            if 'software_name' in processing_config:
                attrs['processing_software'] = processing_config['software_name']
            if 'software_version' in processing_config:
                attrs['processing_software_version'] = processing_config['software_version']
            if 'software_url' in processing_config:
                attrs['processing_software_url'] = processing_config['software_url']
            if 'processing_environment' in processing_config:
                attrs['processing_environment'] = processing_config['processing_environment']

        # Add timestamps (always current)
        now = datetime.now(timezone.utc).isoformat() + 'Z'
        attrs['date_created'] = now
        attrs['date_modified'] = now
        attrs['history'] = f"Created {now}"

        return attrs


    def _cf_region_attrs(
            self,
            lat: np.ndarray,
            lon: np.ndarray,
            regridding_provenance: Optional[dict] = None
    ) -> dict:
        """Return region attributes with full regridding provenance"""
        attrs = {
            'geospatial_lat_min': float(lat.min()),
            'geospatial_lat_max': float(lat.max()),
            'geospatial_lon_min': float(lon.min()),
            'geospatial_lon_max': float(lon.max()),
            'geospatial_lat_units': 'degrees_north',
            'geospatial_lon_units': 'degrees_east',
            'geospatial_lat_resolution': float(np.abs(np.diff(lat).mean())),
            'geospatial_lon_resolution': float(np.abs(np.diff(lon).mean())),
        }

        # Add regridding provenance if available
        if regridding_provenance:
            # Store as individual attrs for CF compliance
            attrs['regridding_method'] = regridding_provenance.get('method', 'barycentric')
            attrs['regridding_triangulation'] = regridding_provenance.get('triangulation', 'delaunay')
            attrs['regridding_direct_hit_threshold'] = regridding_provenance.get('direct_hit_threshold', 0.999)
            attrs['source_projection'] = regridding_provenance.get('source_projection', 'geostationary')


        return attrs

    def _cf_cmi_attrs(self, band: int) -> dict:
        """Return CF attributes for CMI array from config"""
        band_meta = self.BAND_METADATA.get(band, {})

        attrs = {
            'long_name': band_meta.get('long_name', f'Band {band}'),
            'standard_name': band_meta.get('standard_name', ''),
            'units': band_meta.get('units', '1'),
            'radiation_wavelength': band_meta.get('wavelength', 0.0),
            'radiation_wavelength_units': 'um',
            'cell_methods': self.CELL_METHODS,
            'coordinates': 'time lat lon',
            'ancillary_variables': f'DQF_C{band:02d}',
        }

        # Add description if present
        if 'description' in band_meta:
            attrs['description'] = band_meta['description']

        # Add products/applications if present
        if 'products' in band_meta:
            # Store as comma-separated string for CF compliance
            attrs['products'] = ', '.join(band_meta['products'])

        # Add valid range from config
        if 'valid_range' in band_meta:
            attrs['valid_range'] = band_meta['valid_range']

        return attrs

    def _cf_dqf_attrs(self, band: int) -> dict:
        """Return CF attributes for DQF array with extended flags (0-6)."""

        attrs = {
            'long_name': f'ABI L2+ CMI data quality flags for band {band}',
            'standard_name': 'status_flag',
            'units': '1',
            'flag_values': list(multicloudconstants.DQF_FLAGS.keys()),
            'flag_meanings': " ".join(v["meaning"] for v in multicloudconstants.DQF_FLAGS.values()),
            'valid_range': [min(multicloudconstants.DQF_FLAGS), max(multicloudconstants.DQF_FLAGS)],
            'coordinates': 'time lat lon',
            'comment': (
                'Flags 0-4 from original GOES-R ABI L2 CMI product. '
                f'Flag {multicloudconstants.DQF_FLAGS[5]["name"]} ({multicloudconstants.DQF_FLAGS[5]["meaning"]}) '
                'indicates value was computed via barycentric interpolation. '
                f'Flag {multicloudconstants.DQF_FLAGS[6]["name"]} ({multicloudconstants.DQF_FLAGS[6]["meaning"]}) '
                'indicates some source pixels in the interpolation hull were NaN.'
            ),
        }

        return attrs

    ############################################################################################
    # BAND METADATA HELPERS
    ############################################################################################

    def get_bands_for_product(self, product_name: str) -> list:
        """
        Input: product name (e.g., 'Fire/hotspot characterization')
        Output: list of band numbers that support this product
        Job: Search band metadata for product usage
        """
        matching_bands = []

        for band in range(1, 17):
            band_meta = self.BAND_METADATA.get(band, {})
            products = band_meta.get('products', [])

            if product_name in products:
                matching_bands.append(band)

        return sorted(matching_bands)

    def get_products_for_band(self, band: int) -> list:
        """
        Input: band number
        Output: list of products/applications that use this band
        Job: Return products list from metadata
        """
        band_meta = self.BAND_METADATA.get(band, {})
        return band_meta.get('products', [])

    def list_all_products(self) -> list:
        """
        Output: sorted list of all unique products across all bands
        Job: Collect and deduplicate all product names
        """
        all_products = set()

        for band in range(1, 17):
            band_meta = self.BAND_METADATA.get(band, {})
            products = band_meta.get('products', [])
            all_products.update(products)

        return sorted(list(all_products))

    def _get_band_wavelength(self, band: int) -> float:
        """Get wavelength in µm from config"""
        return self.BAND_METADATA.get(band, {}).get('wavelength', 0.0)

    def _get_band_long_name(self, band: int) -> str:
        """Get descriptive name from config"""
        return self.BAND_METADATA.get(band, {}).get('long_name', f'Band {band}')

    def _is_reflectance_band(self, band: int) -> bool:
        """Check if band is reflectance (1-6) or brightness temp (7-16)"""
        return band in multicloudconstants.REFLECTANCE_BANDS