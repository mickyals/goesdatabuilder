from ..zarrstore import ZarrStoreBuilder
from pathlib import Path
import numpy as np
from datetime import datetime, timezone
from typing import Union, Optional, TYPE_CHECKING
import logging
import json


from ...utils.grid_utils import validate_longitude_monotonic

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

    REFLECTANCE_BANDS = list(range(1, 7))
    BRIGHTNESS_TEMP_BANDS = list(range(7, 17))
    CELL_METHODS = 'time: point latitude,longitude: mean' # indicates what each pixel means in metadata

    # Default band metadata (used as fallback if not in config)
    DEFAULT_BAND_METADATA = {
        1: {'wavelength': 0.47, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Blue',
            'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
        2: {'wavelength': 0.64, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Red',
            'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
        3: {'wavelength': 0.86, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Veggie',
            'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
        4: {'wavelength': 1.37, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Cirrus',
            'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
        5: {'wavelength': 1.61, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Snow/Ice',
            'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
        6: {'wavelength': 2.24, 'long_name': 'ABI Cloud and Moisture Imagery reflectance factor - Cloud Particle Size',
            'standard_name': 'toa_bidirectional_reflectance', 'units': '1', 'valid_range': [0.0, 1.0]},
        7: {'wavelength': 3.90,
            'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Shortwave Window',
            'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [197.30, 411.86]},
        8: {'wavelength': 6.19,
            'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Upper-Level Water Vapor',
            'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [138.05, 311.06]},
        9: {'wavelength': 6.93,
            'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Mid-Level Water Vapor',
            'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [137.7 , 311.08]},
        10: {'wavelength': 7.34,
             'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Lower-Level Water Vapor',
             'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [126.91, 331.2]},
        11: {'wavelength': 8.44,
             'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Cloud-Top Phase',
             'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [127.69, 341.3]},
        12: {'wavelength': 9.61,
             'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Ozone',
             'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [117.49, 311.06]},
        13: {'wavelength': 10.33,
             'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Clean Longwave Window',
             'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [ 89.62, 341.27]},
        14: {'wavelength': 11.21,
             'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Longwave Window',
             'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [ 96.19, 341.28]},
        15: {'wavelength': 12.29,
             'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Dirty Longwave Window',
             'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [ 97.38, 341.28]},
        16: {'wavelength': 13.28,
             'long_name': 'ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - CO2 Longwave',
             'standard_name': 'toa_brightness_temperature', 'units': 'K', 'valid_range': [ 92.7 , 318.26]},
    }

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

    def _load_goes_config(self):
        """Load and validate GOES-specific configuration"""
        goes_config = self.config.get('goes', {})

        # Load regions (platforms)
        self.REGIONS = goes_config.get('platforms', ['GOES-East', 'GOES-West', 'GOES-Test', 'GOES-Storage'])

        # Load bands to process
        self.BANDS = goes_config.get('bands', list(range(1, 17)))

        # Load band metadata (with fallback to defaults)
        config_band_metadata = goes_config.get('band_metadata', {})
        self.BAND_METADATA = {}

        for band in range(1, 17):
            if band in config_band_metadata:
                # Use config metadata
                self.BAND_METADATA[band] = config_band_metadata[band]
            else:
                # Fallback to default
                self.BAND_METADATA[band] = self.DEFAULT_BAND_METADATA.get(band, {})

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
        self._create_lat_coord(region, lat)
        self._create_lon_coord(region, lon)
        self._create_time_coord(region)

        # Create auxiliary coordinates
        self._create_auxiliary_coords(region)

        # Create CMI and DQF arrays for each band
        for band in bands:
            self._create_cmi_array(region, band)
            if include_dqf:
                self._create_dqf_array(region, band)

        logger.info(f"Initialized region '{region}' with {len(bands)} bands")

    ############################################################################################
    # COORDINATE CREATION (PRIVATE)
    ############################################################################################

    def _create_lat_coord(self, region: str, lat: np.ndarray):
        """Create lat(lat) dimension coordinate"""
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
            chunks=(len(lat),),
            attrs=attrs,
            preset='default'
        )

        self.write_array(path, lat)

    def _create_lon_coord(self, region: str, lon: np.ndarray):
        """Create lon(lon) dimension coordinate"""
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
            chunks=(len(lon),),
            attrs=attrs,
            preset='default'
        )

        self.write_array(path, lon)

    def _create_time_coord(self, region: str):
        """Create time(time) dimension coordinate, empty/extensible"""
        path = f"{region}/time"

        attrs = {
            'standard_name': 'time',
            'long_name': 'J2000 epoch mid-point between the start and end image scan in seconds',
            'axis': 'T',
            'calendar': 'standard',
        }

        self.create_array(
            path=path,
            shape=(0,),
            dtype='datetime64[ns]',
            chunks=(1,),
            attrs=attrs,
            preset='default'
        )

    def _create_auxiliary_coords(self, region: str):
        """Create empty extensible auxiliary coordinates"""
        # Platform ID
        platform_attrs = {
            'long_name': 'satellite platform identifier',
            'cf_role': 'auxiliary_coordinate',
        }
        self.create_array(
            path=f"{region}/platform_id",
            shape=(0,),
            dtype='U3',
            chunks=(1,),
            attrs=platform_attrs,
            preset='default'
        ) # U3 because G18 G19

        # Scan Mode
        scan_attrs = {
            'long_name': 'ABI scan mode',
            'cf_role': 'auxiliary_coordinate',
        }
        self.create_array(
            path=f"{region}/scan_mode",
            shape=(0,),
            dtype='U10',
            chunks=(1,),
            attrs=scan_attrs,
            preset='default'
        ) # Scan mode is ABI Mode 6 - 3, 4, 6 are valid modes

    ############################################################################################
    # ARRAY CREATION (PRIVATE)
    ############################################################################################

    def _create_cmi_array(self, region: str, band: int):
        """Create CMI_C##(time, lat, lon) float32, empty/extensible on time"""
        if band not in range(1, 17):
            raise ValueError(f"Invalid band {band}. Must be 1-16")

        # Get region dimensions
        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")
        n_lat = lat_arr.shape[0]
        n_lon = lon_arr.shape[0]

        # Get band metadata from config
        attrs = self._cf_cmi_attrs(band)

        # Get chunks from config
        zarr_config = self.config.get('zarr', {})
        chunk_config = zarr_config.get('compression', {}).get('default', {}).get('chunks', {})

        # Default chunks
        time_chunk = chunk_config.get('time', 1) if isinstance(chunk_config, dict) else 1
        lat_chunk = chunk_config.get('lat', min(512, n_lat)) if isinstance(chunk_config, dict) else min(512, n_lat)
        lon_chunk = chunk_config.get('lon', min(512, n_lon)) if isinstance(chunk_config, dict) else min(512, n_lon)

        path = f"{region}/CMI_C{band:02d}"

        return self.create_array(
            path=path,
            shape=(0, n_lat, n_lon),
            dtype=np.float32,
            chunks=(time_chunk, lat_chunk, lon_chunk),
            attrs=attrs,
            preset='default'
        )

    def _create_dqf_array(self, region: str, band: int):
        """Create DQF_C##(time, lat, lon) uint8, empty/extensible on time"""
        if band not in range(1, 17):
            raise ValueError(f"Invalid band {band}. Must be 1-16")

        # Get region dimensions
        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")
        n_lat = lat_arr.shape[0]
        n_lon = lon_arr.shape[0]

        # Get band metadata
        attrs = self._cf_dqf_attrs(band)

        # Get chunks from config
        zarr_config = self.config.get('zarr', {})
        chunk_config = zarr_config.get('compression', {}).get('default', {}).get('chunks', {})

        # Default chunks
        time_chunk = chunk_config.get('time', 1) if isinstance(chunk_config, dict) else 1
        lat_chunk = chunk_config.get('lat', min(512, n_lat)) if isinstance(chunk_config, dict) else min(512, n_lat)
        lon_chunk = chunk_config.get('lon', min(512, n_lon)) if isinstance(chunk_config, dict) else min(512, n_lon)

        path = f"{region}/DQF_C{band:02d}"

        return self.create_array(
            path=path,
            shape=(0, n_lat, n_lon),
            dtype=np.uint8,
            chunks=(time_chunk, lat_chunk, lon_chunk),
            attrs=attrs,
            preset='secondary'
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
        # Validate region
        self._validate_region(region)

        # Validate shapes
        self._validate_observation_shapes(region, cmi_data, dqf_data)

        # Validate bands exist
        bands = list(cmi_data.keys())
        self._validate_bands_exist(region, bands)

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

    def append_batch(
            self,
            region: str,
            observations: list
    ) -> tuple:
        """Append batch of observations to region with single resize operation"""
        if not observations:
            return 0, 0

        # Validate region
        self._validate_region(region)

        n_obs = len(observations)

        # Validate all observations first
        for obs in observations:
            self._validate_observation_shapes(region, obs['cmi_data'], obs.get('dqf_data'))
            bands = list(obs['cmi_data'].keys())
            self._validate_bands_exist(region, bands)

        # Get current time dimension size
        time_arr = self.get_array(f"{region}/time")
        start_idx = time_arr.shape[0]
        end_idx = start_idx + n_obs

        # Collect all data
        timestamps = np.array([np.datetime64(obs['timestamp']) for obs in observations])
        platform_ids = np.array([obs['platform_id'] for obs in observations])
        scan_modes = np.array([obs.get('scan_mode', '') for obs in observations])

        # Resize all arrays once
        time_arr.resize((end_idx,))
        self.get_array(f"{region}/platform_id").resize((end_idx,))
        self.get_array(f"{region}/scan_mode").resize((end_idx,))

        # Write time and auxiliary coords
        time_arr[start_idx:end_idx] = timestamps
        self.get_array(f"{region}/platform_id")[start_idx:end_idx] = platform_ids
        self.get_array(f"{region}/scan_mode")[start_idx:end_idx] = scan_modes

        # Get all bands from first observation (assume consistent)
        all_bands = list(observations[0]['cmi_data'].keys())

        # Stack and write each band
        # Stack and write each band
        for band in all_bands:
            # Stack CMI data (if Dask, will be computed by append_array)
            cmi_stack = np.stack([obs['cmi_data'][band] for obs in observations], axis=0)

            # This might trigger compute if any obs has Dask arrays
            cmi_arr = self.get_array(f"{region}/CMI_C{band:02d}")
            cmi_arr.resize((end_idx, *cmi_arr.shape[1:]))

            # _ensure_numpy called internally via write_array
            cmi_arr[start_idx:end_idx] = self._ensure_numpy(cmi_stack)

            # DQF if available
            dqf_path = f"{region}/DQF_C{band:02d}"
            if self.array_exists(dqf_path) and all('dqf_data' in obs for obs in observations):
                dqf_stack = np.stack([obs['dqf_data'][band] for obs in observations], axis=0)
                dqf_arr = self.get_array(dqf_path)
                dqf_arr.resize((end_idx, *dqf_arr.shape[1:]))
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
    # QUERY
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
        """Track source files used to create this dataset"""
        region_attrs = self.get_attrs(region)

        existing_sources = region_attrs.get('source_files', [])
        if isinstance(existing_sources, str):
            try:
                existing_sources = json.loads(existing_sources)
            except json.JSONDecodeError:
                existing_sources = []

        # Merge and deduplicate
        all_sources = list(set(existing_sources + file_paths))

        # Store as JSON array
        region_attrs['source_files'] = json.dumps(all_sources)
        region_attrs['source_file_count'] = len(all_sources)

        self.set_attrs(region, region_attrs, merge=True)

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
            'keywords': 'GOES, ABI, satellite, imagery, regridded, lat-lon',
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
            'grid_mapping': 'crs',
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
        """Return CF attributes for DQF array with extended flags (0-6)"""

        attrs = {
            'long_name': f'ABI L2+ CMI data quality flags for band {band}',
            'standard_name': 'status_flag',
            'units': '1',
            'flag_values': [0, 1, 2, 3, 4, 5],
            'flag_meanings': 'good_pixels_qf conditionally_usable_pixels_qf out_of_range_pixels_qf no_value_pixels_qf focal_plane_temperature_threshold_exceeded_qf interpolated_qf',
            'valid_range': [0, 5],
            'coordinates': 'time lat lon',
            'grid_mapping': 'crs',
        }

        # Add explanatory comment
        interpolated_flag = 5

        attrs['comment'] = (
            f'Flags 0-4 from original GOES-R ABI L2 CMI product. '
            f'Flag {interpolated_flag} (interpolated_qf) indicates value was computed via barycentric interpolation'
        )

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
        return band in self.REFLECTANCE_BANDS