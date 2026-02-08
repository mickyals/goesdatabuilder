import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Union
import yaml
import json
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration validation error"""
    pass


class GOESMultiCloudObservation:
    """
    CF-aligned interface to GOES ABI L2 CMI data.
    Handles single file or multiple files (time-concatenated).

    All global attributes are promoted to time-indexed variables for:
    - Proper concatenation across files
    - Per-observation provenance tracking
    - Export to CSV for metadata cataloging

    NOTE: This class provides raw geostationary-projected data.
    CMI/DQF arrays require regridding before storage in GOESZarrStore.
    CF Data Model Mapping (Source Data):
    ------------------------------------
    Domain Axes:
        time (T)  — 1 per file, concatenated for multi-file
        y    (N)  — 5424 pixels (Full Disk 2km)
        x    (M)  — 5424 pixels

    Dimension Coordinates:
        time      — datetime64, from 't' coordinate
        y(y)      — float32, radians (scanning angle)
        x(x)      — float32, radians (scanning angle)

    Promoted Global Attributes → Time-indexed Variables:
        Identity:
            observation_id(time)        — UUID per product
            dataset_name(time)          — filename
            naming_authority(time)      — gov.nesdis.noaa

        Satellite/Instrument:
            platform_id(time)           — G16, G17, G18, G19
            orbital_slot(time)          — GOES-East, GOES-West, etc.
            instrument_type(time)       — GOES-R Series ABI
            instrument_id(time)         — FM1, FM2, FM3, FM4

        Scene/Mode:
            scene_id(time)              — Full Disk, CONUS, Mesoscale
            scan_mode(time)             — ABI Mode 3/4/6 (from timeline_id)
            spatial_resolution(time)    — 2km at nadir

        Temporal:
            time_coverage_start(time)   — observation start
            time_coverage_end(time)     — observation end
            date_created(time)          — product creation time

        Production:
            production_site(time)       — NSOF, ESPC, WCDAS
            production_environment(time)— OE, DE
            production_data_source(time)— Realtime, Simulated, etc.
            processing_level(time)      — NASA L2

        Standards:
            conventions(time)           — CF-1.7
            metadata_conventions(time)  — Unidata Dataset Discovery v1.0
            standard_name_vocabulary(time) — CF Standard Name Table version

        Documentation:
            title(time)                 — product title
            summary(time)               — product description
            institution(time)           — DOC/NOAA/NESDIS
            project(time)               — GOES
            license(time)               — access restrictions
            keywords(time)              — GCMD keywords
            keywords_vocabulary(time)   — GCMD version
            cdm_data_type(time)         — Image
            iso_series_metadata_id(time)— ISO metadata UUID

    Auxiliary Coordinates (per-band, scalar):
        band_wavelength_C##  — center wavelength µm
        band_id_C##          — band number

    Coordinate Reference:
        goes_imager_projection — geostationary projection params

    Field Constructs:
        CMI_C01..16(time, y, x)  — reflectance (1-6) or brightness temp (7-16)

    Field Ancillary:
        DQF_C01..16(time, y, x)  — quality flags per CMI

    Pipeline:
        GOESMultiCloudObservation (this) → Regridder → GOESZarrStore
    """

    ############################################################################################
    # CLASS CONSTANTS
    ############################################################################################

    PROMOTED_ATTRS = {
        # Identity
        'id': 'observation_id',
        'dataset_name': 'dataset_name',
        'naming_authority': 'naming_authority',

        # Satellite/Instrument
        'platform_ID': 'platform_id',
        'orbital_slot': 'orbital_slot',
        'instrument_type': 'instrument_type',
        'instrument_ID': 'instrument_id',

        # Scene/Mode
        'scene_id': 'scene_id',
        'timeline_id': 'scan_mode',
        'spatial_resolution': 'spatial_resolution',

        # Temporal
        'time_coverage_start': 'time_coverage_start',
        'time_coverage_end': 'time_coverage_end',
        'date_created': 'date_created',

        # Production
        'production_site': 'production_site',
        'production_environment': 'production_environment',
        'production_data_source': 'production_data_source',
        'processing_level': 'processing_level',

        # Standards
        'Conventions': 'conventions',
        'Metadata_Conventions': 'metadata_conventions',
        'standard_name_vocabulary': 'standard_name_vocabulary',

        # Documentation
        'title': 'title',
        'summary': 'summary',
        'institution': 'institution',
        'project': 'project',
        'license': 'license',
        'keywords': 'keywords',
        'keywords_vocabulary': 'keywords_vocabulary',
        'cdm_data_type': 'cdm_data_type',
        'iso_series_metadata_id': 'iso_series_metadata_id',
    }

    VALID_ORBITAL_SLOTS = {'GOES-East', 'GOES-West', 'GOES-Test', 'GOES-Storage'}
    VALID_PLATFORMS = {'G16', 'G17', 'G18', 'G19'}
    VALID_SCENE_IDS = {'Full Disk', 'CONUS', 'Mesoscale'}

    # GOES filename pattern:  OR_ABI-L2-MCMIPF-M6_G18_s20240030200212_e20240030209521_c20240030210015.nc
    GOES_FILENAME_PATTERN = re.compile(
        r'OR_ABI-L2-MCMIP[FCM]-M\d+_G\d\d+_s(\d{14})_e(\d{14})_c(\d{14})\.nc'
    )

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(self, config: Union[dict, str, Path], strict: bool = None):
        """
        Initialize GOESMultiCloudObservation from configuration

        Parameters:
        config (dict, str, Path): Configuration dictionary or path to YAML/JSON
        strict (bool, optional): If True, fail on inconsistent files; if False, skip and log.
            If None, use value from config (default True)

        Notes:
        The configuration should contain the following keys:
        - 'files': List of files to open
        - 'file_dir': Directory containing files to open
        - 'strict': If True, fail on inconsistent files; if False, skip and log
        - 'chunks': Dictionary of chunk sizes for each variable
        - 'engine': Engine to use for opening the dataset (default 'netcdf4')

        Raises:
        ConfigError: If the configuration is invalid
        """

        # Load and validate configuration
        self.config = self._validate_and_load_config(config)

        # Use strict from parameter or config
        if strict is not None:
            self.strict = strict
        else:
            self.strict = self.config.get('strict', True)

        # Initialize variables
        self._current_band = None
        self._skipped_files = []

        # Open the dataset
        self.ds = self._open_dataset()

    def _validate_and_load_config(self, config, sample_size: int = None) -> dict:
        """
        Validate and load a configuration dictionary from a file path or dict.

        Parameters:
        config (dict, str, Path): Configuration dictionary or path to YAML/JSON
        sample_size (int, optional): Number of files to sample for validation

        Returns:
        dict: Validated config dictionary

        Raises:
        ConfigError: If the configuration is invalid
        """
        # Parse config if path
        if isinstance(config, (str, Path)):
            config_path = Path(config)
            if not config_path.exists():
                raise ConfigError(f"Config file not found: {config_path}")

            with open(config_path, 'r') as f:
                if config_path.suffix in ['.yaml', '.yml']:
                    config = yaml.safe_load(f)
                elif config_path.suffix == '.json':
                    config = json.load(f)
                else:
                    raise ConfigError(f"Unsupported config format: {config_path.suffix}")

        # Validate structure
        if not isinstance(config, dict):
            raise ConfigError("Config must be a dictionary")

        # Extract data_access section if present
        if 'data_access' in config:
            data_config = config['data_access']
        else:
            data_config = config

        # Determine sample size
        if sample_size is None:
            sample_size = data_config.get('sample_size', 5)

        # Get files either from 'files' list or 'file_dir' directory
        if 'files' in data_config:
            files = data_config['files']
            if not files:
                raise ConfigError("Config 'files' list is empty")
            files = [Path(f) for f in files]

        elif 'file_dir' in data_config:
            file_dir = Path(data_config['file_dir'])

            # Expand environment variables
            file_dir_str = str(file_dir)
            if '$' in file_dir_str:
                import os
                file_dir = Path(os.path.expandvars(file_dir_str))

            if not file_dir.exists():
                raise ConfigError(f"Directory not found: {file_dir}")

            if not file_dir.is_dir():
                raise ConfigError(f"Not a directory: {file_dir}")

            # Glob for GOES multiband files
            # Pattern: OR_ABI-L2-CMIPF-*.nc or OR_ABI-L2-CMIPM-*.nc or OR_ABI-L2-CMIPC-*.nc
            files = []
            for pattern in ['OR_ABI-L2-MCMIPF-*.nc', 'OR_ABI-L2-MCMIPM-*.nc', 'OR_ABI-L2-MCMIPC-*.nc']:
                files.extend(file_dir.glob(pattern))

            if not files:
                raise ConfigError(f"No GOES MCMIP files found in {file_dir}")

            logger.info(f"Found {len(files)} GOES files in {file_dir}")

        else:
            raise ConfigError("Config must contain either 'files' or 'file_dir' key")

        # Extract timestamps and validate filename pattern
        file_timestamps = []
        for f in files:
            match = self.GOES_FILENAME_PATTERN.match(f.name)
            if not match:
                logger.warning(f"Skipping file with invalid GOES naming pattern: {f.name}")
                continue

            # Extract start timestamp (s20230010000000 -> 2023001000000)
            timestamp_str = match.group(1)
            timestamp = datetime.strptime(timestamp_str, '%Y%j%H%M%S')
            file_timestamps.append((f, timestamp))

        if not file_timestamps:
            raise ConfigError("No valid GOES files found after pattern matching")

        # Sort by timestamp
        file_timestamps.sort(key=lambda x: x[1])
        sorted_files = [f for f, _ in file_timestamps]

        logger.info(f"Validated {len(sorted_files)} files spanning {file_timestamps[0][1]} to {file_timestamps[-1][1]}")

        # Sample files for validation
        n_sample = min(sample_size, len(sorted_files))
        if n_sample < len(sorted_files):
            import random
            # Sample evenly across time range for better coverage
            sample_indices = [int(i * len(sorted_files) / n_sample) for i in range(n_sample)]
            sample_files = [sorted_files[i] for i in sample_indices]
        else:
            sample_files = sorted_files

        logger.info(f"Validating {n_sample} sample files...")

        # Validate sampled files
        for f in sample_files:
            if not f.exists():
                raise ConfigError(f"File not found: {f}")

            # Quick validation: try opening
            try:
                with xr.open_dataset(f, engine='netcdf4') as ds:
                    # Check for required coordinates/variables
                    if 't' not in ds.coords.keys():
                        raise ConfigError(f"Missing 't' coordinate in {f.name}")
                    if 'orbital_slot' not in ds.attrs.keys():
                        raise ConfigError(f"Missing 'orbital_slot' attribute in {f.name}")
            except Exception as e:
                raise ConfigError(f"Failed to open {f.name}: {e}")

        # Build flat config for internal use
        flat_config = {
            'files': sorted_files,
            'engine': data_config.get('engine', 'netcdf4'),
            'strict': data_config.get('strict', True),
        }

        # Handle chunks - convert from YAML format to xarray format
        if 'chunk_size' in data_config:
            chunk_config = data_config['chunk_size']
            # xarray expects chunks as dict, which is what we have
            flat_config['chunks'] = chunk_config
        else:
            flat_config['chunks'] = 'auto'

        # Store original config sections for reference
        flat_config['_original_config'] = config

        return flat_config

    def _preprocess(self, ds: xr.Dataset) -> Union[xr.Dataset, None]:
        """
        Preprocess a single-file xr.Dataset before combining it with others.

        Parameters:
        ds (xr.Dataset): Single-file xrDataset to preprocess

        Returns:
        Union[xr.Dataset, None]: Preprocessed Dataset, or None if skipped (strict=False)

        Notes:
        The preprocessor performs the following steps:
        1. Validate orbital_slot is a single value and in VALID_ORBITAL_SLOTS
            - strict=True: raise with filename
            - strict=False: log error, return None
        2. Expand time dimension
        3. Assign time coordinate from 't' variable
        4. Promote all PROMOTED_ATTRS to time-indexed variables
        5. Return modified dataset
        """
        # Get filename for error messages
        filename = ds.attrs.get('dataset_name', 'unknown')

        # Validate orbital_slot
        orbital_slot = ds.attrs.get('orbital_slot')
        if orbital_slot not in self.VALID_ORBITAL_SLOTS:
            msg = f"Invalid orbital_slot '{orbital_slot}' in {filename}"
            if self.strict:
                raise ValueError(msg)
            else:
                logger.error(msg)
                self._skipped_files.append(filename)
                return None

        # Expand time dimension
        ds = ds.expand_dims('time')

        # Assign time coordinate from 't' variable
        if 't' in ds.coords:
            ds = ds.assign_coords(time=('time', [ds.coords['t'].values]))
        else:
            raise ValueError(f"Missing 't' coordinate in {filename}")

        # Promote global attributes to time-indexed variables
        for source_attr, target_var in self.PROMOTED_ATTRS.items():
            if source_attr in ds.attrs:
                value = ds.attrs[source_attr]
                # Create a DataArray with time dimension
                ds[target_var] = xr.DataArray(
                    [value],
                    dims=['time'],
                    coords={'time': ds.coords['time']}
                )

        return ds

    def _open_dataset(self) -> xr.Dataset:
        """
        Open a dataset from the configuration files.

        The dataset is opened using `xr.open_mfdataset` with the following parameters:
            - files from the configuration
            - concat_dim='time'
            - combine='nested'
            - preprocess=self._preprocess
            - chunks from the configuration (default 'auto')
            - engine from the configuration (default 'netcdf4')

        If the configuration contains only one file, it is opened using `xr.open_dataset`
        instead.

        Parameters:
        None

        Returns:
        xr.Dataset: The opened dataset

        """
        files = self.config['files']

        if len(files) == 1:
            # Single file: use open_dataset
            ds = xr.open_dataset(
                files[0],
                chunks=self.config['chunks'],
                engine=self.config['engine']
            )
            ds = self._preprocess(ds)
        else:
            # Multiple files: use open_mfdataset
            ds = xr.open_mfdataset(
                files,
                concat_dim='time',
                combine='nested',
                preprocess=self._preprocess,
                chunks=self.config['chunks'],
                engine=self.config['engine']
            )

        return ds

    ############################################################################################
    # PROPERTIES: IDENTITY
    ############################################################################################

    @property
    def is_multi_file(self) -> bool:
        """
        True if the dataset is loaded from multiple files, False otherwise.

        This property is useful for determining whether the dataset
        needs to be regridded before storage.
        """
        return len(self.config['files']) > 1

    @property
    def file_count(self) -> int:
        """
        Number of files loaded

        Returns:
            int: Number of files loaded
        """
        # Return the number of files loaded
        return len(self.config['files'])

    @property
    def observation_id(self) -> xr.DataArray:
        """
        Unique identifier for the observation - (time,)

        Returns:
            xr.DataArray: The observation ID(s)
        """
        # Return the observation ID(s)
        return self.ds['observation_id']

    @property
    def dataset_name(self) -> xr.DataArray:
        """
        Filename(s) — (time,)

        Returns:
            xr.DataArray: The filename(s) of the dataset(s)
        """
        # Return the filename(s) of the dataset(s)
        return self.ds['dataset_name']

    ############################################################################################
    # PROPERTIES: METADATA
    ############################################################################################

    @property
    def naming_authority(self) -> xr.DataArray:
        """
        Naming authority — (time,)

        Returns:
            xr.DataArray: The naming authority of the dataset(s)
        """
        # Return the naming authority of the dataset(s)
        return self.ds['naming_authority']

    ############################################################################################
    # PROPERTIES: BAND SELECTION
    ############################################################################################

    @property
    def band(self) -> Union[int, None]:
        """
        Current selected band (1-16) or None if no band is selected.

        Returns:
            Union[int, None]: The current selected band (1-16) or None
        """
        # Return the current selected band (1-16) or None if no band is selected
        return self._current_band

    @band.setter
    def band(self, band_num: int):
        """
        Set current band (1-16).

        Parameters:
        band_num (int): The current selected band (1-16)

        Raises:
        TypeError: If the band is not an integer
        ValueError: If the band is not 1-16
        """
        # Check if the band is an integer
        if not isinstance(band_num, int):
            raise TypeError(f"Band must be an integer, got {type(band_num).__name__}")
        # Check if the band is 1-16
        if not 1 <= band_num <= 16:
            raise ValueError(f"Band must be 1-16, got {band_num}")
        # Set the current selected band
        self._current_band = band_num

    @property
    def band_type(self) -> Union[str, None]:
        """
        Returns the type of the current band.
        'reflectance' for bands 1-6 and 'brightness_temperature' for bands 7-16.

        Returns:
            Union[str, None]: The type of the current band or None if no band is selected
        """
        if self._current_band is None:
            return None
        # Reflectance bands (1-6)
        if self._current_band <= 6:
            return 'reflectance'
        # Brightness temperature bands (7-16)
        else:
            return 'brightness_temperature'

    @property
    def band_wavelength(self) -> Union[float, None]:
        """
        Center wavelength (µm) for current band

        Returns:
            Union[float, None]: Center wavelength (µm) for current band or None if no band is selected
        """
        # Check if a band is selected
        if self._current_band is None:
            return None
        # Get the coordinate name for the band wavelength
        coord_name = f'band_wavelength_C{self._current_band:02d}'
        # Check if the coordinate exists
        if coord_name in self.ds.coords:
            # Return the center wavelength (µm)
            return float(self.ds.coords[coord_name].values)
        # Return None if the coordinate does not exist
        return None

    @property
    def band_id(self) -> Union[int, None]:
        """
        Band identifier for current band.

        Returns:
            Union[int, None]: Band identifier for current band or None if no band is selected
        """
        # Check if a band is selected
        if self._current_band is None:
            # Return None if no band is selected
            return None
        # Get the coordinate name for the band ID
        coord_name = f'band_id_C{self._current_band:02d}'
        # Check if the coordinate exists
        if coord_name in self.ds.coords:
            # Return the band ID as an integer
            return int(self.ds.coords[coord_name].values)
        # Return None if the coordinate does not exist
        return None

    ############################################################################################
    # PROPERTIES: CF DIMENSION COORDINATES
    ############################################################################################


    @property
    def time(self) -> xr.DataArray:
        """
        Time coordinate — (time,)

        Returns:
            xr.DataArray: The time coordinate of the dataset
        """
        return self.ds.coords['time']

    @property
    def y(self) -> xr.DataArray:
        """
        Y scanning angle — (y,)

        The Y scanning angle is the angle of the instrument with respect to the
        vertical axis. It is measured in the along-track direction (i.e., the
        direction of motion of the satellite).

        Returns:
            xr.DataArray: The Y scanning angle of the dataset
        """
        return self.ds.coords['y']

    @property
    def x(self) -> xr.DataArray:
        """
        X scanning angle — (x,)

        The X scanning angle is the angle of the instrument with respect to the
        horizontal axis. It is measured in the cross-track direction (i.e., the
        direction perpendicular to the motion of the satellite).

        Returns:
            xr.DataArray: The X scanning angle of the dataset
        """
        return self.ds.coords['x']

    ############################################################################################
    # PROPERTIES: SATELLITE/INSTRUMENT (time-indexed)
    ############################################################################################

    @property
    def platform_id(self) -> xr.DataArray:
        """
        Satellite identifier

        The satellite identifier is a unique identifier for each satellite.

        Returns:
            xr.DataArray: The satellite identifier of the dataset
        """
        return self.ds['platform_id']

    @property
    def orbital_slot(self) -> xr.DataArray:
        """
        Orbital position — (time,)

        The orbital position is the position of the satellite in its orbit.

        Returns:
            xr.DataArray: The orbital position of the dataset
        """
        return self.ds['orbital_slot']

    @property
    def instrument_type(self) -> xr.DataArray:
        """
        Instrument type — (time,)

        The instrument type is a description of the instrument used to collect the
        data. This can include the type of sensor, spectrometer, or other type of
        instrument.

        Returns:
            xr.DataArray: The instrument type of the dataset
        """
        return self.ds['instrument_type']

    @property
    def instrument_id(self) -> xr.DataArray:
        """
        Instrument serial — (time,)

        The instrument serial is a unique identifier for each instrument.

        Returns:
            xr.DataArray: The instrument serial of the dataset
        """
        return self.ds['instrument_id']

    ############################################################################################
    # PROPERTIES: SCENE/MODE (time-indexed)
    ############################################################################################

    @property
    def scene_id(self) -> xr.DataArray:
        """
        Scene type — (time,)

        The scene type is a description of the scene type (e.g. land, ocean, etc.).

        Returns:
            xr.DataArray: The scene type of the dataset
        """
        return self.ds['scene_id']

    @property
    def scan_mode(self) -> xr.DataArray:
        """
        Scan mode — (time,)

        The scan mode is a description of the mode in which the data was collected.
        For example, this could be "M6" for Advanced Baseline Imager.

        Returns:
            xr.DataArray: The scan mode of the dataset
        """
        return self.ds['scan_mode']

    @property
    def spatial_resolution(self) -> xr.DataArray:
        """
        Spatial resolution in meters — (time,)

        The spatial resolution is the resolution of the data in kilometers.

        Returns:
            xr.DataArray: The spatial resolution of the dataset
        """
        return self.ds['spatial_resolution']

    ############################################################################################
    # PROPERTIES: TEMPORAL (time-indexed)
    ############################################################################################

    @property
    def time_coverage_start(self) -> xr.DataArray:
        """
        Observation start time — (time,)

        The observation start time is the time at which the observation
        started.

        Returns:
            xr.DataArray: The observation start time of the dataset
        """
        return self.ds['time_coverage_start']

    @property
    def time_coverage_end(self) -> xr.DataArray:
        """
        Observation end time — (time,)

        The observation end time is the time at which the observation
        ended.

        Returns:
            xr.DataArray: The observation end time of the dataset
        """
        return self.ds['time_coverage_end']

    @property
    def date_created(self) -> xr.DataArray:
        """
        Product creation time — (time,)

        The product creation time is the time at which the product was created.

        Returns:
            xr.DataArray: The product creation time of the dataset
        """
        return self.ds['date_created']

    @property
    def time_bounds(self) -> xr.DataArray:
        """
        Time bounds for each observation — (time, 2)

        The time bounds are the start and end times for each observation.

        Returns:
            xr.DataArray: The time bounds of the dataset
        """
        # Check if time_bounds is present in the dataset
        if 'time_bounds' in self.ds:
            # Return the time bounds if present
            return self.ds['time_bounds']
        else:
            # Return None if time_bounds is not present
            return None

    @property
    def time_range(self) -> tuple:
        """
        Convenience property to get the time range of the dataset.

        Returns a tuple of (earliest_start, latest_end) times.
        """
        # Convert time_coverage_start and time_coverage_end to datetime objects
        starts = pd.to_datetime(self.time_coverage_start.values)
        ends = pd.to_datetime(self.time_coverage_end.values)

        # Return a tuple of the earliest start and latest end times
        return (starts.min(), ends.max())

    @property
    def last_timestamp(self) -> np.datetime64:
        """
        Last timestamp in the dataset — for continuity checks with the next batch.

        Returns the last timestamp in the dataset. This property is used to
        check for continuity with the next batch of data.

        Returns:
            np.datetime64: The last timestamp in the dataset
        """
        return self.time.values[-1]

    @property
    def first_timestamp(self) -> np.datetime64:
        """
        First timestamp in the dataset — for continuity checks with the previous batch.

        Returns the first timestamp in the dataset. This property is used to
        check for continuity with the previous batch of data.

        Returns:
            np.datetime64: The first timestamp in the dataset
        """
        return self.time.values[0]

    ############################################################################################
    # PROPERTIES: PRODUCTION (time-indexed)
    ############################################################################################

    @property
    def production_site(self) -> xr.DataArray:
        """
        Production facility - (time,)

        The production facility is the location where the data was produced.

        Returns:
            xr.DataArray: The production facility of the dataset
        """
        return self.ds['production_site']

    @property
    def production_environment(self) -> xr.DataArray:
        """
        Production environment — (time,)

        The production environment is the environment in which the
        data was produced. It is either "OE" (operational
        environment) or "DE" (developmental environment).

        Returns:
            xr.DataArray: The production environment of the dataset
        """
        return self.ds['production_environment']

    @property
    def production_data_source(self) -> xr.DataArray:
        """
        Production data source — (time,)

        The production data source is the source of the data. It can be either
        "Realtime", "Simulated", etc.

        Returns:
            xr.DataArray: The production data source of the dataset
        """
        return self.ds['production_data_source']

    @property
    def processing_level(self) -> xr.DataArray:
        """
        Processing level of the data - (time,)

        The processing level is a description of the level of processing
        that has been applied to the data. For example, "L1" and "L2"
        are common processing levels used by NASA.

        Returns:
            xr.DataArray: The processing level of the dataset
        """
        return self.ds['processing_level']

    ############################################################################################
    # PROPERTIES: STANDARDS (time-indexed)
    ############################################################################################

    @property
    def conventions(self) -> xr.DataArray:
        """
        CF version - (time,)

        The CF version is the version of the Climate and Forecasting
        (CF) convention used in the dataset.

        Returns:
            xr.DataArray: The CF version of the dataset
        """
        return self.ds['conventions']

    @property
    def metadata_conventions(self) -> xr.DataArray:
        """
        Unidata conventions — (time,)

        The Unidata conventions are the conventions used by the Unidata
        library to describe the metadata of the dataset.

        Returns:
            xr.DataArray: The Unidata conventions of the dataset
        """
        return self.ds['metadata_conventions']

    @property
    def standard_name_vocabulary(self) -> xr.DataArray:
        """
        CF Standard Name Table version — (time,)

        The CF Standard Name Table version is the version of the CF
        Standard Name Table used in the dataset.

        Returns:
            xr.DataArray: The CF Standard Name Table version of the dataset
        """
        return self.ds['standard_name_vocabulary']

    ############################################################################################
    # PROPERTIES: DOCUMENTATION (time-indexed)
    ############################################################################################

    @property
    def title(self) -> xr.DataArray:
        """
        Product title - (time,)

        The product title is a short description of the product.

        Returns:
            xr.DataArray: The product title of the dataset
        """
        return self.ds['title']

    @property
    def summary(self) -> xr.DataArray:
        """
        Product description - (time,)

        A longer description of the product.

        Returns:
            xr.DataArray: The product description of the dataset
        """
        return self.ds['summary']

    @property
    def institution(self) -> xr.DataArray:
        """
        Institution responsible for the product - (time,)

        The institution is the organization responsible for creating the
        product.

        Returns:
            xr.DataArray: The institution responsible for the product
        """
        return self.ds['institution']

    ############################################################################################
    # PROPERTIES: DOCUMENTATION (time-indexed)
    ############################################################################################

    @property
    def project(self) -> xr.DataArray:
        """
        Project name - (time,)

        The project name is the name of the project that the data
        belongs to.

        Returns:
            xr.DataArray: The project name of the dataset
        """
        return self.ds['project']

    @property
    def license(self) -> xr.DataArray:
        """
        License information.

        This property provides information about the license under which the
        data is released.

        Returns:
            xr.DataArray: The license information of the dataset
        """
        return self.ds['license']

    @property
    def keywords(self) -> xr.DataArray:
        """
        GCMD keywords - (time,)

        The GCMD keywords are a set of keywords used to describe the
        dataset. They are used to help search for the dataset.

        Returns:
            xr.DataArray: The GCMD keywords of the dataset
        """
        # Return the GCMD keywords of the dataset
        return self.ds['keywords']

    @property
    def keywords_vocabulary(self) -> xr.DataArray:
        """
        Keywords vocabulary version - (time,)

        The keywords vocabulary version is the version of the keywords
        vocabulary used in the dataset.

        Returns:
            xr.DataArray: The keywords vocabulary version of the dataset
        """
        # Return the keywords vocabulary version of the dataset
        return self.ds['keywords_vocabulary']

    @property
    def cdm_data_type(self) -> xr.DataArray:
        """
        CDM data type — (time,)

        The CDM data type is a string that describes the type of data
        in the dataset. It is used by the CDM library to identify the type
        of data in the dataset.

        Returns:
            xr.DataArray: The CDM data type of the dataset
        """
        return self.ds['cdm_data_type']

    @property
    def iso_series_metadata_id(self) -> xr.DataArray:
        """
        ISO metadata UUID — (time,)

        The ISO metadata UUID is a unique identifier for the metadata of
        the dataset. It is used to identify the metadata of the dataset.

        Returns:
            xr.DataArray: The ISO metadata UUID of the dataset
        """
        # Return the ISO metadata UUID of the dataset
        return self.ds['iso_series_metadata_id']

    ############################################################################################
    # PROPERTIES: COORDINATE REFERENCE
    ############################################################################################

    @property
    def projection(self) -> dict:
        """
        Geostationary projection parameters.

        Returns a dictionary containing all "goes_imager_projection" attributes.
        Used by the regridder to transform the data from the instrument's
        native projection to a latitude/longitude grid.

        Returns:
            dict: A dictionary containing all "goes_imager_projection" attributes.
        """
        # Check if the goes_imager_projection variable exists in the dataset
        if 'goes_imager_projection' in self.ds:
            # Return a dictionary containing all goes_imager_projection attrs
            return dict(self.ds['goes_imager_projection'].attrs)
        # If the variable does not exist, return an empty dictionary
        return {}

    @property
    def satellite_position(self) -> dict:
        """
        Returns a dictionary containing the following satellite position
        information:

        - height: Nominal satellite height (meters)
        - subpoint_lon: Nominal satellite subpoint longitude (degrees East)
        - subpoint_lat: Nominal satellite subpoint latitude (degrees North)

        :return: A dictionary containing the satellite position information
        :rtype: dict
        """
        position = {}

        # Nominal satellite height (meters)
        if 'nominal_satellite_height' in self.ds:
            position['height'] = float(self.ds['nominal_satellite_height'].values)

        # Nominal satellite subpoint longitude (degrees East)
        if 'nominal_satellite_subpoint_lon' in self.ds:
            position['subpoint_lon'] = float(self.ds['nominal_satellite_subpoint_lon'].values)

        # Nominal satellite subpoint latitude (degrees North)
        if 'nominal_satellite_subpoint_lat' in self.ds:
            position['subpoint_lat'] = float(self.ds['nominal_satellite_subpoint_lat'].values)

        return position

    ############################################################################################
    # PROPERTIES: CF FIELD CONSTRUCTS (raw geostationary data)
    ############################################################################################

    @property
    def cmi(self) -> xr.DataArray:
        """
        Cloud and Moisture Imagery (CMI) for current band.

        Raw geostationary projection. Requires regridding before storage.

        Returns:
            xr.DataArray: CMI for current band (time, y, x)
        """
        if self._current_band is None:
            raise ValueError("No band selected. Set .band first")

        # Return the CMI for the current band
        return self.get_cmi(self._current_band)

    @property
    def dqf(self) -> xr.DataArray:
        """
        Direct Quality Flags (DQF) for the current band.

        Raw geostationary projection. Requires regridding before storage.

        Returns:
            xr.DataArray: DQF for the current band (time, y, x)
        """
        if self._current_band is None:
            raise ValueError("No band selected. Set .band first")

        # Get the DQF for the current band
        return self.get_dqf(self._current_band)

    @property
    def cmi_statistics(self) -> Union[dict, None]:
        """
        Statistics for current band — each value is (time,)
        Keys: min, max, mean, std_dev, outlier_count

        Statistics are computed for each band in the dataset,
        and are available for both reflectance factor and brightness
        temperature bands.

        Returns:
            Union[dict, None]: Statistics for current band if available,
                else None
        """
        if self._current_band is None:
            return None

        band_str = f'C{self._current_band:02d}'
        stats = {}

        # Reflectance stats (bands 1-6)
        if self._current_band <= 6:
            for key in ['min', 'max', 'mean', 'std_dev']:
                # Variable name for reflectance statistics
                var_name = f'{key}_reflectance_factor_{band_str}'
                if var_name in self.ds:
                    stats[key] = self.ds[var_name]
        else:
            for key in ['min', 'max', 'mean', 'std_dev']:
                # Variable name for brightness temperature statistics
                var_name = f'{key}_brightness_temperature_{band_str}'
                if var_name in self.ds:
                    stats[key] = self.ds[var_name]

        # Outlier count (all bands)
        outlier_var = f'outlier_pixel_count_{band_str}'
        if outlier_var in self.ds:
            stats['outlier_count'] = self.ds[outlier_var]

        return stats if stats else None

    ############################################################################################
    # PROPERTIES: DATA QUALITY
    ############################################################################################

    @property
    def grb_errors_percent(self) -> xr.DataArray:
        """
        GRB uncorrectable error rate — (time,)

        The GRB uncorrectable error rate is the percentage of GRB data points
        that could not be corrected by the GRB algorithm.

        Returns:
            xr.DataArray: GRB uncorrectable error rate (time,)
        """
        # Check if the GRB uncorrectable error rate is present in the dataset
        if 'percent_uncorrectable_GRB_errors' in self.ds:
            # Return the GRB uncorrectable error rate if present
            return self.ds['percent_uncorrectable_GRB_errors']
        # If the variable does not exist, return None
        return None

    @property
    def l0_errors_percent(self) -> xr.DataArray:
        """
        L0 uncorrectable error rate — (time,)

        The L0 uncorrectable error rate is the percentage of L0 data points
        that could not be corrected by the L0 algorithm.

        Returns:
            xr.DataArray: L0 uncorrectable error rate (time,)
        """
        # Check if the L0 uncorrectable error rate is present in the dataset
        if 'percent_uncorrectable_L0_errors' in self.ds:
            # Return the L0 uncorrectable error rate if present
            return self.ds['percent_uncorrectable_L0_errors']
        # If the variable does not exist, return None
        return None

    @property
    def skipped_files(self) -> list:
        """
        List of files skipped during preprocessing (strict=False mode).

        If strict=False, the preprocessor will skip and log files that have
        inconsistent data (e.g. different orbital slots, invalid
        timestamps, etc.). This property returns the list of skipped file
        names.

        Returns:
            list: List of skipped file names
        """
        return self._skipped_files.copy()

    ############################################################################################
    # METHODS: DATA ACCESS
    ############################################################################################

    def get_cmi(self, band: int) -> xr.DataArray:
        """
        Direct CMI access without changing band selection.

        Parameters:
        band (int): The band number to access (1-16)

        Returns:
            xr.DataArray: The CMI data for the specified band (time, y, x)

        Raises:
            ValueError: If band is not 1-16
            KeyError: If the CMI variable is not found in the dataset
        """
        # Check if the band is valid
        if not 1 <= band <= 16:
            raise ValueError(f"Band must be 1-16, got {band}")

        # Construct the variable name for the CMI band
        var_name = f'CMI_C{band:02d}'

        # Check if the variable exists in the dataset
        if var_name not in self.ds:
            raise KeyError(f"{var_name} not found in dataset")

        # Return the CMI data for the specified band
        return self.ds[var_name]

    def get_dqf(self, band: int) -> xr.DataArray:
        """
        Direct DQF access without changing band selection.

        Parameters:
        band (int): The band number to access (1-16)

        Returns:
            xr.DataArray: The DQF data for the specified band (time, y, x)

        Raises:
            ValueError: If band is not 1-16
            KeyError: If the DQF variable is not found in the dataset
        """
        # Check if the band is valid
        if not 1 <= band <= 16:
            raise ValueError(f"Band must be 1-16, got {band}")

        # Construct the variable name for the DQF band
        var_name = f'DQF_C{band:02d}'

        # Check if the variable exists in the dataset
        if var_name not in self.ds:
            raise KeyError(f"{var_name} not found in dataset")

        # Return the DQF data for the specified band
        return self.ds[var_name]

    def get_all_cmi(self) -> dict:
        """
        Get all 16 CMI bands from the dataset.

        Returns a dictionary where the keys are the band numbers (1-16)
        and the values are the corresponding CMI data as an xr.DataArray.

        This function is useful for accessing all CMI bands at once,
        rather than having to call the `get_cmi` method for each band.

        :return: A dictionary where the keys are the band numbers and the values are the CMI data
        :rtype: dict
        """
        cmi_dict = {}
        for band in range(1, 17):
            var_name = f'CMI_C{band:02d}'
            if var_name in self.ds:
                # Get the CMI data for the current band
                cmi_dict[band] = self.ds[var_name]
        return cmi_dict

    def get_all_dqf(self) -> dict:
        """
        Get all 16 DQF bands from the dataset.

        Returns a dictionary where the keys are the band numbers (1-16)
        and the values are the corresponding DQF data as an xr.DataArray.

        This function is useful for accessing all DQF bands at once,
        rather than having to call the `get_dqf` method for each band.

        :return: A dictionary where the keys are the band numbers and the values are the DQF data
        :rtype: dict
        """
        dqf_dict = {}
        for band in range(1, 17):
            # Construct the variable name for the DQF band
            var_name = f'DQF_C{band:02d}'

            # Check if the variable exists in the dataset
            if var_name in self.ds:
                # Get the DQF data for the current band
                dqf_dict[band] = self.ds[var_name]
        return dqf_dict

    def isel_time(self, idx: int) -> xr.Dataset:
        """
        Extract a single observation from the dataset

        Parameters:
        idx (int): The index of the observation to extract

        Returns:
        xr.Dataset: A single-timestep xrDataset containing the extracted observation

        Convenience function for extracting one observation for regridding
        """
        # Extract the observation at the specified index
        return self.ds.isel(time=idx)

    ############################################################################################
    # METHODS: METADATA EXPORT
    ############################################################################################

    def to_metadata_df(self) -> pd.DataFrame:
        """
        Output: DataFrame with one row per observation
        Job: Export all promoted attrs as columns for CSV cataloging
             Columns match PROMOTED_ATTRS values + file_path

        This function is useful for exporting the dataset's metadata to a CSV file.
        The resulting DataFrame contains one row per observation in the dataset, with columns
        corresponding to the promoted attributes and the file path (if available).
        """
        records = self.to_metadata_records()
        # Convert the list of records to a DataFrame
        df = pd.DataFrame(records)
        return df

    def to_metadata_records(self) -> list:
        """
        Output: list of dicts, one per observation
        Job: Same as to_metadata_df but as records for DB insertion
        """
        records = []
        n_times = len(self.time)

        # Iterate over each time step in the dataset
        for i in range(n_times):
            record = {}

            # Add all promoted attributes (e.g. scene_id, spatial_resolution, etc.)
            for target_var, promoted_var in self.PROMOTED_ATTRS.items():
                if target_var in self.ds:
                    value = self.ds[target_var].isel(time=i).values
                    # Convert to Python native types
                    if isinstance(value, (np.integer, np.floating)):
                        value = value.item()
                    elif isinstance(value, np.ndarray):
                        value = value.item() if value.size == 1 else str(value)
                    record[promoted_var] = value

            # Add file path if available
            if i < len(self.config['files']):
                record['file_path'] = str(self.config['files'][i])

            records.append(record)

        return records

    ############################################################################################
    # METHODS: VALIDATION
    ############################################################################################

    def validate_cf_compliance(self) -> dict:
        """
        Output: {'compliant': bool, 'issues': list[str], 'warnings': list[str]}
        Job: Check CF attributes present and valid

        This function checks the dataset for compliance with CF conventions.
        It checks for the presence of required attributes, the validity of the time
        coordinate, and the presence of projection information.

        Returns:
        dict: A dictionary containing the compliance result, a list of issues, and a list of warnings
        """
        issues = []  # List of issues found in the dataset
        warnings = []  # List of warnings found in the dataset

        # Check for required CF conventions
        if 'conventions' not in self.ds:
            issues.append("Missing 'conventions' variable")

        # Check dimension coordinates
        required_coords = ['time', 'y', 'x']  # Required CF coordinates
        for coord in required_coords:
            if coord not in self.ds.coords:
                issues.append(f"Missing required coordinate: {coord}")

        # Check for projection info
        if 'goes_imager_projection' not in self.ds:
            warnings.append("Missing 'goes_imager_projection' variable")

        # Check time coordinate validity
        if 'time' in self.ds.coords:
            time_vals = self.ds.coords['time'].values
            if not np.all(np.diff(time_vals.astype('int64')) > 0):
                issues.append("Time coordinate is not monotonically increasing")

        compliant = len(issues) == 0  # Dataset is compliant if no issues are found

        return {
            'compliant': compliant,
            'issues': issues,
            'warnings': warnings
        }

    def validate_consistency(self) -> dict:
        """
        Output: {'consistent': bool, 'issues': list[str]}
        Job: Check multi-file consistency:
            - Same scene_id across all files
            - Same spatial_resolution
            - Same orbital_slot
            - Monotonic time ordering
        """
        issues = []

        if not self.is_multi_file:
            # If single file, return consistent
            return {'consistent': True, 'issues': []}

        # Check scene_id consistency
        scene_ids = self.scene_id.values
        if len(np.unique(scene_ids)) > 1:
            # If multiple scene_ids, add issue
            issues.append(f"Inconsistent scene_id: {np.unique(scene_ids)}")

        # Check spatial_resolution consistency
        resolutions = self.spatial_resolution.values
        if len(np.unique(resolutions)) > 1:
            # If multiple spatial_resolution, add issue
            issues.append(f"Inconsistent spatial_resolution: {np.unique(resolutions)}")

        # Check orbital_slot consistency
        slots = self.orbital_slot.values
        if len(np.unique(slots)) > 1:
            # If multiple orbital_slot, add issue
            issues.append(f"Inconsistent orbital_slot: {np.unique(slots)}")

        # Check monotonic time ordering
        time_vals = self.time.values.astype('int64')
        if not np.all(np.diff(time_vals) > 0):
            # If time values not monotonically increasing, add issue
            issues.append("Time values are not monotonically increasing")

        consistent = len(issues) == 0

        return {
            'consistent': consistent,
            'issues': issues
        }

    def validate_temporal_continuity(self, previous_last: np.datetime64) -> bool:
        """
        Validate that there is no temporal overlap/regression between batches.

        Parameters:
        previous_last (np.datetime64): Last timestamp from previous batch

        Returns:
        bool: True if the first timestamp of the current batch is greater than the last timestamp of the previous batch

        Job: Ensure no temporal overlap/regression between batches
        """
        # Check if the first timestamp of the current batch is greater than the last timestamp of the previous batch
        return self.first_timestamp > previous_last

    ############################################################################################
    # CONTEXT MANAGER
    ############################################################################################

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        This method is called when the execution passes to the line right after an object of this class is used in a with statement.

        Returns:
            GOESMultiCloudObservation: The object itself.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """
        Close the underlying dataset and release all file handles.

        This method is used to clean up any system resources used by the dataset.
        """
        if hasattr(self, 'ds') and self.ds is not None:
            # Close the underlying dataset to release file handles
            self.ds.close()
            # Set the dataset to None to prevent accidental use
            self.ds = None

    ############################################################################################
    # DUNDER
    ############################################################################################

    def __repr__(self) -> str:
        """
        Representation of the GOESMultiCloudObservation object.

        Returns a string representation of the object in the form:
        GOESMultiCloudObservation(platform='G18', slot='GOES-West', times=24, band=8)

        The string representation includes the platform, orbital slot, number of time steps,
        and the selected band (if applicable).
        """
        platform = self.platform_id.values[0] if len(self.platform_id) > 0 else 'unknown'
        slot = self.orbital_slot.values[0] if len(self.orbital_slot) > 0 else 'unknown'
        n_times = len(self.time)
        band_str = f", band={self.band}" if self.band is not None else ""

        return f"GOESMultiCloudObservation(platform='{platform}', slot='{slot}', times={n_times}{band_str}"

    def __len__(self) -> int:
        """
        Return the number of time steps in the dataset.

        This method is used by the built-in len() function to get the size of the object.
        It is also used by the built-in bool() function to check if the object is empty.

        Returns:
            int: The number of time steps in the dataset.
        """
        return len(self.time)