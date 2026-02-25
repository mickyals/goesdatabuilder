import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
import os
from typing import Union, Optional
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

    Pipeline:
        GOESMultiCloudObservation (this) → Regridder → GOESZarrStore

    Lazy vs Eager Operations:
    -------------------------
    Most properties return xr.DataArray objects without triggering computation,
    preserving Dask's lazy evaluation:
        - cmi, dqf: Lazy access to imagery data (time, y, x)
        - All promoted attributes: Lazy access to metadata variables (time,)
        - Coordinates (time, x, y): Lazy access

    The following properties trigger computation on small metadata:
        - time_range: Computes min/max from time_coverage_start/end (one value per file)
        - first_timestamp, last_timestamp: Accesses time coordinate (1D array)
        - band_wavelength, band_id: Accesses scalar coordinates (single values)
        - satellite_position: Accesses scalar variables (single values)
        - validate_*: Validation methods explicitly compute to check consistency

    For large-scale processing, use the lazy properties (cmi, dqf) and avoid
    calling eager properties in loops.
    """

    ############################################################################################
    # CLASS CONSTANTS
    ############################################################################################

    # Metadata field mapping from NetCDF attributes to catalog columns
    # These are the global attributes that get promoted to time-indexed variables
    # in GOESMultiCloudObservation for proper concatenation and provenance tracking
    # The key is the GOES Attribute, the value is the new corresponding Zarr variable name

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

        # Scene
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
    # CONUS - MCMIPC
    # Mesoscale - MCMIPM
    # FullDisk - MCMIPF
    GOES_FILENAME_PATTERN = re.compile(
        r'OR_ABI-L2-MCMIP(?P<scene>[FCM])-M(?P<mode>\d)_G(?P<satellite>\d{2})_s(?P<start>\d{14})_e(?P<end>\d{14})_c(?P<created>\d{14})\.nc'
    )

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(self, config: Union[dict, str, Path], strict: bool = None):
        """
        Initialize a GOESMultiCloudObservation instance.

        Parameters:
        config (dict, str, Path): Configuration dictionary or path to YAML/JSON file.
        strict (bool, optional): Whether to raise an error if a file is invalid. Defaults to None.

        Attributes:
        config (dict): Configuration dictionary.
        strict (bool): Whether to raise an error if a file is invalid.
        _current_band (int): The currently selected band.
        _skipped_files (list): A list of skipped file paths.
        ds (xarray.Dataset): The currently open dataset.
        """

        # Load and validate configuration
        self.config = self._validate_and_load_config(config)

        # Use strict from parameter or config
        if strict is not None:
            self.strict = strict
        else:
            self.strict = self.config.get('strict', True)

        # Initialize instance variables
        self._current_band = None
        self._skipped_files = []

        # Open the dataset
        self.ds = self._open_dataset()

    def _validate_and_load_config(self, config, sample_size: int = None) -> dict:
        """
        Validate and load a configuration dictionary from a file path or dict.

        This method takes a configuration dictionary or path to a YAML/JSON file,
        validates the structure and content, and loads the configuration into a dictionary.

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

        # Determine sample size for the number random file to validate.
        # Use a sample size that will provide a representative view of your data
        if sample_size is None:
            sample_size = data_config.get('sample_size', 5)

        # Get files either from 'files' list or 'file_dir' directory

        if 'files' in data_config:
            data_files = data_config['files']
            if not data_files:
                raise ConfigError("Config 'files' list is empty")
            # Convert to Path and validate GOES pattern
            validated_files = []
            for f in data_files:
                p = Path(f)
                if self.GOES_FILENAME_PATTERN.match(p.name):
                    validated_files.append(p)
                else:
                    raise ConfigError(
                        f"File does not match GOES MCMIP pattern: {p.name}"
                    )
            files = validated_files
        elif 'file_dir' in data_config:
            file_dir_str = data_config['file_dir']

            # Expand env vars if present, then convert to Path
            file_dir = Path(os.path.expandvars(file_dir_str))

            if not file_dir.exists():
                raise ConfigError(f"Directory not found: {file_dir}")

            if not file_dir.is_dir():
                raise ConfigError(f"Not a directory: {file_dir}")

            # Determine if recursive search (default: True)
            recursive = data_config.get('recursive', True)

            # Search for all .nc files
            if recursive:
                nc_files = list(file_dir.rglob('*.nc'))
            else:
                nc_files = list(file_dir.glob('*.nc'))

            if not nc_files:
                search_type = "recursively" if recursive else "in top-level directory"
                raise ConfigError(f"No .nc files found {search_type} in {file_dir}")

            logger.info(f"Found {len(nc_files)} .nc files in {file_dir} (recursive={recursive})")

            # Filter using GOES filename pattern regex
            files = []
            for f in nc_files:
                if self.GOES_FILENAME_PATTERN.match(f.name):
                    files.append(f)
                else:
                    logger.debug(f"Skipping non-GOES file: {f.name}")

            if not files:
                raise ConfigError(
                    f"No valid GOES MCMIP files found matching pattern "
                    f"'OR_ABI-L2-MCMIP[FCM]-M*_G**_s*_e*_c*.nc' in {file_dir}"
                )

            logger.info(f"Filtered to {len(files)} valid GOES MCMIP files from {len(nc_files)} potential files")

        else:
            raise ConfigError("Config must contain either 'files' or 'file_dir' key")

        # Extract timestamps and sort files
        file_timestamps = []
        for f in files:
            match = self.GOES_FILENAME_PATTERN.match(f.name)
            if not match:
                # Shouldn't happen since we already filtered, but defensive
                logger.warning(f"Unexpected: file passed filter but doesn't match pattern: {f.name}")
                continue

            # Extract start timestamp (s20240030200212 -> datetime)
            timestamp_str = match.group('start')
            timestamp = datetime.strptime(timestamp_str, '%Y%j%H%M%S')
            file_timestamps.append((f, timestamp))

        if not file_timestamps:
            raise ConfigError("No valid GOES files with parseable timestamps")

        # Sort by timestamp
        file_timestamps.sort(key=lambda x: x[1])
        sorted_files = [f for f, _ in file_timestamps]

        logger.info(
            f"Validated {len(sorted_files)} files spanning "
            f"{file_timestamps[0][1]} to {file_timestamps[-1][1]}"
        )

        # Sample files for validation
        n_sample = min(sample_size, len(sorted_files))
        if n_sample < len(sorted_files):
            sampling_type = data_config.get('sampling_type', 'even')

            if sampling_type == 'even':
                # Sample evenly across time range for better coverage
                sample_indices = [int(i * len(sorted_files) / n_sample) for i in range(n_sample)]
                sample_files = [sorted_files[i] for i in sample_indices]

            elif sampling_type == 'random':
                seed = data_config.get('seed', 42)
                logger.info(f"Using seed {seed} for random number generator")
                rng = np.random.default_rng(seed=seed)
                sample_indices = rng.choice(len(sorted_files), size=n_sample, replace=False)
                sample_files = [sorted_files[i] for i in sample_indices]

            else:
                raise ConfigError(f"Unknown sampling type: {sampling_type}. Must be 'even' or 'random'.")

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
                    if 't' not in ds.coords:
                        raise ConfigError(f"Missing 't' coordinate in {f.name}")
                    if 'orbital_slot' not in ds.attrs:
                        raise ConfigError(f"Missing 'orbital_slot' attribute in {f.name}")
            except ConfigError:
                raise
            except Exception as e:
                raise ConfigError(f"Failed to open {f.name}: {e}")

        # Build flat config for internal use
        flat_config = {
            'files': sorted_files,
            'engine': data_config.get('engine', 'netcdf4'),
            'strict': data_config.get('strict', True),
            'chunks': data_config.get('chunk_size', 'auto'),
            'parallel': data_config.get('parallel', False),
            '_original_config': config
        }

        return flat_config

    def _preprocess(self, ds: xr.Dataset) -> Optional[xr.Dataset]:
        """
        Preprocess a single GOES MCMIP file.

        This method is called by the _open_dataset method, which opens a dataset from a list of files.
        If there is only one file, this method is called to preprocess the dataset.
        If there are multiple files, this method is passed as the preprocess argument to xr.open_mfdataset,
        which will apply the preprocessing to each file individually.

        The preprocessing method checks the orbital slot of the dataset and
        expands the dataset to have a 'time' dimension if it doesn't already have one.
        It also adds some variables to the dataset from the attributes of the dataset.

        :param ds: The GOES MCMIP file to preprocess.
        :return: The preprocessed dataset, or None if the file is invalid.
        """

        filename = ds.attrs.get('dataset_name', 'unknown')

        # Check the orbital slot
        orbital_slot = ds.attrs.get('orbital_slot')
        if orbital_slot not in self.VALID_ORBITAL_SLOTS:
            msg = f"Invalid orbital_slot '{orbital_slot}' in {filename}"
            if self.strict:
                # If strict mode is enabled, raise an error
                raise ValueError(msg)
            else:
                # If strict mode is disabled, log an error and skip the file
                logger.error(msg)
                self._skipped_files.append(filename)
                return None

        # Expand the dataset to have a 'time' dimension
        ds = ds.expand_dims('time')

        # Check if the dataset already has a 'time' coordinate
        if 't' in ds.coords:
            # If it does, assign the values to a new 'time' variable
            ds = ds.assign_coords(time=('time', [ds.coords['t'].values]))
        else:
            # If it doesn't, raise an error
            raise ValueError(f"Missing 't' coordinate in {filename}")

        # Add some variables to the dataset from the attributes of the dataset
        for source_attr, target_var in self.PROMOTED_ATTRS.items():
            if source_attr in ds.attrs:
                value = ds.attrs[source_attr]
                ds[target_var] = xr.DataArray(
                    [value],
                    dims=['time'],
                    coords={'time': ds.coords['time']}
                )

        return ds

    def _open_dataset(self) -> xr.Dataset:
        """
        Open a dataset from a list of files.

        If there is only one file, open it directly using xr.open_dataset.
        If there are multiple files, open them using xr.open_mfdataset.

        If there is only one file, preprocess it using the _preprocess method.
        If there are multiple files, pass the _preprocess method as the preprocess argument to xr.open_mfdataset,
        which will apply the preprocessing to each file individually.

        The preprocessing method checks the orbital slot of the dataset and
        expands the dataset to have a 'time' dimension if it doesn't already have one.
        It also adds some variables to the dataset from the attributes of the dataset.

        """

        files = self.config['files']

        if not self.is_multi_file:
            # Open the single file directly
            ds = xr.open_dataset(
                files[0],
                chunks=self.config['chunks'],
                engine=self.config['engine']
            )
            # Preprocess the single file
            ds = self._preprocess(ds)
        else:
            # Open the multiple files using xr.open_mfdataset
            ds = xr.open_mfdataset(
                files,
                concat_dim='time',
                combine='nested',
                preprocess=self._preprocess,
                chunks=self.config['chunks'],
                engine=self.config['engine'],
                parallel=self.config['parallel']
            )

        return ds

    ############################################################################################
    # PROPERTIES: IDENTITY
    ############################################################################################

    @property
    def is_multi_file(self) -> bool:
        """
        Whether the dataset spans multiple files
        """
        return len(self.config['files']) > 1

    @property
    def file_count(self) -> int:
        """
        The number of files in the dataset.
        """
        return len(self.config['files'])

    @property
    def observation_id(self) -> xr.DataArray:
        """
        The observation ID is a unique identifier for the observation.

        Returns
        -------
        xr.DataArray
            A DataArray containing the observation ID of the data.
        """
        return self.ds['observation_id']

    @property
    def dataset_name(self) -> xr.DataArray:
        """
        The original filename name is a string that identifies the dataset.

        Returns
        -------
        xr.DataArray
            The name of the dataset.
        """
        return self.ds['dataset_name']

    @property
    def naming_authority(self) -> xr.DataArray:
        """
        The naming authority is a string that identifies the source of the dataset, such as the organization or institution that produced the data.

        Returns
        -------
        xr.DataArray
            The naming authority for the dataset.
        """
        return self.ds['naming_authority']

    ############################################################################################
    # PROPERTIES: BAND SELECTION
    ############################################################################################

    @property
    def band(self) -> Optional[int]:
        """
        This property returns the currently selected band number (1-16) or None if no band has been selected.

        Returns
        -------
        Optional[int]
            The currently selected band number, or None if no band has been selected.
        """
        return self._current_band

    @band.setter
    def band(self, band_num: int):
        """
        Set the currently selected band.

        Parameters
        ----------
        band_num : int
            The band number (1-16) to select.

        Raises
        ------
        TypeError
            If the input is not an integer.
        ValueError
            If the band number is not in the range 1-16.

        Notes
        -----
        This property sets the currently selected band and is used to select the band to use for operations.

        """
        # Check if the input is an integer
        if not isinstance(band_num, int):
            raise TypeError(f"Band must be an integer, got {type(band_num).__name__}")

        # Check if the band number is in the range 1-16
        if not 1 <= band_num <= 16:
            raise ValueError(f"Band must be 1-16, got {band_num}")

        # Set the currently selected band
        self._current_band = band_num

    @property
    def band_type(self) -> Optional[str]:
        """
        Band type coordinate.

        This property returns a string containing the band type coordinate of the data.

        The band type coordinate is a single value that represents the type of band (reflectance or brightness temperature).

        Returns
        -------
        Optional[str]
            A string containing the band type coordinate of the data, or None if the band has not been selected.
        """
        if self._current_band is None:
            return None
        # Reflectance bands (1-6)
        if self._current_band < 7:
            return 'reflectance'
        # Brightness temperature bands (7-16)
        else:
            return 'brightness_temperature'

    @property
    def band_wavelength(self) -> Optional[float]:
        """
        The band wavelength coordinate is a single value that represents the wavelength of the data in micrometers.

        Returns
        -------
        Optional[float]
            A float containing the band wavelength coordinate of the data, or None if the band wavelength coordinate is not found in the dataset.
        """
        # Check if a band has been selected
        if self._current_band is None:
            return None

        # Get the band wavelength coordinate from the dataset
        coord_name = f'band_wavelength_C{self._current_band:02d}'
        if coord_name in self.ds.coords:
            # Get the band wavelength value and convert it to a float
            wavelength = self.ds.coords[coord_name].values
            return float(wavelength) if np.isscalar(wavelength) else float(wavelength.item())
        else:
            return None

    @property
    def band_id(self) -> Optional[int]:
        """
        The band ID coordinate is a single value that represents the band number (1-16) of the data.

        Returns
        -------
        Optional[int]
            A DataArray containing the band ID coordinate of the data, or None if the band ID coordinate is not found in the dataset.
        """
        # Check if a band has been selected
        if self._current_band is None:
            return None

        # Get the band ID coordinate name
        coord_name = f'band_id_C{self._current_band:02d}'

        # Check if the band ID coordinate exists in the dataset
        if coord_name in self.ds.coords:
            # Return the band ID coordinate as an integer
            return int(self.ds.coords[coord_name].values)
        else:
            # Return None if the band ID coordinate does not exist
            return None

    ############################################################################################
    # PROPERTIES: CF DIMENSION COORDINATES
    ############################################################################################

    @property
    def time(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the time coordinate of the data.

        Returns
        -------
        xr.DataArray
            A DataArray containing the time coordinate of the data.
        """
        return self.ds.coords['time']

    @property
    def y(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the y-coordinate of the data.

        Returns
        -------
        xr.DataArray
            A DataArray containing the y-coordinate of the data.
        """
        return self.ds.coords['y']

    @property
    def x(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the x-coordinate of the data.

        Returns
        -------
        xr.DataArray
            A DataArray containing the x-coordinate of the data.
        """
        return self.ds.coords['x']

    ############################################################################################
    # PROPERTIES: SATELLITE/INSTRUMENT (time-indexed)
    ############################################################################################

    @property
    def platform_id(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the platform ID of the data.

        Returns
        -------
        xr.DataArray
            A DataArray containing the platform ID of the data.
        """
        return self.ds['platform_id']

    @property
    def orbital_slot(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the orbital slot of the data.

        Returns
        -------
        xr.DataArray
            A DataArray containing the orbital slot of the data.
        """
        return self.ds['orbital_slot']

    @property
    def instrument_type(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the type of instrument that collected the data.

        Returns
        -------
        xr.DataArray
            A DataArray containing the type of instrument that collected the data.
        """
        return self.ds['instrument_type']

    @property
    def instrument_id(self) -> xr.DataArray:
        """
        The serial number of the instrument.

        Returns
        -------
        xr.DataArray
            The instrument ID.
        """
        return self.ds['instrument_id']

    ############################################################################################
    # PROPERTIES: SCENE/MODE (time-indexed)
    ############################################################################################

    @property
    def scene_id(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the scene ID of the data.

        Returns
        -------
        xr.DataArray
            A DataArray containing the scene ID of the data.
        """
        return self.ds['scene_id']

    @property
    def scan_mode(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the scan mode of the data.

        Returns
        -------
        xr.DataArray
            The scan mode of the data.
        """
        return self.ds['scan_mode']

    @property
    def spatial_resolution(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the spatial resolution of the data.
        Note this should be an array of str "Xkm at nadir"

        Returns
        -------
        xr.DataArray
            A DataArray containing the spatial resolution of the data.
        """
        return self.ds['spatial_resolution']

    ############################################################################################
    # PROPERTIES: TEMPORAL (time-indexed)
    ############################################################################################

    @property
    def time_coverage_start(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the start time of the data coverage period.

        Returns
        -------
        xr.DataArray: Start time of the data coverage period.
        """
        return self.ds['time_coverage_start']

    @property
    def time_coverage_end(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the end time of the data coverage period.

        Returns
        -------
        xr.DataArray
            A DataArray containing the end time of the data coverage period.
        """
        return self.ds['time_coverage_end']

    @property
    def date_created(self) -> xr.DataArray:
        """
        This property returns a DataArray containing the date when the dataset was created.

        Returns
        -------
        xr.DataArray
            A DataArray containing the date when the dataset was created.
        """
        return self.ds['date_created']

    @property
    def time_bounds(self) -> Optional[xr.DataArray]:
        """
        This property returns a DataArray containing the time bounds for each scene.
        The DataArray has shape (n_scenes, 2), where the first column contains the start
        time and the second column contains the end time for each scene.

        If the 'time_bounds' variable is not present in the dataset, return None.

        Returns
        -------
        Optional[xr.DataArray]
            The time bounds for each scene, or None if not available.
        """
        if 'time_bounds' in self.ds:
            return self.ds['time_bounds']
        return None

    @property
    def time_range(self) -> tuple:
        """
        Convenience property to get the time range of the dataset.

        This property provides a tuple of two datetime objects: the earliest and
        latest timestamps in the dataset.

        Note: This property triggers computation on time_coverage_start and
        time_coverage_end variables (one value per file). For lazy access,
        use the properties directly.

        Returns:
            tuple: (earliest, latest) timestamps in the dataset.
        """
        # More explicit about computation
        starts = self.time_coverage_start.compute()
        ends = self.time_coverage_end.compute()
        earliest = pd.to_datetime(starts.values).min()
        latest = pd.to_datetime(ends.values).max()
        return earliest, latest

    @property
    def first_timestamp(self) -> np.datetime64:
        """
        This property accesses the time coordinate, which is typically loaded in memory.
        The first timestamp is the timestamp at the first index of the time coordinate.

        Returns:
            np.datetime64: The first timestamp in the dataset.
        """

        return self.time.isel(time=0).values.item()

    @property
    def last_timestamp(self) -> np.datetime64:
        """
        This property accesses the time coordinate, which is typically loaded in memory.
        The last timestamp is the timestamp at the last index of the time coordinate.

        Returns:
            np.datetime64: The last timestamp in the dataset.
        """
        # Use .item() for single value access - clearer intent
        return self.time.isel(time=-1).values.item()

    ############################################################################################
    # PROPERTIES: PRODUCTION (time-indexed)
    ############################################################################################

    @property
    def production_site(self) -> xr.DataArray:
        """
        The site at which the dataset was produced.

        Returns
        -------
        xr.DataArray
            The production site.
        """
        return self.ds['production_site']

    @property
    def production_environment(self) -> xr.DataArray:
        """
        The environment in which the dataset was produced.

        Returns
        -------
        xr.DataArray
            The production environment.
        """
        return self.ds['production_environment']

    @property
    def production_data_source(self) -> xr.DataArray:
        """
        The source of the data used to produce the dataset.

        Returns
        -------
        xr.DataArray
            The production data source.
        """
        return self.ds['production_data_source']

    @property
    def processing_level(self) -> xr.DataArray:
        """
        The processing level of the satellite imagery.

        Returns
        -------
        xr.DataArray
            The processing level of the satellite imagery.
        """
        return self.ds['processing_level']

    ############################################################################################
    # PROPERTIES: STANDARDS (time-indexed)
    ############################################################################################

    @property
    def conventions(self) -> xr.DataArray:
        """
        The conventions used to create the dataset.

        Returns
        -------
        xr.DataArray
            The conventions used to create the dataset.
        """
        return self.ds['conventions']

    @property
    def metadata_conventions(self) -> xr.DataArray:
        """
        The conventions used to create the metadata in the dataset.

        Returns
        -------
        xr.DataArray
            The conventions used to create the metadata in the dataset.
        """
        return self.ds['metadata_conventions']

    @property
    def standard_name_vocabulary(self) -> xr.DataArray:
        """
        The standard name vocabulary used to define the variables in the dataset.

        Returns
        -------
        xr.DataArray
            The standard name vocabulary used to define the variables in the dataset.
        """
        return self.ds['standard_name_vocabulary']

    ############################################################################################
    # PROPERTIES: DOCUMENTATION (time-indexed)
    ############################################################################################

    @property
    def title(self) -> xr.DataArray:
        """
        A short title that describes the dataset.

        Returns
        -------
        xr.DataArray
            A short title that describes the dataset.
        """
        return self.ds['title']

    @property
    def summary(self) -> xr.DataArray:
        """
        A brief summary of the dataset.

        Returns
        -------
        xr.DataArray
            A brief summary of the dataset.
        """
        return self.ds['summary']

    @property
    def institution(self) -> xr.DataArray:
        """
        The institution responsible for collecting the data.

        Returns
        -------
        xr.DataArray
            The institution.
        """
        return self.ds['institution']

    @property
    def project(self) -> xr.DataArray:
        """
        The project under which the data was collected.

        Returns
        -------
        xr.DataArray
            The project.
        """
        return self.ds['project']

    @property
    def license(self) -> xr.DataArray:
        """
        The license under which the data is distributed.

        Returns
        -------
        xr.DataArray
            The license.
        """
        # Get the license variable
        return self.ds['license']

    @property
    def keywords(self) -> xr.DataArray:
        """
        The keywords or phrases describing the data.

        Returns
        -------
        xr.DataArray
            The keywords.
        """
        # Get the keywords variable
        return self.ds['keywords']

    @property
    def keywords_vocabulary(self) -> xr.DataArray:
        """
        The controlled vocabulary used to define the keywords.

        Returns
        -------
        xr.DataArray
            The keywords vocabulary.
        """
        return self.ds['keywords_vocabulary']

    @property
    def cdm_data_type(self) -> xr.DataArray:
        """
        The type of data stored in the CDM.

        Returns
        -------
        xr.DataArray
            The data type of the CDM.
        """
        return self.ds['cdm_data_type']

    @property
    def iso_series_metadata_id(self) -> xr.DataArray:
        """
        A unique identifier for the ISO series metadata record.

        Returns
        -------
        xr.DataArray
            Data array containing the ISO series metadata ID.
        """
        return self.ds['iso_series_metadata_id']

    ############################################################################################
    # PROPERTIES: COORDINATE REFERENCE
    ############################################################################################

    @property
    def projection(self) -> dict:
        """
        if the dataset contains the 'goes_imager_projection' variable, return a dictionary containing the projection attributes.
        Otherwise, return an empty dictionary.

        Returns
        -------
        dict
            Dictionary containing the projection attributes.
        """
        if 'goes_imager_projection' in self.ds:
            return dict(self.ds['goes_imager_projection'].attrs)
        return {}

    @property
    def satellite_position(self) -> dict:
        """
        Return a dictionary containing the satellite position (height, subpoint longitude, subpoint latitude).

        Returns
        -------
        dict
            Dictionary containing the satellite position.
        """
        position = {}

        # Nominal satellite height
        if 'nominal_satellite_height' in self.ds:
            position['height'] = float(self.ds['nominal_satellite_height'].values)

        # Nominal satellite subpoint longitude
        if 'nominal_satellite_subpoint_lon' in self.ds:
            position['subpoint_lon'] = float(self.ds['nominal_satellite_subpoint_lon'].values)

        # Nominal satellite subpoint latitude
        if 'nominal_satellite_subpoint_lat' in self.ds:
            position['subpoint_lat'] = float(self.ds['nominal_satellite_subpoint_lat'].values)

        return position

    ############################################################################################
    # PROPERTIES: CF FIELD CONSTRUCTS (raw geostationary data)
    ############################################################################################

    @property
    def cmi(self) -> xr.DataArray:
        """
        Return the Cloud Moisture Image (CMI) data for the currently selected band.

        Raises
        ------
        ValueError
            If no band has been selected (i.e., .band is None).

        Returns
        -------
        cmi : xr.DataArray
            The CMI data for the currently selected band.
        """
        # Check if band is selected
        if self._current_band is None:
            raise ValueError("No band selected. Set .band first")

        # Return the CMI data for the currently selected band
        return self.get_cmi(self._current_band)

    @property
    def dqf(self) -> xr.DataArray:
        """
        Return the Data Quality Flags (DQF) for the currently selected band.

        The DQF array contains information about the quality of the CMI data
        for each pixel in the currently selected band.

        Raises
        ------
        ValueError
            If no band has been selected (i.e., .band is None).

        Returns
        -------
        dqf : xr.DataArray
            The DQF array for the currently selected band.
        """
        if self._current_band is None:
            raise ValueError("No band selected. Set .band first")
        return self.get_dqf(self._current_band)

    @property
    def cmi_statistics(self) -> Optional[dict]:
        """
        Return a dictionary containing statistics for the currently selected band's CMI.

        The dictionary contains the following keys:
            min: The minimum value in the CMI.
            max: The maximum value in the CMI.
            mean: The mean value in the CMI.
            std_dev: The standard deviation of the values in the CMI.
            outlier_count: The number of outlier pixels in the CMI.

        If the band is not set, or if the band does not have a CMI variable,
        return None.
        """
        if self._current_band is None:
            return None

        band_str = f'C{self._current_band:02d}'
        stats = {}

        # Reflectance stats (bands 1-6)
        if self._current_band < 7:
            for key in ['min', 'max', 'mean', 'std_dev']:
                var_name = f'{key}_reflectance_factor_{band_str}'
                if var_name in self.ds:
                    # Get the variable and store it in the stats dict
                    stats[key] = self.ds[var_name]
        # Brightness temperature stats (bands 7-16)
        else:
            for key in ['min', 'max', 'mean', 'std_dev']:
                var_name = f'{key}_brightness_temperature_{band_str}'
                if var_name in self.ds:
                    # Get the variable and store it in the stats dict
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
    def grb_errors_percent(self) -> Optional[xr.DataArray]:
        """
        The percentage of GRB data that were not usable due to errors.

        This property returns the percentage of GRB data that were not usable due to errors.
        If the percentage is not available, it returns None.

        Returns
        -------
        Optional[xr.DataArray]
            The percentage of GRB data that were not usable due to errors, or None if not available.
        """
        # Check if the variable is present in the dataset
        if 'percent_uncorrectable_GRB_errors' in self.ds:
            return self.ds['percent_uncorrectable_GRB_errors']
        # If not present, return None
        return None

    @property
    def l0_errors_percent(self) -> Optional[xr.DataArray]:
        """
        The percentage of Level 0 (L0) data that were not usable due to errors.

        This property returns the percentage of Level 0 (L0) data that were not usable due to errors.
        If the percentage is not available, it returns None.

        Returns
        -------
        Optional[xr.DataArray]
            The percentage of Level 0 (L0) data that were not usable due to errors, or None if not available.
        """
        if 'percent_uncorrectable_L0_errors' in self.ds:
            return self.ds['percent_uncorrectable_L0_errors']
        return None

    @property
    def skipped_files(self) -> list:
        """
        A list of files that were skipped during data ingestion.

        This property returns a list of file paths that were skipped during data ingestion.
        The list is a copy of the internal list, so modifying it will not affect the internal state of the object.

        Returns
        -------
        list
            A list of file paths that were skipped during data ingestion.
        """
        return self._skipped_files.copy()

    ############################################################################################
    # METHODS: DATA ACCESS
    ############################################################################################

    def get_cmi(self, band: int) -> xr.DataArray:
        """
        Retrieve the Cloud Moisture Image (CMI) for a given band.

        Parameters
        ----------
        band : int
            The band number (1-16) for which to retrieve the CMI.

        Returns
        -------
        xr.DataArray
            The CMI DataArray for the given band.

        Raises
        ------
        ValueError
            If the band number is not in the range 1-16.
        KeyError
            If the CMI variable is not found in the dataset.

        Notes
        -----
        The CMI DataArray is a 2D array containing the cloud moisture
        data for the given band. The first dimension corresponds to the
        y, and the second dimension corresponds to the x.

        The CMI DataArray is stored in the dataset under the variable name
        'CMI_C##', where ## is the band number (01-16).
        """
        if not 1 <= band <= 16:
            raise ValueError(f"Band must be 1-16, got {band}")

        var_name = f'CMI_C{band:02d}'
        if var_name not in self.ds:
            raise KeyError(f"{var_name} not found in dataset")

        # Return the CMI DataArray for the given band
        return self.ds[var_name]

    def get_dqf(self, band: int) -> xr.DataArray:
        """
        Return the Data Quality Flags (DQF) for a given band.

        Parameters
        ----------
        band : int
            The band number (1-16) for which to retrieve the DQF.

        Returns
        -------
        dqf : xr.DataArray
            The DQF array for the given band.

        Raises
        ------
        ValueError
            If band is not 1-16.
        KeyError
            If the DQF variable for the given band is not found in the dataset.
        """
        if not 1 <= band <= 16:
            raise ValueError(f"Band must be 1-16, got {band}")

        var_name = f'DQF_C{band:02d}'
        if var_name not in self.ds:
            raise KeyError(f"{var_name} not found in dataset")

        return self.ds[var_name]

    def get_all_cmi(self) -> dict:
        """
        Return a dictionary containing all the Cloud Moisture Images (CMI)
        in the dataset.

        The dictionary is keyed by band number (1-16) and the values
        are the CMI DataArrays for each band.

        This function iterates over all possible band numbers (1-16), checks
        if the corresponding CMI variable exists in the dataset, and if so,
        adds it to the dictionary.

        If a band does not have a CMI variable, it will not be included
        in the dictionary.
        """
        cmi_dict = {}
        for band in range(1, 17):
            var_name = f'CMI_C{band:02d}'
            if var_name in self.ds:
                cmi_dict[band] = self.ds[var_name]
        return cmi_dict

    def get_all_dqf(self) -> dict:
        """
        Return a dictionary containing all the Data Quality Flags (DQF)
        in the dataset.

        The dictionary is keyed by band number (1-16) and the values
        are the DQF DataArrays for each band.

        If a band does not have a DQF variable, it will not be included
        in the dictionary.
        """
        dqf_dict = {}
        for band in range(1, 17):
            var_name = f'DQF_C{band:02d}'
            if var_name in self.ds:
                # Get the DQF DataArray for this band
                dqf_da = self.ds[var_name]
                # Store it in the dictionary
                dqf_dict[band] = dqf_da
        return dqf_dict

    def isel_time(self, idx: int) -> xr.Dataset:
        """
        Return a new Dataset with only the data at the specified time index.

        This method is used to extract a single timestep from the dataset.

        Parameters:
            idx (int): The time index to extract.

        Returns:
            xr.Dataset: A new Dataset containing the data at the specified time index.
        """
        return self.ds.isel(time=idx)

    def load(self) -> 'GOESMultiCloudObservation':
        """
        Load the dataset into memory.

        This method uses Dask's compute() method to load the dataset into memory.
        It is typically used before performing data analysis or visualization.

        Returns:
            GOESMultiCloudObservation: The loaded GOESMultiCloudObservation object.
        """
        # Load the dataset into memory
        self.ds = self.ds.compute()
        return self

    ############################################################################################
    # METHODS: METADATA EXPORT
    ############################################################################################

    def to_metadata_df(self) -> pd.DataFrame:
        """
        Convert the GOES Multi-Cloud Observation object to a Pandas DataFrame.

        This method first calls to_metadata_records() to convert the observation object to a list of dictionaries, where each dictionary represents a single record in the observation.

        Then, it constructs a Pandas DataFrame from the list of dictionaries.

        This method is useful for data analysis and visualization, as Pandas DataFrames provide many useful features for data manipulation and analysis.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the records of the observation.
        """
        records = self.to_metadata_records()
        return pd.DataFrame(records)

    def to_metadata_records(self) -> list:
        """
        Convert the GOES Multi-Cloud Observation object to a list of dictionaries, where each dictionary represents a single record in the observation.

        The resulting list of dictionaries can be used to construct a Pandas DataFrame.

        This method iterates over the time coordinate and extracts the corresponding values from the promoted attributes (i.e., the attributes that are specified in the PROMOTED_ATTRS dictionary).

        The method first computes all promoted attributes by calling .compute() on the corresponding variables. This is done to avoid redundant computation.

        Then, the method iterates over the computed NumPy arrays and constructs a dictionary for each record. The dictionary contains the attribute name as the key and the corresponding value as the value.

        Finally, the method returns the list of dictionaries.
        """
        records = []
        n_times = len(self.time)

        # Compute all promoted attrs
        computed_attrs = {}
        for target_var in self.PROMOTED_ATTRS.values():
            if target_var in self.ds:
                # Compute entire variable once
                computed_attrs[target_var] = self.ds[target_var].compute().values

        # Now iterate over computed NumPy arrays
        for i in range(n_times):
            record = {}

            # Iterate over computed NumPy arrays and construct a dictionary for each record
            for target_var, values in computed_attrs.items():
                value = values[i]
                # Convert to Python native types
                if isinstance(value, (np.integer, np.floating)):
                    value = value.item()
                elif isinstance(value, np.ndarray):
                    value = value.item() if value.size == 1 else str(value)
                record[target_var] = value

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
        Check CF attributes and structure.

        This method checks if all required CF attributes are present and if the time coordinate is monotonically increasing.

        Required CF attributes:
            - conventions
            - time
            - y
            - x

        Optional CF attributes:
            - goes_imager_projection

        If any required CF attributes are missing, the method returns an 'issues' list containing the missing attributes.
        If the time coordinate is not monotonically increasing, the method returns an 'issues' list containing a string describing the problem.

        If any optional CF attributes are missing, the method returns a 'warnings' list containing the missing attributes.
        """
        issues = []
        warnings = []

        # Check if all required CF attributes are present
        if 'conventions' not in self.ds:
            issues.append("Missing 'conventions' variable")
        required_coords = ['time', 'y', 'x']
        for coord in required_coords:
            if coord not in self.ds.coords:
                issues.append(f"Missing required coordinate: {coord}")

        # Check if the time coordinate is monotonically increasing
        if 'time' in self.ds.coords:
            time_vals = self.ds.coords['time'].compute().values  # Explicitly compute the time coordinate
            if not np.all(np.diff(time_vals.astype('int64')) > 0):
                issues.append("Time coordinate is not monotonically increasing")

        # Check if any optional CF attributes are present
        if 'goes_imager_projection' not in self.ds:
            warnings.append("Missing 'goes_imager_projection' variable")

        return {
            'compliant': len(issues) == 0,
            'issues': issues,
            'warnings': warnings
        }

    def validate_consistency(self) -> dict:
        """
        Validate multi-file consistency.

        This method checks whether the values of four variables are consistent across all files of a multi-file observation.

        The variables checked are:
            - scene_id
            - spatial_resolution
            - orbital_slot
            - time

        The method returns a dictionary with two keys: 'consistent' and 'issues'.
        'consistent' is a boolean indicating whether the observation is consistent or not.
        'issues' is a list of strings describing the inconsistencies found.

        :return: A dictionary with consistency information
        :rtype: dict
        """
        issues = []

        if not self.is_multi_file:
            # If this is not a multi-file observation, return immediately
            return {'consistent': True, 'issues': []}

        # Explicitly compute the variables we need to check for consistency
        scene_ids = self.scene_id.compute().values
        resolutions = self.spatial_resolution.compute().values
        slots = self.orbital_slot.compute().values
        time_vals = self.time.compute().values.astype('int64')

        # Check if all scene_id values are the same
        if len(np.unique(scene_ids)) > 1:
            # If there are multiple unique scene_id values, add an issue
            issues.append(f"Inconsistent scene_id: {np.unique(scene_ids)}")

        # Check if all spatial_resolution values are the same
        if len(np.unique(resolutions)) > 1:
            # If there are multiple unique spatial_resolution values, add an issue
            issues.append(f"Inconsistent spatial_resolution: {np.unique(resolutions)}")

        # Check if all orbital_slot values are the same
        if len(np.unique(slots)) > 1:
            # If there are multiple unique orbital_slot values, add an issue
            issues.append(f"Inconsistent orbital_slot: {np.unique(slots)}")

        # Check if all time values are monotonically increasing
        if not np.all(np.diff(time_vals) > 0):
            # If the time values are not monotonically increasing, add an issue
            issues.append("Time values are not monotonically increasing")

        # Return the consistency information
        return {
            'consistent': len(issues) == 0,
            'issues': issues
        }

    def validate_temporal_continuity(self, previous_last: np.datetime64) -> bool:
        """
        Validate that the temporal coverage of this observation is continuous with the previous observation.

        This method checks whether the first timestamp of the current observation is greater than the last timestamp of the previous observation.

        :param previous_last: The last timestamp of the previous observation.
        :return: True if the temporal coverage is continuous, False otherwise.
        """
        return self.first_timestamp > previous_last

    ############################################################################################
    # CONTEXT MANAGER
    ############################################################################################

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        This method is called when the execution passes to the line right after an object of this class is used in a with statement.

        :return: self
        :rtype: GOESMultiCloudObservation
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context related to this object.

        This method is called when the execution passes to the line right after an object of this class is used in a with statement.

        :param exc_type: The type of the exception that was thrown (if any).
        :param exc_val: The value of the exception that was thrown (if any).
        :param exc_tb: The traceback of the exception that was thrown (if any).
        """
        self.close()

    def close(self):
        """
        Close the opened dataset and release any system resources.

        This method is safe to call multiple times, and it will not raise any errors
        if the dataset is already closed.

        :raises ValueError: If the store type is invalid.
        """
        if hasattr(self, 'ds') and self.ds is not None:
            # Close the dataset to release system resources
            self.ds.close()
            # Release the dataset object
            self.ds = None

    ############################################################################################
    # DUNDER
    ############################################################################################

    def __repr__(self) -> str:
        """
        Return a string representation of the GOESMultiCloudObservation object.

        The string will include the platform ID, orbital slot, number of time steps,
        and the band number (if applicable).

        Returns:
            str: A string representation of the GOESMultiCloudObservation object.
        """
        platform = self.platform_id.compute().values[0] if len(self.platform_id) > 0 else 'unknown'
        slot = self.orbital_slot.compute().values[0] if len(self.orbital_slot) > 0 else 'unknown'
        n_times = len(self.time)
        band_str = f", band={self.band}" if self.band is not None else ""
        return f"GOESMultiCloudObservation(platform='{platform}', slot='{slot}', times={n_times}{band_str}"

    def __len__(self) -> int:
        """
        Return the number of time steps in the GOESMultiCloudObservation object.

        Returns:
            int: The number of time steps in the GOESMultiCloudObservation object.
        """
        return len(self.time)