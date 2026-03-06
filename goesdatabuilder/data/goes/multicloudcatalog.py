"""
GOES Metadata Catalog Module

This module provides the GOESMetadataCatalog class for comprehensive scanning and cataloging
of GOES ABI L2+ NetCDF files. It operates in a lightweight, memory-efficient manner by extracting
only metadata without loading full data arrays, making it suitable for processing large datasets.

The module implements a complete workflow for:
- File validation using GOES naming conventions and structural requirements
- Parallel and sequential file scanning with progress tracking
- Metadata extraction from global attributes, band statistics, and data quality metrics
- CSV-based persistence for incremental catalog building and loading
- Query interface for filtering by time ranges, platforms, and orbital slots
- Comprehensive error handling and validation reporting

Author: GOES Data Builder Team
Version: 1.0.1
"""

import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Union, Optional
from datetime import datetime
import logging
from tqdm import tqdm

from . import multicloudconstants

# Configure module logger for metadata catalog operations
logger = logging.getLogger(__name__)


class GOESMetadataCatalog:
    """
    Scans GOES files and builds metadata catalog.
    Lightweight -- opens files only for attrs, not full arrays.

    Usage:
        catalog = GOESMetadataCatalog(output_dir='./catalog')
        catalog.scan_files(glob.glob('/data/GOES18/**/*.nc'))
        catalog.to_csv()

        # Later, query the catalog
        df = catalog.load_observations()
        g18_files = df[df['platform_id'] == 'G18']
    """

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(self, output_dir: Union[str, Path]):
        """
        Initialize the GOES metadata catalog.
        
        Args:
            output_dir: Directory path where CSV catalog files will be stored
            
        Creates:
            - Empty DataFrames for observations, band statistics, data quality, and validation errors
            - Output directory if it doesn't exist
            - Internal error tracking list
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize empty dataframes
        self._observations = pd.DataFrame()
        self._band_statistics = pd.DataFrame()
        self._data_quality = pd.DataFrame()
        self._validation_errors = pd.DataFrame()

        self._pending_errors = []

        logger.info(f"Initialized GOESMetadataCatalog at {self.output_dir}")

    ############################################################################################
    # SCANNING
    ############################################################################################

    def scan_file(self, file_path: Union[str, Path]) -> Optional[dict]:
        """
        Scan a single GOES file and extract metadata.
        
        Args:
            file_path: Path to the GOES NetCDF file to scan
            
        Returns:
            Dictionary containing extracted metadata with keys:
            - 'global_attrs': Observation-level metadata
            - 'band_statistics': List of band-specific statistics
            - 'data_quality': Data quality metrics
            Returns None if validation fails or file cannot be processed
            
        Process:
            1. Validate file naming pattern and accessibility
            2. Open file with xarray (metadata only, no data arrays)
            3. Extract global attributes and validate orbital consistency
            4. Extract band statistics for all 16 ABI bands
            5. Extract data quality metrics
            6. Close file immediately to conserve memory
            7. Log any validation errors
        """
        if not isinstance(file_path, Path):
            file_path = Path(file_path)


        # Validate file first
        is_valid, error_msg = self._validate_file(file_path)
        if not is_valid:
            self._log_validation_error(file_path, error_msg)
            return None

        try:
            # Open without loading data arrays
            with xr.open_dataset(file_path, engine='netcdf4', chunks=None) as ds:
                # Extract metadata
                global_attrs = self._extract_global_attrs(ds)

                # Validate orbital consistency
                is_valid, error_msg = self._validate_orbital_consistency(global_attrs)
                if not is_valid:
                    self._log_validation_error(file_path, error_msg)
                    return None

                # Add file path and size
                global_attrs['file_path'] = str(file_path.absolute())
                global_attrs['file_size_mb'] = file_path.stat().st_size / (1024 ** 2)

                # Extract band statistics and data quality
                band_stats = self._extract_band_statistics(ds)
                data_quality = self._extract_data_quality(ds)

                # Combine all metadata
                metadata = {
                    'global_attrs': global_attrs,
                    'band_statistics': band_stats,
                    'data_quality': data_quality,
                }

                return metadata

        except Exception as e:
            error_msg = f"Failed to extract metadata: {str(e)}"
            self._log_validation_error(file_path, error_msg)
            return None

    def scan_files(self, file_paths: list) -> 'GOESMetadataCatalog':
        """
        Scan multiple GOES files and build the catalog.
        
        Args:
            file_paths: List of file paths to scan
            
        Returns:
            Self (for method chaining)
            
        Process:
            - Processes files sequentially with progress bar
            - Extracts metadata from valid files
            - Populates internal DataFrames with observations, band stats, and data quality
            - Tracks and logs validation errors
            - Converts time columns to datetime objects
            
        Performance:
            - Uses single DataFrame concatenation at the end for efficiency
            - Shows real-time progress with valid/invalid counts
        """
        file_paths = [Path(f) for f in file_paths]
        logger.info(f"Scanning {len(file_paths)} files...")


        observations = []
        band_stats_list = []
        data_quality_list = []

        with tqdm(total=len(file_paths), desc="Scanning files") as pbar:
            for file_path in file_paths:
                try:
                    metadata = self.scan_file(file_path)

                    if metadata:
                        observations.append(metadata['global_attrs'])
                        band_stats_list.extend(metadata['band_statistics'])
                        data_quality_list.append(metadata['data_quality'])

                except Exception as e:
                    logger.warning(f"Unexpected error scanning {file_path}: {e}")

                pbar.update(1)
                pbar.set_postfix({
                    "valid": len(observations),
                    "errors": len(self._validation_errors) + len(self._pending_errors)
                })

        # Single concat at end for each dataframe
        if observations:
            new_obs_df = pd.DataFrame(observations)

            for col in ['time_coverage_start', 'time_coverage_end', 'date_created']:
                if col in new_obs_df.columns:
                    new_obs_df[col] = pd.to_datetime(new_obs_df[col])

            self._observations = pd.concat([self._observations, new_obs_df], ignore_index=True)

        if band_stats_list:
            new_stats_df = pd.DataFrame(band_stats_list)
            self._band_statistics = pd.concat([self._band_statistics, new_stats_df], ignore_index=True)

        if data_quality_list:
            new_dq_df = pd.DataFrame(data_quality_list)
            self._data_quality = pd.concat([self._data_quality, new_dq_df], ignore_index=True)

        if self._pending_errors:
            new_errors_df = pd.DataFrame(self._pending_errors)
            self._validation_errors = pd.concat([self._validation_errors, new_errors_df], ignore_index=True)
            self._pending_errors = []

        logger.info(f"Scan complete: {len(observations)} valid files, {len(self._validation_errors)} errors")

        return self

    def scan_directory(self, directory: Union[str, Path], pattern: str = '**/*.nc') -> 'GOESMetadataCatalog':
        """
        Scan all GOES files in a directory matching a pattern.
        
        Args:
            directory: Directory path to search for files
            pattern: Glob pattern for file matching (default: '**/*.nc')
            
        Returns:
            Self (for method chaining)
            
        Process:
            1. Validate directory exists
            2. Find all files matching the glob pattern
            3. Delegate to scan_files() for processing
            
        Raises:
            ValueError: If directory doesn't exist
        """
        directory = Path(directory)

        if not directory.exists():
            raise ValueError(f"Directory not found: {directory}")

        # Find files matching pattern
        file_paths = list(directory.glob(pattern))

        if not file_paths:
            logger.warning(f"No files found matching pattern '{pattern}' in {directory}")
            return self

        logger.info(f"Found {len(file_paths)} files in {directory}")

        # Scan the files
        return self.scan_files(file_paths)

    ############################################################################################
    # VALIDATION
    ############################################################################################

    def _validate_file(self, file_path: Path) -> tuple[bool, Optional[str]]:
        """
        Validate that a file meets GOES file requirements.
        
        Args:
            file_path: Path to the file to validate
            
        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if file passes all validation checks
            - error_message: Description of validation failure, None if valid
            
        Validation checks:
            - File exists and is accessible
            - Filename matches GOES ABI L2+ naming convention
            - File is a regular file (not directory)
        """
        # Check file exists
        if not file_path.exists():
            return False, "File not found"

        # Check naming pattern
        match = multicloudconstants.GOES_FILENAME_PATTERN.match(file_path.name)
        if not match:
            return False, f"Invalid GOES filename pattern: {file_path.name}"

        # Check readable
        if not file_path.is_file():
            return False, "Not a regular file"

        return True, None

    def _validate_orbital_consistency(self, metadata: dict) -> tuple[bool, Optional[str]]:
        """
        Validate orbital metadata consistency.
        
        Args:
            metadata: Dictionary of extracted global attributes
            
        Returns:
            Tuple of (is_valid, error_message)
            
        Validates:
            - orbital_slot is in VALID_ORBITAL_SLOTS
            - platform_id is in VALID_PLATFORMS  
            - scene_id is in VALID_SCENE_IDS
            
        Ensures metadata follows GOES conventions and expected value ranges.
        """
        # Check orbital_slot
        orbital_slot = metadata.get('orbital_slot')
        if orbital_slot and orbital_slot not in multicloudconstants.VALID_ORBITAL_SLOTS:
            return False, f"Invalid orbital_slot: {orbital_slot}"

        # Check platform_id
        platform_id = metadata.get('platform_id')
        if platform_id and platform_id not in multicloudconstants.VALID_PLATFORMS:
            return False, f"Invalid platform_id: {platform_id}"

        # Check scene_id
        scene_id = metadata.get('scene_id')
        if scene_id and scene_id not in multicloudconstants.VALID_SCENE_IDS:
            return False, f"Invalid scene_id: {scene_id}"

        return True, None

    def _log_validation_error(self, file_path: Path, error_msg: str):
        """
        Record a validation error for later processing.
        
        Args:
            file_path: Path to the file that failed validation
            error_msg: Description of the validation error
            
        Process:
            - Creates error entry with file path, message, and timestamp
            - Adds to pending errors list for batch processing
            - Errors will be flushed to validation_errors DataFrame after scanning
        """
        self._pending_errors.append({
            'file_path': str(file_path.absolute()),
            'error_message': error_msg,
            'timestamp': pd.Timestamp.now(),
        })

    ############################################################################################
    # EXTRACTION (PRIVATE)
    ############################################################################################

    def _extract_global_attrs(self, ds: xr.Dataset) -> dict:
        """
        Extract observation-level metadata from dataset attributes.
        
        Args:
            ds: Opened xarray Dataset
            
        Returns:
            Dictionary of extracted metadata with keys defined in PROMOTED_ATTRS
            
        Process:
            - Maps source attributes to target names using PROMOTED_ATTRS
            - Converts numpy types to Python native types
            - Extracts time coordinate if available
            - Handles datetime conversion with error handling
            
        Note:
            Only extracts attributes defined in the PROMOTED_ATTRS mapping
            to ensure consistent catalog schema.
        """
        metadata = {}

        for source_attr, target_name in multicloudconstants.PROMOTED_ATTRS.items():
            if source_attr in ds.attrs:
                value = ds.attrs[source_attr]
                # Convert numpy types to Python native
                if isinstance(value, (np.integer, np.floating)):
                    value = value.item()
                elif isinstance(value, np.ndarray):
                    value = value.item() if value.size == 1 else str(value)
                metadata[target_name] = value

        # Extract time coordinate if available
        if 't' in ds.coords:
            try:
                time_val = ds.coords['t'].values
                if isinstance(time_val, np.ndarray) and time_val.size == 1:
                    time_val = time_val.item()
                if isinstance(time_val, (np.datetime64, np.timedelta64)) and not np.isnat(time_val):
                    metadata['time'] = pd.Timestamp(time_val)
                else:
                    metadata['time'] = pd.Timestamp(time_val)  # Try pandas for other and if it fails, exception
            except Exception as e:
                logger.warning(f"Invalid 't' coordinate: {e}")
                metadata['time'] = None

        return metadata

    def _extract_band_statistics(self, ds: xr.Dataset) -> list[dict]:
        """
        Extract statistical information for all 16 ABI bands.
        
        Args:
            ds: Opened xarray Dataset
            
        Returns:
            List of 16 dictionaries, one per band (C01-C16)
            
        For each band, extracts:
            - Observation ID and band number
            - Reflectance statistics (bands 1-6): min, max, mean, std_dev
            - Brightness temperature statistics (bands 7-16): min, max, mean, std_dev
            - Outlier pixel count
            - Boolean flag indicating if CMI data exists for the band
            
        Note:
            Bands 1-6 are reflective solar channels
            Bands 7-16 are emissive infrared channels
        """
        band_stats = []
        observation_id = ds.attrs.get('id', 'unknown')

        for band in range(1, 17):
            band_str = f'C{band:02d}'

            stats = {
                'observation_id': observation_id,
                'band': band,
            }

            # Reflectance stats (bands 1-6)
            if band <= 6:
                for stat_type in ['min', 'max', 'mean', 'std_dev']:
                    var_name = f'{stat_type}_reflectance_factor_{band_str}'
                    if var_name in ds:
                        value = ds[var_name].values
                        stats[f'{stat_type}_reflectance'] = float(value) if np.isscalar(value) else float(value.item())

            # Brightness temperature stats (bands 7-16)
            else:
                for stat_type in ['min', 'max', 'mean', 'std_dev']:
                    var_name = f'{stat_type}_brightness_temperature_{band_str}'
                    if var_name in ds:
                        value = ds[var_name].values
                        stats[f'{stat_type}_brightness_temp'] = float(value) if np.isscalar(value) else float(
                            value.item())

            # Outlier count (all bands)
            outlier_var = f'outlier_pixel_count_{band_str}'
            if outlier_var in ds:
                value = ds[outlier_var].values
                stats['outlier_count'] = int(value) if np.isscalar(value) else int(value.item())

            # Check if CMI exists for this band
            cmi_var = f'CMI_{band_str}'
            stats['has_cmi'] = cmi_var in ds

            band_stats.append(stats)

        return band_stats

    def _extract_data_quality(self, ds: xr.Dataset) -> dict:
        """
        Extract data quality metrics from the dataset.
        
        Args:
            ds: Opened xarray Dataset
            
        Returns:
            Dictionary with quality metrics:
            - grb_errors_percent: Percentage of uncorrectable GRB errors
            - l0_errors_percent: Percentage of uncorrectable L0 errors
            
        Note:
            These metrics indicate data transmission and processing quality.
            Higher values suggest potential data quality issues.
        """
        quality = {}

        if 'percent_uncorrectable_GRB_errors' in ds:
            value = ds['percent_uncorrectable_GRB_errors'].values
            quality['grb_errors_percent'] = float(value) if np.isscalar(value) else float(value.item())

        if 'percent_uncorrectable_L0_errors' in ds:
            value = ds['percent_uncorrectable_L0_errors'].values
            quality['l0_errors_percent'] = float(value) if np.isscalar(value) else float(value.item())

        return quality

    ############################################################################################
    # PERSISTENCE
    ############################################################################################

    def to_csv(self):
        """
        Save catalog data to CSV files in the output directory.
        
        Creates four CSV files:
        - observations.csv: Observation-level metadata
        - band_statistics.csv: Per-band statistical data
        - global_data_quality.csv: Data quality metrics
        - validation_errors.csv: Files that failed validation
        
        Only writes files that contain data. Empty DataFrames are skipped.
        Logs the number of records written to each file.
        """
        if not self._observations.empty:
            obs_path = self.output_dir / 'observations.csv'
            self._observations.to_csv(obs_path, index=False)
            logger.info(f"Wrote {len(self._observations)} observations to {obs_path}")

        if not self._band_statistics.empty:
            stats_path = self.output_dir / 'band_statistics.csv'
            self._band_statistics.to_csv(stats_path, index=False)
            logger.info(f"Wrote {len(self._band_statistics)} band statistics to {stats_path}")

        if not self._data_quality.empty:
            data_quality_path = self.output_dir / 'global_data_quality.csv'
            self._data_quality.to_csv(data_quality_path, index=False)
            logger.info(f"Wrote {len(self._data_quality)} data quality to {data_quality_path}")

        if not self._validation_errors.empty:
            errors_path = self.output_dir / 'validation_errors.csv'
            self._validation_errors.to_csv(errors_path, index=False)
            logger.info(f"Wrote {len(self._validation_errors)} validation errors to {errors_path}")

    @property
    def from_csv(self) -> 'GOESMetadataCatalog':
        """
        Load catalog data from existing CSV files.
        
        Returns:
            Self (for method chaining)
            
        Process:
            - Reads all four CSV files if they exist
            - Converts time columns to datetime objects
            - Populates internal DataFrames with loaded data
            - Logs the number of records loaded from each file
            
        Note:
            Missing CSV files are silently skipped, allowing partial
            catalog loading. Time columns are automatically converted
            to ensure proper datetime handling.
        """
        obs_path = self.output_dir / 'observations.csv'
        if obs_path.exists():
            self._observations = pd.read_csv(obs_path)

            # Convert time columns
            for col in ['time_coverage_start', 'time_coverage_end', 'date_created', 'time']:
                if col in self._observations.columns:
                    self._observations[col] = pd.to_datetime(self._observations[col], format="ISO8601")

            logger.info(f"Loaded {len(self._observations)} observations from {obs_path}")

        stats_path = self.output_dir / 'band_statistics.csv'
        if stats_path.exists():
            self._band_statistics = pd.read_csv(stats_path)
            logger.info(f"Loaded {len(self._band_statistics)} band statistics from {stats_path}")

        data_quality_path = self.output_dir / 'global_data_quality.csv'
        if data_quality_path.exists():
            self._data_quality = pd.read_csv(data_quality_path)
            logger.info(f"Loaded {len(self._data_quality)} data quality from {data_quality_path}")

        errors_path = self.output_dir / 'validation_errors.csv'
        if errors_path.exists():
            self._validation_errors = pd.read_csv(errors_path)
            if 'timestamp' in self._validation_errors.columns:
                self._validation_errors['timestamp'] = pd.to_datetime(self._validation_errors['timestamp'])
            logger.info(f"Loaded {len(self._validation_errors)} validation errors from {errors_path}")

        return self

    def append_to_csv(self):
        """
        Append new records to existing CSV files for incremental updates.
        
        Process:
            - Validates column compatibility with existing CSVs
            - Reorders columns to match existing schema
            - Appends data without headers to existing files
            - Creates new files if they don't exist
            
        Raises:
            ValueError: If column schema mismatch between new and existing data
            
        Use case:
            Ideal for incremental catalog building when scanning
            new files without rewriting entire catalog.
        """

        def _append_df_to_csv(df: pd.DataFrame, csv_path: Path, df_name: str):
            """Helper to append a dataframe to existing CSV or create new one."""
            if df.empty:
                return

            if csv_path.exists():
                # Read existing columns (just header, no data)
                existing_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
                new_cols = df.columns.tolist()

                # Check column match
                if set(existing_cols) != set(new_cols):
                    raise ValueError(
                        f"Column mismatch in {df_name}. "
                        f"Existing: {existing_cols}, New: {new_cols}"
                    )

                # Reorder columns to match existing CSV and append
                df = df[existing_cols]
                df.to_csv(csv_path, mode='a', header=False, index=False)
                logger.info(f"Appended {len(df)} records to {csv_path}")
            else:
                # Create new file
                df.to_csv(csv_path, index=False)
                logger.info(f"Created {csv_path} with {len(df)} records")

        _append_df_to_csv(
            self._observations,
            self.output_dir / 'observations.csv',
            'observations'
        )

        _append_df_to_csv(
            self._band_statistics,
            self.output_dir / 'band_statistics.csv',
            'band_statistics'
        )

        _append_df_to_csv(
            self._data_quality,
            self.output_dir / 'global_data_quality.csv',
            'data_quality'
        )

        _append_df_to_csv(
            self._validation_errors,
            self.output_dir / 'validation_errors.csv',
            'validation_errors'
        )

    ############################################################################################
    # QUERY
    ############################################################################################

    @property
    def observations(self) -> pd.DataFrame:
        """
        Get a copy of the observations DataFrame.
        
        Returns:
            DataFrame containing observation-level metadata including:
            - Platform and orbital information
            - Time coverage and file details
            - Scene and observation identifiers
            
        Note:
            Returns a copy to prevent accidental modification of internal data.
        """
        return self._observations.copy()

    @property
    def band_statistics(self) -> pd.DataFrame:
        """
        Get a copy of the band statistics DataFrame.
        
        Returns:
            DataFrame containing per-band statistical data:
            - Min/max/mean/std for reflectance or brightness temperature
            - Outlier pixel counts
            - Band availability flags
            
        Note:
            Returns a copy to prevent accidental modification of internal data.
        """
        return self._band_statistics.copy()

    @property
    def validation_errors(self) -> pd.DataFrame:
        """
        Get a copy of the validation errors DataFrame.
        
        Returns:
            DataFrame containing files that failed validation:
            - File paths and error messages
            - Timestamps of validation failures
            
        Note:
            Returns a copy to prevent accidental modification of internal data.
        """
        return self._validation_errors.copy()

    @property
    def data_quality(self) -> pd.DataFrame:
        """
        Get a copy of the data quality DataFrame.
        
        Returns:
            DataFrame containing data quality metrics:
            - GRB error percentages
            - L0 error percentages
            
        Note:
            Returns a copy to prevent accidental modification of internal data.
        """
        return self._data_quality.copy()

    def get_files_for_period(
            self,
            start: datetime,
            end: datetime,
            orbital_slot: Optional[str] = None
    ) -> list[str]:
        """
        Get file paths for observations within a time period.
        
        Args:
            start: Start of time range (inclusive)
            end: End of time range (inclusive)
            orbital_slot: Optional filter for specific orbital slot
            
        Returns:
            List of file paths matching the criteria
            
        Filtering:
            - Uses time_coverage_start for time matching
            - Can optionally filter by orbital slot
            - Returns empty list if no matches found
            
        Use case:
            Ideal for finding files for specific time periods
            in data processing workflows.
        """
        if self._observations.empty:
            return []

        # Filter by time
        mask = (
                (self._observations['time_coverage_start'] >= start) &
                (self._observations['time_coverage_start'] <= end)
        )

        # Filter by orbital slot if specified
        if orbital_slot:
            mask &= (self._observations['orbital_slot'] == orbital_slot)

        filtered = self._observations[mask]
        return filtered['file_path'].tolist()

    def get_files_for_platform(self, platform_id: str) -> list[str]:
        """
        Get all file paths for a specific GOES platform.
        
        Args:
            platform_id: Platform identifier (e.g., 'G16', 'G17', 'G18')
            
        Returns:
            List of file paths for the specified platform
            
        Use case:
            Useful for platform-specific data processing
            or when working with data from a single satellite.
        """
        if self._observations.empty:
            return []

        filtered = self._observations[self._observations['platform_id'] == platform_id]
        return filtered['file_path'].tolist()

    def get_valid_files(self) -> list[str]:
        """
        Get file paths for all successfully validated files.
        
        Returns:
            List of file paths that passed validation
            
        Note:
            Returns all files in the observations catalog,
            which represents files that successfully passed
            all validation checks.
        """
        if self._observations.empty:
            return []

        return self._observations['file_path'].tolist()

    def get_invalid_files(self) -> pd.DataFrame:
        """
        Get validation error information for failed files.
        
        Returns:
            DataFrame with validation errors including:
            - File paths that failed validation
            - Error messages explaining failures
            - Timestamps of validation attempts
            
        Use case:
            Helpful for debugging file issues and
            identifying problematic data files.
        """
        return self._validation_errors.copy()

    ############################################################################################
    # SUMMARY
    ############################################################################################

    def summary(self) -> dict:
        """
        Get comprehensive statistics about the catalog.
        
        Returns:
            Dictionary containing:
            - total_scanned: Total files processed
            - valid_files: Files that passed validation
            - invalid_files: Files that failed validation
            - platforms: Dict of platform_id -> count
            - orbital_slots: Dict of orbital_slot -> count  
            - scenes: Dict of scene_id -> count
            - time_range: Tuple of (earliest_time, latest_time)
            - total_size_gb: Total size of all valid files in GB
            
        Use case:
            Ideal for catalog overview and data quality assessment.
        """
        summary = {
            'total_scanned': len(self._observations) + len(self._validation_errors),
            'valid_files': len(self._observations),
            'invalid_files': len(self._validation_errors),
        }

        if not self._observations.empty:
            # Platform breakdown
            platform_counts = self._observations['platform_id'].value_counts().to_dict()
            summary['platforms'] = platform_counts

            # Orbital slot breakdown
            slot_counts = self._observations['orbital_slot'].value_counts().to_dict()
            summary['orbital_slots'] = slot_counts

            # Scene breakdown
            scene_counts = self._observations['scene_id'].value_counts().to_dict()
            summary['scenes'] = scene_counts

            # Time range
            if 'time_coverage_start' in self._observations.columns:
                earliest = self._observations['time_coverage_start'].min()
                latest = self._observations['time_coverage_end'].max()
                summary['time_range'] = (earliest, latest)

            # Total size
            if 'file_size_mb' in self._observations.columns:
                total_mb = self._observations['file_size_mb'].sum()
                summary['total_size_gb'] = total_mb / 1024

        return summary

    def __repr__(self) -> str:
        """
        Return a concise string representation of the catalog.
        
        Returns:
            String in format: "GOESMetadataCatalog(observations=N, errors=M, time_range=YYYY-MM-DD..YYYY-MM-DD)"
            
        Includes:
            - Number of valid observations
            - Number of validation errors (if any)
            - Time range of observations (if available)
        """
        n_obs = len(self._observations)
        n_errors = len(self._validation_errors)

        parts = [f"observations={n_obs}"]

        if n_errors > 0:
            parts.append(f"errors={n_errors}")

        if not self._observations.empty and 'time_coverage_start' in self._observations.columns:
            start = self._observations['time_coverage_start'].min()
            end = self._observations['time_coverage_end'].max()
            parts.append(f"time_range={start.date()}..{end.date()}")

        return f"GOESMetadataCatalog({', '.join(parts)})"

    def __len__(self) -> int:
        """
        Return the number of valid observations in the catalog.
        
        Returns:
            Count of successfully processed files
            
        Use case:
            Allows len(catalog) syntax for quick catalog size checks.
        """
        return len(self._observations)
