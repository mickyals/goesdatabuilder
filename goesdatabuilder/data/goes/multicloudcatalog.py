"""
GOES Metadata Catalog Module

This module provides the GOESMetadataCatalog class for scanning GOES ABI L2+ files
and building comprehensive metadata catalogs. It operates in a lightweight manner,
extracting only metadata without loading full data arrays.

Key Features:
- Parallel file scanning with ThreadPoolExecutor
- Comprehensive validation of GOES file structure and naming conventions
- Extraction of observation-level metadata, band statistics, and data quality metrics
- CSV-based persistence for incremental catalog building
- Query interface for filtering by time, platform, and orbital slot

Author: GOES Data Builder Team
Version: 2.0.0
"""

import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Union, Optional
from datetime import datetime
import logging
from tqdm import tqdm

from multicloudconstants import PROMOTED_ATTRS, VALID_PLATFORMS, VALID_ORBITAL_SLOTS, VALID_SCENE_IDS, GOES_FILENAME_PATTERN

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
        Input: directory for CSV outputs
        Output: None
        Job: Initialize empty dataframes for observations, band_stats, errors
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
        Input: single file path
        Output: dict of extracted metadata, or None if failed
        Job:
            1. Open file with xr.open_dataset (no chunks, just metadata)
            2. Extract all global attrs
            3. Extract band statistics (scalar variables)
            4. Extract data quality metrics
            5. Close file immediately
            6. Return metadata dict
        On failure: log to validation_errors, return None
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
        Input: list of file paths
        Output: self (for chaining)
        Job: Scan all files sequentially, populate internal dataframes
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

    def scan_directory(self, directory: Union[str, Path], pattern: str = '**/*.nc', **kwargs) -> 'GOESMetadataCatalog':
        """
        Input: directory path, glob pattern
        Output: self
        Job: Find all matching files, scan them
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
        Input: file path
        Output: (is_valid, error_message)
        Job: Check file matches GOES naming pattern, is readable, has expected structure
        """
        # Check file exists
        if not file_path.exists():
            return False, "File not found"

        # Check naming pattern
        match = GOES_FILENAME_PATTERN.match(file_path.name)
        if not match:
            return False, f"Invalid GOES filename pattern: {file_path.name}"

        # Check readable
        if not file_path.is_file():
            return False, "Not a regular file"

        return True, None

    def _validate_orbital_consistency(self, metadata: dict) -> tuple[bool, Optional[str]]:
        """
        Input: extracted metadata dict
        Output: (is_valid, error_message)
        Job: Check orbital_slot, scene_id, platform_id are valid values
        """
        # Check orbital_slot
        orbital_slot = metadata.get('orbital_slot')
        if orbital_slot and orbital_slot not in VALID_ORBITAL_SLOTS:
            return False, f"Invalid orbital_slot: {orbital_slot}"

        # Check platform_id
        platform_id = metadata.get('platform_id')
        if platform_id and platform_id not in VALID_PLATFORMS:
            return False, f"Invalid platform_id: {platform_id}"

        # Check scene_id
        scene_id = metadata.get('scene_id')
        if scene_id and scene_id not in VALID_SCENE_IDS:
            return False, f"Invalid scene_id: {scene_id}"

        return True, None

    def _log_validation_error(self, file_path: Path, error_msg: str):
        """Record validation error"""
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
        Input: opened dataset
        Output: dict of observation-level metadata
        Job: Pull all PROMOTED_ATTRS from ds.attrs
        """
        metadata = {}

        for source_attr, target_name in PROMOTED_ATTRS.items():
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
                    metadata['time'] = pd.Timestamp(time_val)  # Let pandas try
            except Exception as e:
                logger.warning(f"Invalid 't' coordinate: {e}")
                metadata['time'] = None

        return metadata

    def _extract_band_statistics(self, ds: xr.Dataset) -> list[dict]:
        """
        Input: opened dataset
        Output: list of 16 dicts (one per band)
        Job: Extract min/max/mean/std/outlier_count for each band
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
        Input: opened dataset
        Output: dict with grb_errors_percent, l0_errors_percent
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
        Output: None
        Job: Write observations.csv, band_statistics.csv, validation_errors.csv
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

    def from_csv(self) -> 'GOESMetadataCatalog':
        """
        Output: self
        Job: Load existing CSVs into internal dataframes
        """
        obs_path = self.output_dir / 'observations.csv'
        if obs_path.exists():
            self._observations = pd.read_csv(obs_path)

            # Convert time columns
            for col in ['time_coverage_start', 'time_coverage_end', 'date_created', 'time']:
                if col in self._observations.columns:
                    self._observations[col] = pd.to_datetime(self._observations[col])

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
        Output: None
        Job: Append new records to existing CSVs (for incremental updates)
        Raises: ValueError if column mismatch between existing CSV and new data
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
        """The observations dataframe"""
        return self._observations.copy()

    @property
    def band_statistics(self) -> pd.DataFrame:
        """The band statistics dataframe"""
        return self._band_statistics.copy()

    @property
    def validation_errors(self) -> pd.DataFrame:
        """The validation errors dataframe"""
        return self._validation_errors.copy()

    @property
    def data_quality(self) -> pd.DataFrame:
        return self._data_quality.copy()

    def get_files_for_period(
            self,
            start: datetime,
            end: datetime,
            orbital_slot: Optional[str] = None
    ) -> list[str]:
        """
        Input: time range, optional orbital slot filter
        Output: list of file paths
        Job: Query observations df, return matching file_paths
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
        """Filter by satellite"""
        if self._observations.empty:
            return []

        filtered = self._observations[self._observations['platform_id'] == platform_id]
        return filtered['file_path'].tolist()

    def get_valid_files(self) -> list[str]:
        """Return only files that passed validation"""
        if self._observations.empty:
            return []

        return self._observations['file_path'].tolist()

    def get_invalid_files(self) -> pd.DataFrame:
        """Return validation_errors df"""
        return self._validation_errors.copy()

    ############################################################################################
    # SUMMARY
    ############################################################################################

    def summary(self) -> dict:
        """
        Output: dict with catalog statistics
            total_files, valid_files, invalid_files,
            platforms: {G16: count, G18: count, ...},
            orbital_slots: {GOES-East: count, GOES-West: count},
            time_range: (earliest, latest),
            total_observations_gb
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
        """GOESMetadataCatalog(observations=1000, valid=985, time_range=...)"""
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
        """Number of observations"""
        return len(self._observations)
