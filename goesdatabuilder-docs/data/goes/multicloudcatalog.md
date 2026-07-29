# GOESMetadataCatalog

## Overview

The `GOESMetadataCatalog` class provides a lightweight scanning and cataloging solution for GOES ABI L2+ NetCDF files. It extracts only metadata (global attributes, band statistics, data quality metrics) without loading data arrays, making it suitable for processing large GOES archives.

### Key Features

- Sequential file scanning with tqdm progress tracking
- Validation pipeline for GOES naming conventions, orbital consistency, and file structure
- Metadata extraction from global attributes, per-band statistics, and data quality metrics
- CSV persistence with support for incremental append operations
- Query interface for filtering by time range, platform, and orbital slot
- Error tracking with timestamped validation error records
- Cumulative in-memory state (multiple `scan_files` calls accumulate results)

## Architecture

Files are processed through a validation and extraction pipeline:

```
File Discovery -> Pattern Validation -> NetCDF Open (metadata only) ->
  Global Attribute Extraction -> Orbital Consistency Check ->
  Band Statistics Extraction -> Data Quality Extraction -> Catalog Update
```

Failed files are recorded in the validation errors DataFrame with timestamps and error messages. Processing continues regardless of individual file failures.

## Data Model

The catalog maintains four internal DataFrames:

1. **observations** (`_observations`): File-level metadata from global attributes, mapped via `PROMOTED_ATTRS`. Includes `file_path` and `file_size_mb`.
2. **band_statistics** (`_band_statistics`): Per-band statistics for all 16 ABI channels (one row per band per file).
3. **data_quality** (`_data_quality`): GRB and L0 error percentages per file.
4. **validation_errors** (`_validation_errors`): Files that failed validation with error messages and timestamps.

All four are exposed as read-only copies via properties.

### Observations Schema

Columns correspond to the values in `multicloudconstants.PROMOTED_ATTRS` (mapping of ~30 GOES NetCDF global attributes to standardized names), plus:
- `file_path`: Absolute path to the source file
- `file_size_mb`: File size in megabytes
- `time`: Timestamp from the `t` coordinate in the NetCDF file

Time columns (`time_coverage_start`, `time_coverage_end`, `date_created`, `time`) are converted to `datetime64` after scanning and on CSV load (using ISO8601 format parsing).

### Band Statistics Schema

One row per band per observation (16 rows per file):
- `observation_id`: Links to the observation
- `band`: Band number (1-16)
- `has_cmi`: Boolean flag for CMI variable presence
- Bands 1-6: `min_reflectance`, `max_reflectance`, `mean_reflectance`, `std_dev_reflectance`
- Bands 7-16: `min_brightness_temp`, `max_brightness_temp`, `mean_brightness_temp`, `std_dev_brightness_temp`
- `outlier_count`: Outlier pixel count (all bands)

### Data Quality Schema

- `grb_errors_percent`: Percentage of uncorrectable GRB errors
- `l0_errors_percent`: Percentage of uncorrectable L0 errors

### Validation Rules

File validation checks:
- File exists and is a regular file
- Filename matches `multicloudconstants.GOES_FILENAME_PATTERN`

Orbital consistency (`_validate_orbital_consistency`) validates extracted metadata against:
- `multicloudconstants.VALID_ORBITAL_SLOTS`: GOES-East, GOES-West, GOES-Test, GOES-Storage
- `multicloudconstants.VALID_PLATFORMS`: G16, G17, G18, G19
- `multicloudconstants.VALID_SCENE_IDS`: Full Disk, CONUS, Mesoscale

Validation errors are collected in a pending list during scanning and flushed to the `_validation_errors` DataFrame at the end of `scan_files`.

## Usage Examples

### Basic Scanning

```python
from goesdatabuilder.data.goes.multicloudcatalog import GOESMetadataCatalog

catalog = GOESMetadataCatalog()

# Scan from directory (glob pattern, delegates to scan_files)
catalog.scan_directory('/data/GOES18/', pattern='**/*.nc')

# Or scan explicit file list
catalog.scan_files(file_list)

# Or scan a single file
metadata = catalog.scan_file('/path/to/file.nc')
# Returns dict with 'global_attrs', 'band_statistics', 'data_quality'
# or None if validation fails

# Save to default location (default specified by the catalog.output_dir configuration option)
catalog.to_csv()

# Save to another location
catalog.to_csv("/path/to/output/dir/")
```

### Cumulative Scanning

`scan_files` concatenates new results with existing internal DataFrames, so multiple calls accumulate:

```python
catalog = GOESMetadataCatalog()
catalog.scan_directory('/data/january/')
catalog.scan_directory('/data/february/')
# catalog.observations now contains both months
catalog.to_csv()
```

### Loading Existing Catalog

```python
catalog = GOESMetadataCatalog()
# Load from default location (default specified by the catalog.output_dir configuration option)
catalog.from_csv()

# Load from another another location
catalog.from_csv("/path/to/output/dir/")


print(len(catalog))  # number of valid observations
print(catalog)
# GOESMetadataCatalog(observations=500, errors=3, time_range=2024-01-01..2024-01-31)
```

### Querying

```python
# Access DataFrames (returns copies)
obs = catalog.observations
stats = catalog.band_statistics
dq = catalog.data_quality
errors = catalog.validation_errors

# Filter by time period
files = catalog.get_files_for_period(
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31),
    orbital_slot='GOES-East',  # optional
)

# Filter by platform
g18_files = catalog.get_files_for_platform('G18')

# All valid/invalid files
valid = catalog.get_valid_files()       # list[str]
invalid = catalog.get_invalid_files()   # DataFrame

# Summary statistics
summary = catalog.summary()
# {'total_scanned': ..., 'valid_files': ..., 'invalid_files': ...,
#  'platforms': {'G18': 500, ...}, 'orbital_slots': {...},
#  'scenes': {...}, 'time_range': (earliest, latest),
#  'total_size_gb': ...}
```

### Incremental CSV Updates

```python
catalog = GOESMetadataCatalog()

# Scan new files (accumulates with loaded data in memory)
catalog.scan_directory('/data/new_files/')

# Append only new records to existing CSVs (validates column schema)
catalog.append_to_csv()
```

`append_to_csv` checks that new data columns match existing CSV columns and raises `ValueError` on schema mismatch.

### Error Monitoring

```python
errors = catalog.validation_errors
if not errors.empty:
    print(f"Found {len(errors)} validation errors")
    print(errors[['file_path', 'error_message']])
```

## Output Files

```
catalog/
├── observations.csv          # File-level metadata (PROMOTED_ATTRS + file_path + file_size_mb)
├── band_statistics.csv       # Per-band statistics (16 rows per observation)
├── global_data_quality.csv   # GRB/L0 error percentages
└── validation_errors.csv     # Failed files with error messages and timestamps
```

Only non-empty DataFrames are written. `to_csv` overwrites existing files; `append_to_csv` appends with schema validation.

## Integration

```
Raw GOES Files -> GOESMetadataCatalog -> file list -> GOESMultiCloudObservation -> Regridder -> GOESZarrStore
```

The `GOESMultiCloudObservation` can load GOES files from a catalog saved to a directory with `to_csv`. 


## Performance

- Opens files with `xr.open_dataset(engine='netcdf4', chunks=None)` for metadata-only access
- No data arrays are loaded into memory
- Sequential processing with tqdm progress bar showing valid/invalid counts in real time
- Single `pd.concat` at the end of `scan_files` rather than per-file DataFrame append
- NumPy types automatically converted to Python native types during extraction
