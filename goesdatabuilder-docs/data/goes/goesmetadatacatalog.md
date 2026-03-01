# GOESMetadataCatalog

## Overview

The `GOESMetadataCatalog` class provides a lightweight, high-performance solution for scanning GOES ABI L2+ NetCDF files and building comprehensive metadata catalogs. It operates efficiently by extracting only metadata without loading full data arrays, making it suitable for processing large GOES data archives.

### Key Features

- **Sequential file scanning** with progress tracking via tqdm
- **Comprehensive validation** of GOES file structure and naming conventions
- **Metadata extraction** including observation-level attributes, band statistics, and data quality metrics
- **CSV-based persistence** for incremental catalog building and sharing
- **Error handling** with detailed validation error tracking

## Architecture

Files are processed through a pipeline of validation and extraction steps:
1. File discovery and basic validation
2. GOES filename pattern matching
3. NetCDF structure validation
4. Global attribute extraction
5. Band statistics computation
6. Data quality metrics extraction
7. Error logging and catalog updates

## Class Structure

### Initialization

```python
from goesdatabuilder.data.goes.multicloudcatalog import GOESMetadataCatalog

# Basic initialization
catalog = GOESMetadataCatalog(output_dir='./catalog')
```

**Parameters:**
- `output_dir` (str | Path): Directory for CSV outputs and catalog storage

### Core Methods

#### File Scanning

```python
# Scan individual file
metadata = catalog.scan_file('/path/to/file.nc')

# Scan multiple files (sequential processing)
catalog.scan_files(file_list)

# Scan directory with glob pattern
catalog.scan_directory('/data/GOES18/', pattern='**/*.nc')
```

#### Persistence

```python
# Save catalog to CSV files
catalog.to_csv()

# Load existing catalog from CSV
catalog.from_csv()

# Append new records to existing CSV files
catalog.append_to_csv()
```

## Data Model

### Internal DataFrames

The catalog maintains four primary DataFrames:

1. **observations**: File-level metadata (platform, time coverage, orbital info, file paths)
2. **band_statistics**: Per-band statistical data for all 16 ABI channels
3. **data_quality**: Global data quality metrics (GRB/L0 error percentages)
4. **validation_errors**: Files that failed validation with error details and timestamps

The catalog extracts NetCDF global attributes and maps them to standardized column names using the `PROMOTED_ATTRS` mapping from multicloudconstants.py.

**Key Mappings:**
- `id` → `observation_id`
- `platform_ID` → `platform_id` 
- `orbital_slot` → `orbital_slot`
- `timeline_id` → `scan_mode`
- `time_coverage_start/end` → `time_coverage_start/end`

### Validation Rules

**Valid Values** (from multicloudconstants.py):
- Platforms: G16, G17, G18, G19
- Orbital slots: GOES-East, GOES-West, GOES-Test, GOES-Storage
- Scene IDs: Full Disk, CONUS, Mesoscale

**Filename Pattern:**
```
OR_ABI-L2-MCMIP(?P<scene>[FCM])-M(?P<mode>\d)_G(?P<satellite>\d{2})_s(?P<start>\d{14})_e(?P<end>\d{14})_c(?P<created>\d{14})\.nc
```

Scene codes: F=Full Disk, C=CONUS, M=Mesoscale

### Band Statistics

For each of the 16 ABI bands, extracts:
- Min/max/mean/std deviation values
- Outlier pixel count
- CMI variable availability flag

**Band Types:**
- Bands 1-6: Reflectance factors (min/max/mean/std_dev_reflectance)
- Bands 7-16: Brightness temperatures (min/max/mean/std_dev_brightness_temp)

### Data Quality Metrics

- `grb_errors_percent`: Percentage of uncorrectable GRB errors
- `l0_errors_percent`: Percentage of uncorrectable L0 errors

## Usage Examples

### Basic Usage

```python
from goesdatabuilder.data.goes.multicloudcatalog import GOESMetadataCatalog
import glob

# Initialize catalog
catalog = GOESMetadataCatalog(output_dir='./catalog')

# Scan files
go_files = glob.glob('/data/GOES18/**/*.nc')
catalog.scan_files(go_files)

# Save and query
catalog.to_csv()
observations = catalog.observations
print(f"Processed {len(observations)} files")
```

### Query Operations

```python
# Get files for time period
files = catalog.get_files_for_period(
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31),
    orbital_slot='GOES-East'
)

# Filter by platform
g18_files = catalog.get_files_for_platform('G18')

# Get summary statistics
summary = catalog.summary()
print(f"Valid files: {summary['valid_files']}")
print(f"Platforms: {summary['platforms']}")
```

### Error Handling

```python
# Check validation errors
errors = catalog.validation_errors
if not errors.empty:
    print(f"Found {len(errors)} validation errors")
    print(errors['error_message'].value_counts())

# Get catalog length
print(f"Catalog contains {len(catalog)} valid observations")

# String representation
print(catalog)  # Shows observations, errors, and time range
```

### Incremental Updates

```python
# Load existing catalog
catalog = GOESMetadataCatalog(output_dir='./catalog')
catalog.from_csv()

# Scan new files
catalog.scan_directory('/data/new_files/', pattern='**/*.nc')

# Append to existing CSV files
catalog.append_to_csv()
```

## Performance

### Memory Efficiency
- Only metadata extraction, no data arrays loaded
- Uses xarray with `chunks=None` for metadata-only access
- Incremental DataFrame building with single concatenation at end
- CSV persistence for large datasets

### Processing Model
- Sequential file processing (not parallel as previously documented)
- Progress tracking with tqdm showing valid/invalid counts
- Error resilience: failed files don't stop processing
- Type conversion from numpy to Python native types

### Error Resilience
- Failed files don't stop processing
- Detailed error logging with timestamps
- Validation error tracking with pending error list
- Comprehensive exception handling

## Output Files

### Generated CSV Files

1. **observations.csv**: Main metadata with file paths, sizes, and global attributes
2. **band_statistics.csv**: Per-band statistics for all observations
3. **global_data_quality.csv**: Data quality metrics (GRB/L0 error percentages)
4. **validation_errors.csv**: Processing errors with timestamps

### Directory Structure

```
catalog/
├── observations.csv           # Primary metadata catalog
├── band_statistics.csv        # Band-level statistics
├── global_data_quality.csv    # Data quality metrics
└── validation_errors.csv      # Error log
```

## Integration

The catalog fits into the GOES data processing workflow:

```
Raw Files → GOESMetadataCatalog → GOESMultiCloudObservation → Processing Pipeline
```

Key integration points:
- File discovery and validation
- Metadata provision for downstream processing
- Processing history and provenance tracking

## Best Practices

1. **Output Directory**: Use absolute paths for reproducibility
2. **Error Monitoring**: Regularly check validation_errors.csv
3. **Incremental Updates**: Use `from_csv()` and `append_to_csv()` for large datasets
4. **Storage**: Consider disk space for large metadata catalogs
5. **Type Handling**: Be aware of numpy to Python type conversions
6. **Time Columns**: Automatic datetime conversion for time-based columns

## API Reference

### Constructor
```python
GOESMetadataCatalog(output_dir: Union[str, Path])
```

### Primary Methods
```python
scan_file(file_path: Union[str, Path]) -> Optional[dict]
scan_files(file_paths: list) -> 'GOESMetadataCatalog'
scan_directory(directory: Union[str, Path], pattern: str = '**/*.nc') -> 'GOESMetadataCatalog'
to_csv() -> None
from_csv() -> 'GOESMetadataCatalog'
append_to_csv() -> None
```

### Query Methods
```python
get_files_for_period(start: datetime, end: datetime, orbital_slot: Optional[str] = None) -> list[str]
get_files_for_platform(platform_id: str) -> list[str]
get_valid_files() -> list[str]
get_invalid_files() -> pd.DataFrame
summary() -> dict
```

### Properties
```python
observations: pd.DataFrame      # Observation metadata (copy)
band_statistics: pd.DataFrame  # Band statistics (copy)
data_quality: pd.DataFrame      # Data quality metrics (copy)
validation_errors: pd.DataFrame # Processing errors (copy)
```

### Magic Methods
```python
__repr__() -> str              # String representation with observations, errors, time range
__len__() -> int               # Number of valid observations
```

## Dependencies

- **xarray**: NetCDF file handling with metadata-only access
- **pandas**: DataFrame operations and CSV persistence
- **numpy**: Data type conversion and validation
- **pathlib**: Cross-platform path handling
- **datetime**: Time-based filtering and operations
- **logging**: Structured logging and error reporting
- **tqdm**: Progress bars for long-running operations

## Version Information

- **Author**: GOES Data Builder Team
- **Version**: 1.0.1
- **License**: MIT
