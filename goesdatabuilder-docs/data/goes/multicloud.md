# GOESMultiCloudObservation

## Overview

The `GOESMultiCloudObservation` class provides a CF-compliant interface for accessing and processing GOES ABI L2+ CMI data with time-indexed structure. It handles both single-file and multi-file datasets, providing a unified interface for geostationary satellite data analysis.

### Key Features

- **CF-compliant data structure** with time-indexed variables following climate data conventions
- **Single and multi-file support** with automatic temporal concatenation along the time dimension
- **Lazy evaluation** using xarray/Dask for memory-efficient operations on large datasets
- **Band selection** with wavelength-based operations and validation
- **Comprehensive metadata access** through promoted global attributes from NetCDF files
- **Flexible configuration system** supporting YAML/JSON files or dictionaries
- **Built-in validation** for GOES naming conventions, orbital parameters, and multi-file consistency
- **Export functionality** for metadata cataloging via DataFrame or records

## Architecture

The class promotes NetCDF global attributes to time-indexed variables (defined in `multicloudconstants.PROMOTED_ATTRS`), enabling proper concatenation across files while maintaining per-observation provenance tracking.

### Data Processing Pipeline

```
Raw GOES Files -> Validation -> Preprocessing -> Time-Indexing -> CF-Compliant Dataset
```

Preprocessing steps per file:
1. Validate orbital slot against `VALID_ORBITAL_SLOTS`
2. Expand dataset with `time` dimension
3. Assign `time` coordinate from the `t` coordinate in the source file
4. Promote global attributes to time-indexed variables

### Pipeline Integration

```
GOESMultiCloudObservation (this) -> GeostationaryRegridder -> GOESZarrStore
```

This class provides raw geostationary-projected data. CMI/DQF arrays require regridding before storage.

## Class Structure

### Initialization

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation

# Single file
obs = GOESMultiCloudObservation('path/to/file.nc')

# Multiple files via config dict
obs = GOESMultiCloudObservation({
    'data_access': {
        'files': ['file1.nc', 'file2.nc'],
        'sampling_type': 'even',
        'sample_size': 10,
    }
})

# From YAML config file
obs = GOESMultiCloudObservation('config.yaml')
```

### Configuration

The configuration supports two modes for specifying input files:

#### File List Configuration

```yaml
data_access:
  files:
    - 'path/to/file1.nc'
    - 'path/to/file2.nc'
  engine: 'netcdf4'         # xarray engine (default: netcdf4)
  chunk_size: 'auto'        # Dask chunk size (default: auto)
  parallel: false            # Parallel file opening (default: false)
  sampling_type: 'even'     # Validation sampling: 'even' or 'random'
  sample_size: 5             # Number of files to validate (default: 5)
```

#### Directory Configuration

```yaml
data_access:
  file_dir: '/path/to/goes/files'
  recursive: true            # Search subdirectories (default: true)
  engine: 'netcdf4'
  chunk_size: 'auto'
  parallel: false
  sampling_type: 'random'
  sample_size: 20
  seed: 42                   # Random seed for reproducible sampling
```

#### Configuration Validation

The system validates:
- File existence and GOES filename pattern matching (`GOES_FILENAME_PATTERN`)
- Orbital slot validity against `VALID_ORBITAL_SLOTS`
- Platform ID validity against `VALID_PLATFORMS`
- Presence of `t` coordinate and `orbital_slot` attribute in sampled files
- Files are sorted by timestamp extracted from filenames

Invalid configurations raise `ConfigError`.

## Lazy vs Eager Operations

Most properties return `xr.DataArray` objects without triggering Dask computation.

**Lazy properties** (no computation triggered):
- `cmi`, `dqf`: Imagery data `(time, y, x)` per band
- All promoted attributes: Metadata variables indexed by `(time,)`
- Coordinates: `time`, `y`, `x`
- Band coordinates: `band_wavelength`, `band_id`

**Eager properties** (trigger computation):
- `time_range`: Computes min/max from `time_coverage_start`/`time_coverage_end`
- `first_timestamp`, `last_timestamp`: Accesses time coordinate values
- `satellite_position`: Accesses scalar variables
- `validate_*` methods: Explicitly compute to check consistency
- `__repr__`: Computes `platform_id` and `orbital_slot` for display

For large-scale processing, use lazy properties and avoid calling eager properties in loops.

## Usage Examples

### Basic Data Access

```python
obs = GOESMultiCloudObservation('path/to/file.nc')

# Lazy access
cmi_data = obs.cmi           # Requires band to be set first
platform = obs.platform_id   # time-indexed DataArray

# Eager access
time_range = obs.time_range
first_obs = obs.first_timestamp
```

### Band Operations

```python
# Select a band (required before accessing cmi/dqf)
obs.band = 7
cmi = obs.cmi                     # DataArray (time, y, x)
dqf = obs.dqf                     # DataArray (time, y, x)
wavelength = obs.band_wavelength   # float, e.g. 3.9
band_type = obs.band_type          # 'brightness_temperature'

obs.band = 2
band_type = obs.band_type          # 'reflectance'

# Direct band access without setting .band
cmi_band7 = obs.get_cmi(7)
dqf_band7 = obs.get_dqf(7)

# All bands at once
all_cmi = obs.get_all_cmi()   # {1: DataArray, 2: DataArray, ...}
all_dqf = obs.get_all_dqf()   # {1: DataArray, 2: DataArray, ...}
```

### Multi-File Processing

```python
obs = GOESMultiCloudObservation({
    'data_access': {
        'file_dir': '/data/goes',
        'sampling_type': 'even',
        'sample_size': 10,
    }
})

print(obs.is_multi_file)   # True
print(obs.file_count)      # Number of files

# Access concatenated data along time dimension
all_cmi = obs.get_cmi(7)   # Shape: (n_times, y, x)
```

### Time-Based Operations

```python
start_time, end_time = obs.time_range
first = obs.first_timestamp
last = obs.last_timestamp

# Extract single timestep
single_obs = obs.isel_time(0)   # xr.Dataset at time index 0

# Time slicing via xarray
subset = obs.ds.sel(time=slice('2024-01-01', '2024-01-31'))
```

### Metadata Export

```python
# Export to DataFrame for cataloging
df = obs.to_metadata_df()

# Or as list of dicts
records = obs.to_metadata_records()
```

### Validation

```python
# CF compliance check
cf_result = obs.validate_cf_compliance()
if not cf_result['compliant']:
    for issue in cf_result['issues']:
        print(f'Issue: {issue}')

# Multi-file consistency check
consistency = obs.validate_consistency()
if not consistency['consistent']:
    for issue in consistency['issues']:
        print(f'Inconsistency: {issue}')

# Temporal continuity with previous observation
is_continuous = obs.validate_temporal_continuity(previous_obs.last_timestamp)
```

### Projection and Satellite Position

```python
# Geostationary projection parameters (for regridder)
proj = obs.projection
# {'perspective_point_height': ..., 'semi_major_axis': ..., ...}

# Satellite position
pos = obs.satellite_position
# {'height': ..., 'subpoint_lon': ..., 'subpoint_lat': ...}
```

### Context Manager

```python
with GOESMultiCloudObservation('config.yaml') as obs:
    obs.band = 7
    data = obs.cmi.compute()
# Dataset automatically closed on exit
```

## Properties Reference

### Dataset Properties
- `is_multi_file -> bool`: Whether the dataset spans multiple files
- `file_count -> int`: Number of files in the dataset

### Identity Properties (time-indexed)
- `observation_id -> DataArray`: Unique observation identifier
- `dataset_name -> DataArray`: Original filename identifier
- `naming_authority -> DataArray`: Source organization
- `platform_id -> DataArray`: Satellite platform (G16, G17, G18, G19)
- `orbital_slot -> DataArray`: Orbital slot assignment
- `instrument_type -> DataArray`: Instrument classification
- `instrument_id -> DataArray`: Instrument serial number

### Band Selection
- `band -> Optional[int]`: Currently selected band (settable, 1-16)
- `band_type -> Optional[str]`: 'reflectance' (1-6) or 'brightness_temperature' (7-16)
- `band_wavelength -> Optional[float]`: Wavelength in micrometers
- `band_id -> Optional[int]`: Band identifier coordinate

### Dimension Coordinates
- `time -> DataArray`: Time coordinate
- `y -> DataArray`: Y spatial coordinate (geostationary projection)
- `x -> DataArray`: X spatial coordinate (geostationary projection)

### Scene/Mode Properties (time-indexed)
- `scene_id -> DataArray`: Scene type (Full Disk, CONUS, Mesoscale)
- `scan_mode -> DataArray`: Scanning mode
- `spatial_resolution -> DataArray`: Grid resolution string

### Temporal Properties
- `time_coverage_start -> DataArray`: Observation start time (time-indexed)
- `time_coverage_end -> DataArray`: Observation end time (time-indexed)
- `date_created -> DataArray`: File creation timestamp (time-indexed)
- `time_bounds -> Optional[DataArray]`: Time bounds if available
- `time_range -> tuple`: (earliest, latest) timestamps (eager)
- `first_timestamp -> datetime64`: First time value (eager)
- `last_timestamp -> datetime64`: Last time value (eager)

### Production Properties (time-indexed)
- `production_site -> DataArray`: Processing location
- `production_environment -> DataArray`: Processing environment
- `production_data_source -> DataArray`: Data source
- `processing_level -> DataArray`: Processing level

### Standards Properties (time-indexed)
- `conventions -> DataArray`: CF/ACDD conventions
- `metadata_conventions -> DataArray`: Metadata standards
- `standard_name_vocabulary -> DataArray`: Variable naming conventions

### Documentation Properties (time-indexed)
- `title -> DataArray`: Dataset title
- `summary -> DataArray`: Dataset summary
- `institution -> DataArray`: Responsible institution
- `project -> DataArray`: Project name
- `license -> DataArray`: Distribution license
- `keywords -> DataArray`: Dataset keywords
- `keywords_vocabulary -> DataArray`: Controlled vocabulary reference
- `cdm_data_type -> DataArray`: CDM data type
- `iso_series_metadata_id -> DataArray`: ISO metadata identifier

### Coordinate Reference
- `projection -> dict`: Geostationary projection parameters from `goes_imager_projection`
- `satellite_position -> dict`: Satellite height, subpoint lon/lat (eager)

### Field Constructs
- `cmi -> DataArray`: CMI for current band `(time, y, x)` (requires `.band` set)
- `dqf -> DataArray`: DQF for current band `(time, y, x)` (requires `.band` set)
- `cmi_statistics -> Optional[dict]`: Min/max/mean/std/outlier stats for current band

### Data Quality
- `grb_errors_percent -> Optional[DataArray]`: GRB error percentage
- `l0_errors_percent -> Optional[DataArray]`: L0 error percentage

## Methods Reference

### Data Access
```python
get_cmi(band: int) -> xr.DataArray              # CMI for specific band
get_dqf(band: int) -> xr.DataArray              # DQF for specific band
get_all_cmi() -> dict[int, xr.DataArray]         # All CMI bands present
get_all_dqf() -> dict[int, xr.DataArray]         # All DQF bands present
isel_time(idx: int) -> xr.Dataset                # Single timestep dataset
load() -> GOESMultiCloudObservation               # Load into memory (compute Dask)
```

### Metadata Export
```python
to_metadata_df() -> pd.DataFrame                  # Promoted attrs as DataFrame
to_metadata_records() -> list[dict]                # Promoted attrs as list of dicts
```

### Validation
```python
validate_cf_compliance() -> dict                   # {'compliant': bool, 'issues': [...], 'warnings': [...]}
validate_consistency() -> dict                     # {'consistent': bool, 'issues': [...]}
validate_temporal_continuity(previous_last: datetime64) -> bool
```

### Lifecycle
```python
close() -> None                                    # Close dataset, release resources
__enter__() -> GOESMultiCloudObservation
__exit__(...) -> None                              # Calls close()
```

## Error Handling

```python
from goesdatabuilder.data.goes.multicloud import ConfigError

# Configuration errors
try:
    obs = GOESMultiCloudObservation('bad_config.yaml')
except ConfigError as e:
    print(f'Config error: {e}')
# Raised for: missing files, invalid patterns, bad orbital slots,
# missing coordinates, unsupported config format

# Band access without selection
try:
    data = obs.cmi
except ValueError:
    print('Set obs.band first')

# Missing band variable
try:
    data = obs.get_cmi(99)
except ValueError:
    print('Band must be 1-16')
```

## Performance Considerations

### Memory Efficiency
- Uses xarray with Dask for out-of-core processing
- Lazy evaluation preserves memory until `.compute()` is called
- Configurable chunk sizes via `chunk_size` config parameter

### File Validation
- Only a sample of files are validated on init (configurable via `sample_size`)
- Even sampling provides temporal coverage; random sampling with seed for reproducibility

### Best Practices
1. Set `.band` once and reuse for multiple operations
2. Use `get_cmi(band)` / `get_dqf(band)` for direct access without changing state
3. Avoid calling eager properties (`time_range`, `first_timestamp`) in loops
4. Use `isel_time(idx)` for single-timestep extraction
5. Call `.close()` or use context manager when done

## Dependencies

- **xarray**: Core data array handling and Dask integration
- **numpy**: Numerical operations
- **pandas**: Time series operations and DataFrame export
- **pathlib**: Cross-platform path handling
- **yaml/json**: Configuration file parsing

## Related Modules

- `multicloudconstants.py`: `PROMOTED_ATTRS`, `VALID_PLATFORMS`, `VALID_ORBITAL_SLOTS`, `VALID_SCENE_IDS`, `GOES_FILENAME_PATTERN`, `DQF_FLAGS`, `DEFAULT_BAND_METADATA`, `REGIONS`, `REFLECTANCE_BANDS`
- `goesmetadatacatalog.py`: Metadata cataloging and file scanning
- `GeostationaryRegridder`: Regridding from geostationary to lat/lon grid
- `GOESZarrStore`: CF-compliant Zarr storage for regridded data