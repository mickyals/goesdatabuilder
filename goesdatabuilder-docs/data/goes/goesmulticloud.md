# GOESMultiCloudObservation

## Overview

The `GOESMultiCloudObservation` class provides a CF-compliant interface for GOES ABI L2 CMI data. It handles single files or multi-file time series with metadata promotion and provenance tracking. The class provides raw geostationary-projected data that requires regridding before storage.

### Key Features

- **CF-compliant data structure** with proper coordinate systems
- **Multi-file support** with automatic time concatenation
- **Metadata promotion** from global attributes to time-indexed variables
- **Lazy evaluation** using xarray/Dask for memory efficiency
- **Band selection** with metadata access for all 16 ABI bands

## Data Organization

### Dimensions

```
time: 1 per file (concatenated for multi-file)
y: 5424 pixels (geostationary scanning angle)
x: 5424 pixels (geostationary projection)
```

### Data Variables

```
CMI_C##: Cloud and Moisture Imagery (time, y, x)
DQF_C##: Data Quality Flags (time, y, x)
```

### Promoted Attributes

Global attributes promoted to time-indexed variables:
- `observation_id`, `platform_id`, `orbital_slot`
- `scene_id`, `scan_mode`, `time_coverage_start/end`
- Production metadata and standards information

## Class Structure

### Initialization

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation

# From configuration file
obs = GOESMultiCloudObservation('./config.yaml')

# From configuration dict
config = {
    'data_access': {
        'files': ['file1.nc', 'file2.nc'],
        'engine': 'netcdf4',
        'chunks': 'auto'
    }
}
obs = GOESMultiCloudObservation(config)
```

### Constructor

```python
GOESMultiCloudObservation(config: Union[dict, str, Path], strict: bool = None)
```

**Parameters:**
- `config`: Configuration dictionary or path to YAML/JSON file
- `strict`: Whether to raise error for invalid files (default from config)

### Configuration

```yaml
data_access:
  # File selection: either files list or directory
  files: ["file1.nc", "file2.nc"]  # Explicit file list
  file_dir: "$GOES_DATA/GOES18/2024"  # Directory with pattern matching
  recursive: true  # Search subdirectories
  
  # Chunking for Dask
  chunk_size:
    time: 1
    y: -1  # Full spatial extent for regridding
    x: -1  # Full spatial extent for regridding
  
  # Validation
  sample_size: 5
  sampling_type: 'even'  # or 'random'
  strict: true
  
  # xarray settings
  engine: netcdf4
  parallel: false

regridding:
  weights_dir: "${WEIGHTS_PATH}/GOES-East/"
  load_cached: true
  reference_band: 7
  decimals: 6
  
  target:
    resolution: 0.02
```

## Core Methods

### Data Access

```python
# Get CMI and DQF for specific band
cmi_band1 = obs.get_cmi(1)  # DataArray
dqf_band1 = obs.get_dqf(1)  # DataArray

# Get all bands
all_cmi = obs.get_all_cmi()  # dict[int, DataArray]
all_dqf = obs.get_all_dqf()  # dict[int, DataArray]

# Select timestep
timestep_ds = obs.isel_time(idx)  # Dataset

# Load into memory
obs.load()  # Computes all lazy arrays
```

### Band Selection

```python
# Set current band (property setter)
obs.band = 7

# Current band properties
current_band = obs.band  # Currently selected band
band_type = obs.band_type  # 'reflectance' or 'brightness_temperature'
wavelength = obs.band_wavelength
band_id = obs.band_id

# Access via current band (properties)
cmi_current = obs.cmi  # Uses current band
dqf_current = obs.dqf  # Uses current band
```

### Metadata Access

```python
# Time properties
time_range = obs.time_range  # (start, end)
first_time = obs.first_timestamp
last_time = obs.last_timestamp
time_bounds = obs.time_bounds  # Optional

# Satellite info (lazy DataArrays)
platform_ids = obs.platform_id
orbital_slots = obs.orbital_slot
scene_ids = obs.scene_id
scan_modes = obs.scan_mode

# Projection and position
projection = obs.projection  # Projection parameters dict
satellite_pos = obs.satellite_position  # Height, subpoint coords

# File info
is_multi = obs.is_multi_file
n_files = obs.file_count
skipped = obs.skipped_files

# Data quality
grb_errors = obs.grb_errors_percent  # Optional
l0_errors = obs.l0_errors_percent    # Optional
```

## Usage Examples

### Basic Usage

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation

# Load from config file
obs = GOESMultiCloudObservation('./config.yaml')

# Access data for specific band
obs.band = 7  # Select brightness temperature band
cmi_data = obs.cmi  # Lazy DataArray
dqf_data = obs.dqf  # Lazy DataArray

# Get metadata
print(f"Platform: {obs.platform_id.values[0]}")
print(f"Time: {obs.time.values[0]}")
print(f"Shape: {cmi_data.shape}")
```

### Multi-File Processing

```python
# Load multiple files from config
config = {
    'data_access': {
        'file_dir': '$GOES_DATA/GOES18/2024',
        'recursive': true
    }
}
obs = GOESMultiCloudObservation(config)

# Process each timestep
for i in range(len(obs.time)):
    # Access data for timestep i
    obs.band = 7
    cmi_timestep = obs.cmi.isel(time=i)
    
    print(f"Processing timestep {i}: {obs.time.values[i]}")
```

### Band Statistics

```python
# Select band and get statistics
obs.band = 7
stats = obs.cmi_statistics

if stats:
    print(f"Min: {stats['min']}")
    print(f"Max: {stats['max']}")
    print(f"Mean: {stats['mean']}")
```

### Context Manager

```python
# Use with statement for automatic cleanup
with GOESMultiCloudObservation('./config.yaml') as obs:
    obs.band = 7
    data = obs.cmi
    # Processing...
# Dataset automatically closed
```

## Performance Considerations

### Memory Efficiency

- **Lazy Evaluation**: Most properties return lazy xarray objects
- **Chunking**: Configure chunks based on available memory
- **Band Selection**: Use `obs.band = N` to work with one band at a time

### Multi-File Handling

- **Automatic Concatenation**: Files are concatenated along time dimension
- **File Validation**: GOES filename patterns validated during loading
- **Environment Variables**: Support for `$GOES_DATA` in paths
- **Sampling**: Configurable file sampling for validation

### Error Handling

- **ConfigError**: Raised for configuration validation failures
- **File Validation**: Invalid files are filtered out during initialization
- **Graceful Degradation**: Processing continues with valid files

## API Reference

### Constructor
```python
GOESMultiCloudObservation(config: Union[dict, str, Path], strict: bool = None)
```

### Data Access Methods
```python
get_cmi(band: int) -> xr.DataArray
get_dqf(band: int) -> xr.DataArray
get_all_cmi() -> dict[int, xr.DataArray]
get_all_dqf() -> dict[int, xr.DataArray]
isel_time(idx: int) -> xr.Dataset
load() -> 'GOESMultiCloudObservation'
close() -> None
```

### Properties

**Data Access (Lazy):**
```python
cmi: xr.DataArray              # Current band CMI
dqf: xr.DataArray              # Current band DQF
time: xr.DataArray             # Time coordinate
y: xr.DataArray               # Y coordinate
x: xr.DataArray               # X coordinate
```

**Metadata (Lazy):**
```python
observation_id: xr.DataArray
dataset_name: xr.DataArray
platform_id: xr.DataArray
orbital_slot: xr.DataArray
scene_id: xr.DataArray
scan_mode: xr.DataArray
spatial_resolution: xr.DataArray
time_coverage_start: xr.DataArray
time_coverage_end: xr.DataArray
date_created: xr.DataArray
production_site: xr.DataArray
production_environment: xr.DataArray
processing_level: xr.DataArray
conventions: xr.DataArray
metadata_conventions: xr.DataArray
standard_name_vocabulary: xr.DataArray
title: xr.DataArray
summary: xr.DataArray
institution: xr.DataArray
project: xr.DataArray
license: xr.DataArray
keywords: xr.DataArray
cdm_data_type: xr.DataArray
iso_series_metadata_id: xr.DataArray
```

**Band Selection:**
```python
band: Optional[int]           # Current band (getter/setter)
band_type: Optional[str]        # 'reflectance' or 'brightness_temperature'
band_wavelength: Optional[float]
band_id: Optional[int]
```

**Computed (Eager):**
```python
time_range: tuple             # (start, end)
first_timestamp: np.datetime64
last_timestamp: np.datetime64
projection: dict               # Projection parameters
satellite_position: dict       # Height, subpoint coords
cmi_statistics: Optional[dict] # Stats for current band
```

**Data Quality:**
```python
grb_errors_percent: Optional[xr.DataArray]
l0_errors_percent: Optional[xr.DataArray]
```

**File Info:**
```python
is_multi_file: bool           # Whether multiple files
file_count: int                # Number of files
skipped_files: list            # List of skipped files
```

### Validation Methods
```python
validate_cf_compliance() -> dict
validate_consistency() -> dict
validate_temporal_continuity(previous_last: np.datetime64) -> bool
```

### Export Methods
```python
to_metadata_df() -> pd.DataFrame
to_metadata_records() -> list
```

### Context Manager
```python
__enter__() -> 'GOESMultiCloudObservation'
__exit__(exc_type, exc_val, exc_tb) -> None
```

### Dunder Methods
```python
__repr__() -> str
__len__() -> int  # Number of timesteps
```
