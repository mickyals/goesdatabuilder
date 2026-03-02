# GOESMultiCloudObservation

## Overview

The `GOESMultiCloudObservation` class provides a CF-compliant interface for accessing and processing GOES ABI L2+ CMI data with time-indexed structure. It handles both single-file and multi-file datasets, providing a unified interface for geostationary satellite data analysis.

## Key Features

- **CF-compliant data structure** with time-indexed variables following climate data conventions
- **Single and multi-file support** with automatic temporal concatenation along time dimension
- **Lazy evaluation** using xarray/Dask for memory-efficient operations on large datasets
- **Band selection** with wavelength-based operations and validation
- **Comprehensive metadata access** through promoted global attributes from NetCDF files
- **Flexible configuration system** supporting YAML/JSON files with validation
- **Built-in validation** for GOES naming conventions and orbital parameters
- **Export functionality** for metadata cataloging and downstream processing

## Architecture

The class promotes NetCDF global attributes to time-indexed variables, enabling proper concatenation across files while maintaining provenance tracking. It provides both lazy (for large-scale operations) and eager (for metadata) access patterns to optimize performance for different use cases.

### Data Processing Pipeline

```
Raw GOES Files → Validation → Preprocessing → Time-Indexing → CF-Compliant Dataset → Analysis/Export
```

## Dependencies

- **xarray**: Core data array handling and Dask integration
- **numpy**: Numerical operations and array handling  
- **pandas**: Time series operations and validation
- **pathlib**: Cross-platform path handling
- **yaml/json**: Configuration file parsing
- **datetime**: Time-based operations
- **copy**: Deep copying for configuration preservation
- **logging**: Structured logging throughout pipeline

## Class Structure

### Initialization

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation

# Single file
obs = GOESMultiCloudObservation('path/to/file.nc')

# Multiple files with configuration
obs = GOESMultiCloudObservation({
    'files': ['file1.nc', 'file2.nc'],
    'data_access': {
        'sampling_type': 'even', 
        'sample_size': 10
    }
})
```

**Parameters:**
- `config` (dict, str, Path): Configuration dictionary or path to YAML/JSON file

**Attributes:**
- `config` (dict): Validated configuration dictionary
- `ds` (xarray.Dataset): The currently open dataset
- `_current_band` (int): The currently selected band number

### Configuration Options

The configuration system supports both direct file lists and directory-based file discovery:

#### File List Configuration
```yaml
files:
  - 'path/to/file1.nc'
  - 'path/to/file2.nc'
  - 'path/to/file3.nc'

data_access:
  sampling_type: 'even'  # or 'random'
  sample_size: 5
```

#### Directory Configuration  
```yaml
file_dir: '/path/to/goes/files'
recursive: true
data_access:
  sampling_type: 'random'
  sample_size: 20
  seed: 42
```

#### Configuration Validation

The system validates:
- File existence and GOES filename pattern matching
- Orbital slot validity (GOES-East, GOES-West, GOES-Test, GOES-Storage)
- Platform ID validity (G16, G17, G18, G19)
- Scene ID validity (Full Disk, CONUS, Mesoscale)

Invalid configurations raise `ConfigError` with descriptive messages.

## Data Access

### Lazy vs Eager Operations

The class optimizes performance through lazy evaluation:

**Lazy Properties** (no computation triggered):
- `cmi`, `dqf`: Access to imagery data (time, y, x)
- All promoted attributes: Access to metadata variables (time)
- Coordinates (`time`, `y`, `x`): Lazy access
- Band coordinates (`band_wavelength_*`, `band_id_*`): Scalar access

**Eager Properties** (triggers computation):
- `time_range`: Computes min/max from time_coverage_start/end
- `first_timestamp`, `last_timestamp`: Accesses time coordinate
- `validate_*` methods: Explicit consistency checks

**Performance Guidance:**
For large-scale processing, use lazy properties and avoid calling eager properties in loops to maintain Dask's lazy evaluation benefits.

## Usage Examples

### Basic Data Access

```python
# Initialize observation
obs = GOESMultiCloudObservation('path/to/file.nc')

# Access data (lazy - doesn't trigger computation)
cmi_data = obs.cmi  # xarray DataArray
platform = obs.platform_id  # time-indexed variable

# Access metadata (eager - triggers computation)
time_range = obs.time_range  # computes min/max times
first_obs = obs.first_timestamp  # accesses time coordinate
```

### Band Operations

```python
# Select band 7 (infrared)
obs.band = 7
wavelength = obs.band_wavelength  # Get wavelength in micrometers
band_type = obs.band_type  # 'brightness_temperature'

# Select band 2 (reflective)  
obs.band = 2
wavelength = obs.band_wavelength  # Get wavelength in micrometers
band_type = obs.band_type  # 'reflectance'
```

### Multi-File Processing

```python
# Process multiple files with even sampling
obs = GOESMultiCloudObservation({
    'files': ['file1.nc', 'file2.nc', 'file3.nc'],
    'data_access': {
        'sampling_type': 'even',
        'sample_size': 10
    }
})

# Access concatenated data
all_cmi = obs.cmi  # Shape: (time, y, x, band)
all_platforms = obs.platform_id  # Shape: (time,)
```

### Time-Based Operations

```python
# Get time range
start_time, end_time = obs.time_range

# Get specific time slice
obs_time_slice = obs.ds.sel(time=slice('2024-01-01', '2024-01-31'))

# Convert to pandas for analysis
df = obs.time.to_pandas()
```

### Export and Validation

```python
# Export metadata for cataloging
metadata = {
    'platform_id': obs.platform_id.compute().values[0],
    'orbital_slot': obs.orbital_slot.compute().values[0],
    'time_range': obs.time_range,
    'band_count': len(obs.band_id.compute().values)
}

# Validate orbital consistency
is_valid = obs.validate_orbital_slot()
is_valid_platform = obs.validate_platform_id()
```

## Properties Reference

### Identity Properties
- `observation_id`: Unique observation identifier
- `dataset_name`: Original filename identifier  
- `naming_authority`: Source organization
- `platform_id`: Satellite platform (G16, G17, G18, G19)
- `instrument_type`: Instrument classification
- `instrument_id`: Serial number

### Band Selection Properties
- `band`: Currently selected band number (1-16)
- `band_type`: Band type ('reflectance' or 'brightness_temperature')
- `band_wavelength`: Wavelength in micrometers
- `band_id`: Band identifier coordinate

### Coordinate Properties
- `time`: Time coordinate (lazy access)
- `y`, `x`: Spatial coordinates (lazy access)

### Metadata Properties
- `time_coverage_start/end`: Observation time bounds
- `date_created`: File creation timestamp
- `production_site`: Processing location
- `scene_id`: Scene type (Full Disk, CONUS, Mesoscale)
- `scan_mode`: Scanning mode
- `spatial_resolution`: Grid resolution information

### Standards Properties
- `conventions`: CF/ACDD conventions used
- `metadata_conventions`: Metadata standards applied
- `standard_name_vocabulary`: Variable naming conventions

## Error Handling

The class uses structured exception handling:

```python
class ConfigError(Exception):
    """Configuration validation error"""
```

Common validation errors:
- File not found or inaccessible
- Invalid GOES filename pattern
- Missing required NetCDF attributes
- Invalid orbital slot, platform, or scene ID
- Configuration file format errors

## Performance Considerations

### Memory Efficiency
- Uses xarray with Dask for out-of-core processing
- Lazy evaluation preserves memory until actual computation needed
- Chunked loading for large datasets

### Scalability
- Automatic file concatenation along time dimension
- Sampling support for large file collections
- Parallel processing capabilities (when enabled)

### Best Practices

1. **Configuration Management**: Use YAML files for complex setups
2. **Band Selection**: Set band once, reuse for multiple operations
3. **Time Operations**: Use time slicing for efficient data access
4. **Memory Management**: Use lazy properties for large-scale analysis
5. **Error Handling**: Catch ConfigError exceptions for graceful degradation

## Integration

The class integrates with the GOES data processing pipeline:

```
GOES Files → GOESMultiCloudObservation → Regridder → GOESZarrStore
```

It provides raw geostationary-projected data suitable for:
- Climate data analysis workflows
- Time series processing
- Multi-file concatenation studies
- Satellite data validation and quality control

## Version Information

- **Author**: GOES Data Builder Team
- **Version**: 1.0.1
- **License**: MIT

## Related Modules

- `multicloudconstants.py`: Validation constants and filename patterns
- `goesmetadatacatalog.py`: Metadata cataloging and file scanning
- `goesdatabuilder.data.goes.multicloud`: Main processing class
