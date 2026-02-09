# GOESMultiCloudObservation

## Overview

The `GOESMultiCloudObservation` class provides a sophisticated, CF-compliant interface for accessing and manipulating GOES ABI L2 CMI (Cloud and Moisture Imagery) data. It serves as the primary data access layer in the GOES Data Builder pipeline, handling everything from single file loading to multi-file time series analysis with comprehensive metadata management and provenance tracking.

### Design Philosophy

This class is built around these fundamental principles:

- **CF Compliance**: Strict adherence to Climate and Forecast conventions for interoperability
- **Multi-Scale Support**: Seamless handling from single observations to massive time series
- **Memory Efficiency**: Lazy loading and intelligent chunking for large datasets
- **Provenance First**: Complete metadata tracking and attribute promotion
- **Processing Ready**: Optimized for downstream regridding and storage operations

### Key Capabilities

The class bridges the gap between raw GOES NetCDF files and analysis-ready data by:

1. **Data Model Normalization**: Converting GOES-specific formats to standard CF conventions
2. **Temporal Harmonization**: Handling time concatenation across multiple files and platforms
3. **Metadata Enhancement**: Promoting global attributes to time-indexed variables for tracking
4. **Quality Integration**: Incorporating DQF (Data Quality Flags) into the data model
5. **Performance Optimization**: Implementing lazy loading and efficient memory management

## Architecture and Data Model

### CF-Compliant Data Structure

The class maps native GOES ABI L2 data to a standardized CF-compliant data model:

#### Domain Axes (Dimensions)
```
time (T): 1 per file, automatically concatenated for multi-file datasets
y (N): 5424 pixels (Full Disk at 2km resolution) - geostationary scanning angle
x (M): 5424 pixels (geostationary projection) - geostationary scanning angle
band: 16 ABI spectral bands (1-16)
```

#### Dimension Coordinates
```
time: datetime64[ns] - UTC observation times from 't' coordinate
y(y): float32 - Y scanning angle in radians (-0.088 to +0.088)
x(x): float32 - X scanning angle in radians (-0.088 to +0.088)
band: int16 - Band numbers (1-16)
```

#### Data Variables
```
CMI(band, time, y, x): Cloud and Moisture Imagery radiances/brightness temperatures
DQF(band, time, y, x): Data Quality Flags (0=good, 1=conditionally usable, etc.)
```

#### Time-Indexed Metadata Variables (Promoted from Global Attributes)

**Identity and Provenance:**
- `observation_id(time)`: UUID per product for unique identification
- `dataset_name(time)`: Original filename for traceability
- `naming_authority(time)`: gov.nesdis.noaa (standard GOES authority)

**Satellite and Instrument Information:**
- `platform_id(time)`: G16, G17, G18, G19 (GOES satellite identifiers)
- `instrument_type(time)`: GOES Imager (instrument category)
- `instrument_id(time)`: ABI (specific instrument identifier)
- `orbital_slot(time)`: GOES-East, GOES-West, GOES-Test, GOES-Storage

**Scene and Temporal Characteristics:**
- `scene_id(time)`: Full Disk, CONUS, Mesoscale (coverage area)
- `scan_mode(time)`: M3, M4, M6 (scan mode identifiers)
- `time_coverage_start(time)`: ISO format observation start time
- `time_coverage_end(time)`: ISO format observation end time

**Processing and Quality Information:**
- `processing_level(time)`: L2+ (GOES Level 2 Plus processing)
- `date_created(time)`: File creation timestamp
- `production_site(time)`: Processing site location
- `production_environment(time)`: Processing environment details

### File Handling

The class supports multiple input scenarios:

1. **Single File**: Standard GOES ABI L2 NetCDF file
2. **Multiple Files**: Time-concatenated files from same platform/time
3. **Mixed Sources**: Files from different platforms or times

## Class Structure

### Initialization

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation

# Load single file
obs = GOESMultiCloudObservation('/data/GOES18/2024/01/01/OR_ABI-L2-MCMIPF-M6_G18_s20240101100212_e20240101139512_c20240101139514.nc')

# Load multiple files (time-concatenated)
files = [
    '/data/GOES18/2024/01/01/file1.nc',
    '/data/GOES18/2024/01/01/file2.nc',
    '/data/GOES18/2024/01/01/file3.nc'
]
obs = GOESMultiCloudObservation(files)

# Load with configuration
obs = GOESMultiCloudObservation.from_config('./config.yaml')
```

### Constructor Parameters

```python
GOESMultiCloudObservation(
    file_path: Union[str, Path, list],
    engine: str = 'netcdf4',
    chunks: Optional[dict] = None,
    strict: bool = True
)
```

**Parameters:**
- `file_path`: Single file path, list of paths, or directory pattern
- `engine`: xarray engine for reading NetCDF files
- `chunks`: Chunking strategy for memory efficiency
- `strict`: Whether to enforce strict validation

### Class Methods

```python
from_config(config_path: str) -> 'GOESMultiCloudObservation'
from_directory(dir_path: str, pattern: str = '*.nc') -> 'GOESMultiCloudObservation'
concatenate_observations(observations: list) -> 'GOESMultiCloudObservation'
```

## Core Methods

### Data Access

#### CMI Data Access

```python
# Get CMI data for specific band
cmi_band1 = obs.get_cmi(1)  # Returns xarray DataArray
cmi_band7 = obs.get_cmi(7)  # Brightness temperature

# Get all CMI bands
all_cmi = obs.get_all_cmi()  # Returns dict {band: DataArray}

# Get CMI data with time selection
cmi_subset = obs.get_cmi(1, time_slice=slice('2024-01-01T12:00', '2024-01-01T13:00'))

# Get CMI data with spatial selection
cmi_region = obs.get_cmi(1, y_slice=slice(1000, 2000), x_slice=slice(1000, 2000))
```

#### DQF Data Access

```python
# Get DQF data for specific band
dqf_band1 = obs.get_dqf(1)  # Returns xarray DataArray

# Get all DQF bands
all_dqf = obs.get_all_dqf()  # Returns dict {band: DataArray}

# Get DQF statistics
dqf_stats = obs.get_dqf_statistics(1)
print(f"Good pixels: {dqf_stats['good']}")
print(f"Conditionally usable: {dqf_stats['conditionally_usable']}")
```

#### Coordinate Access

```python
# Get coordinate arrays
time_coords = obs.time  # Time coordinate
y_coords = obs.y        # Y coordinate (radians)
x_coords = obs.x        # X coordinate (radians)

# Get projection information
projection = obs.projection  # Projection parameters dict
```

### Metadata Access

#### Global Metadata

```python
# Get global attributes
global_attrs = obs.global_attributes
print(f"Platform: {global_attrs['platform_ID']}")
print(f"Instrument: {global_attrs['instrument_type']}")

# Get time-indexed metadata
observation_ids = obs.observation_id  # Array of observation IDs
platform_ids = obs.platform_id        # Array of platform IDs
scan_modes = obs.scan_mode           # Array of scan modes
```

#### Band Metadata

```python
# Get metadata for specific band
band1_meta = obs.get_band_metadata(1)
print(f"Wavelength: {band1_meta['band_wavelength']}")
print(f"Description: {band1_meta['long_name']}")

# Get all band metadata
all_band_meta = obs.get_all_band_metadata()
```

### Data Manipulation

#### Time Operations

```python
# Select specific time
obs_single = obs.isel_time(0)  # First timestep
obs_range = obs.sel_time('2024-01-01T12:00')  # Specific time

# Time slice
obs_subset = obs.time_slice('2024-01-01T12:00', '2024-01-01T13:00')

# Get time range
time_range = obs.get_time_range()
print(f"Start: {time_range['start']}")
print(f"End: {time_range['end']}")
```

#### Spatial Operations

```python
# Get spatial extent
extent = obs.get_spatial_extent()
print(f"Y range: {extent['y_min']} to {extent['y_max']}")
print(f"X range: {extent['x_min']} to {extent['x_max']}")

# Spatial subsetting
obs_region = obs.spatial_subset(y_min=1000, y_max=2000, x_min=1000, x_max=2000)

# Convert to lat/lon (requires external projection)
lat_lon = obs.to_latlon()  # Returns lat/lon coordinates
```

### Data Export

#### Export to Different Formats

```python
# Export to NetCDF
obs.to_netcdf('/path/to/output.nc')

# Export specific bands
obs.export_bands([1, 2, 3], '/path/to/bands.nc')

# Export metadata to CSV
obs.export_metadata('/path/to/metadata.csv')

# Export to pandas DataFrame
df = obs.to_dataframe()
```

## Configuration Schema

### Configuration File

```yaml
# GOESMultiCloudObservation configuration
data_access:
  file_pattern: "OR_ABI-L2-MCMIP*.nc"
  engine: netcdf4
  strict: true
  
  # Chunking strategy
  chunks:
    time: 1
    y: 512
    x: 512
  
  # Validation options
  validate_bands: true
  validate_times: true
  check_continuity: true

# Processing options
processing:
  promote_globals: true
  time_indexing: true
  coordinate_validation: true
  
  # Memory management
  memory_limit: "4GB"
  cache_size: "1GB"

# Output options
output:
  default_format: netcdf
  compression: true
  include_metadata: true
```

### Environment Variables

```bash
# Data directory
export GOES_DATA_PATH="/data/goes"

# Cache directory
export GOES_CACHE_PATH="/cache/goes"

# Memory limits
export GOES_MEMORY_LIMIT="8GB"
```

## Usage Examples

### Basic Usage

```python
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation

# Load single observation
obs = GOESMultiCloudObservation('/data/GOES18/2024/01/01/OR_ABI-L2-MCMIPF-M6_G18_s20240101100212_e20240101139512_c20240101139514.nc')

# Access data
cmi_band1 = obs.get_cmi(1)
dqf_band1 = obs.get_dqf(1)

# Get metadata
print(f"Platform: {obs.platform_id.values[0]}")
print(f"Time: {obs.time.values[0]}")
print(f"Shape: {cmi_band1.shape}")
```

### Multi-File Processing

```python
# Load multiple files
files = glob.glob('/data/GOES18/2024/01/01/*.nc')
obs = GOESMultiCloudObservation(files)

# Process all timesteps
for i in range(len(obs.time)):
    obs_single = obs.isel_time(i)
    
    # Get data for this timestep
    cmi_data = {band: obs_single.get_cmi(band) for band in [1, 2, 3]}
    dqf_data = {band: obs_single.get_dqf(band) for band in [1, 2, 3]}
    
    # Process data...
    print(f"Processing timestep {i}: {obs_single.time.values[0]}")
```

### Time Series Analysis

```python
# Load time series
obs = GOESMultiCloudObservation('/data/GOES18/2024/01/*.nc')

# Extract time series for specific location
y_idx = 2700  # Center of image
x_idx = 2700

time_series = obs.get_cmi(7)[:, y_idx, x_idx]  # Band 7 at center

# Plot time series
import matplotlib.pyplot as plt
plt.figure(figsize=(12, 6))
plt.plot(obs.time.values, time_series)
plt.title('Band 7 Brightness Temperature Time Series')
plt.xlabel('Time')
plt.ylabel('Temperature (K)')
plt.show()
```

### Regional Analysis

```python
# Load observation
obs = GOESMultiCloudObservation('/data/GOES18/2024/01/01/file.nc')

# Define region (e.g., CONUS)
y_min, y_max = 1500, 2500  # Approximate CONUS bounds
x_min, x_max = 2000, 3000

# Extract regional data
cmi_region = obs.get_cmi(7, y_slice=slice(y_min, y_max), x_slice=slice(x_min, x_max))

# Regional statistics
print(f"Mean temperature: {cmi_region.mean().values:.2f} K")
print(f"Min temperature: {cmi_region.min().values:.2f} K")
print(f"Max temperature: {cmi_region.max().values:.2f} K")
```

### Quality Control

```python
# Load observation
obs = GOESMultiCloudObservation('/data/GOES18/2024/01/01/file.nc')

# Get DQF statistics for band 7
dqf_stats = obs.get_dqf_statistics(7)

print("DQF Statistics for Band 7:")
print(f"  Good pixels: {dqf_stats['good']} ({dqf_stats['good_percent']:.1f}%)")
print(f"  Conditionally usable: {dqf_stats['conditionally_usable']} ({dqf_stats['conditionally_usable_percent']:.1f}%)")
print(f"  Out of range: {dqf_stats['out_of_range']} ({dqf_stats['out_of_range_percent']:.1f}%)")
print(f"  No value: {dqf_stats['no_value']} ({dqf_stats['no_value_percent']:.1f}%)")

# Create quality mask
good_mask = obs.get_dqf(7) == 0  # Good pixels only
cmi_good = obs.get_cmi(7).where(good_mask)

print(f"Good pixel temperature: {cmi_good.mean().values:.2f} K")
```

### Integration with Processing Pipeline

```python
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
from goesdatabuilder.store.datasets.goes import GOESZarrStore

# Load observation
obs = GOESMultiCloudObservation('/data/GOES18/2024/01/01/file.nc')

# Initialize regridder
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02
)

# Initialize store
store = GOESZarrStore('./configurations/store/goesmulticloudzarr.yaml')
store.initialize_store('./output/goes_data.zarr')
store.initialize_region('GOES-East', regridder.target_lat, regridder.target_lon)

# Process all timesteps
for i in range(len(obs.time)):
    # Get observation dict for store
    obs_dict = regridder.regrid_to_observation_dict(obs, time_idx=i)
    
    # Append to store
    time_idx = store.append_observation('GOES-East', obs_dict)
    print(f"Appended timestep {i} at store index {time_idx}")
```

## Performance Optimization

### Memory Management

```python
# Configure chunking for memory efficiency
obs = GOESMultiCloudObservation(
    files,
    chunks={'time': 1, 'y': 512, 'x': 512}
)

# Process in chunks
for i in range(0, len(obs.time), 10):  # Process 10 timesteps at a time
    obs_chunk = obs.isel_time(slice(i, i+10))
    
    # Process chunk
    process_chunk(obs_chunk)
    
    # Clear memory
    del obs_chunk
```

### Parallel Processing

```python
from concurrent.futures import ThreadPoolExecutor
import glob

def process_file(file_path):
    """Process single file"""
    obs = GOESMultiCloudObservation(file_path)
    return {
        'file': file_path,
        'time': obs.time.values[0],
        'platform': obs.platform_id.values[0],
        'shape': obs.get_cmi(1).shape
    }

# Process files in parallel
files = glob.glob('/data/GOES18/2024/01/*.nc')

with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(process_file, files))

for result in results:
    print(f"Processed: {result['file']}")
```

## Error Handling

### Common Issues

#### File Not Found

```python
try:
    obs = GOESMultiCloudObservation('/nonexistent/file.nc')
except FileNotFoundError as e:
    print(f"File not found: {e}")
```

#### Invalid Band Number

```python
try:
    cmi = obs.get_cmi(99)  # Invalid band
except ValueError as e:
    print(f"Invalid band: {e}")
```

#### Memory Issues

```python
# Handle memory errors
try:
    obs = GOESMultiCloudObservation(large_file_list)
except MemoryError:
    print("Memory error - try smaller chunks or fewer files")
    
    # Retry with smaller chunks
    obs = GOESMultiCloudObservation(
        large_file_list,
        chunks={'time': 1, 'y': 256, 'x': 256}
    )
```

### Validation

```python
# Validate observation
validation_results = obs.validate()

if not validation_results['valid']:
    print("Validation errors:")
    for error in validation_results['errors']:
        print(f"  - {error}")
else:
    print("Observation is valid")
```

## API Reference

### Constructor

```python
GOESMultiCloudObservation(
    file_path: Union[str, Path, list],
    engine: str = 'netcdf4',
    chunks: Optional[dict] = None,
    strict: bool = True
)
```

### Class Methods

```python
from_config(config_path: str) -> 'GOESMultiCloudObservation'
from_directory(dir_path: str, pattern: str = '*.nc') -> 'GOESMultiCloudObservation'
concatenate_observations(observations: list) -> 'GOESMultiCloudObservation'
```

### Data Access Methods

```python
get_cmi(band: int, time_slice=None, y_slice=None, x_slice=None) -> xr.DataArray
get_dqf(band: int, time_slice=None, y_slice=None, x_slice=None) -> xr.DataArray
get_all_cmi() -> dict[int, xr.DataArray]
get_all_dqf() -> dict[int, xr.DataArray]
```

### Time Operations

```python
isel_time(index: int) -> 'GOESMultiCloudObservation'
sel_time(time: Union[str, datetime]) -> 'GOESMultiCloudObservation'
time_slice(start: str, end: str) -> 'GOESMultiCloudObservation'
get_time_range() -> dict
```

### Spatial Operations

```python
spatial_subset(y_min: int, y_max: int, x_min: int, x_max: int) -> 'GOESMultiCloudObservation'
get_spatial_extent() -> dict
to_latlon() -> tuple[np.ndarray, np.ndarray]
```

### Metadata Methods

```python
get_band_metadata(band: int) -> dict
get_all_band_metadata() -> dict[int, dict]
get_dqf_statistics(band: int) -> dict
export_metadata(output_path: str) -> None
```

### Export Methods

```python
to_netcdf(output_path: str) -> None
export_bands(bands: list[int], output_path: str) -> None
to_dataframe() -> pd.DataFrame
```

### Properties

```python
time: xr.DataArray                    # Time coordinate
y: xr.DataArray                      # Y coordinate (radians)
x: xr.DataArray                      # X coordinate (radians)
projection: dict                      # Projection parameters
global_attributes: dict               # Global attributes
observation_id: xr.DataArray          # Observation IDs
platform_id: xr.DataArray             # Platform IDs
scan_mode: xr.DataArray                # Scan modes
```

## Best Practices

### Data Loading

1. **Use Appropriate Chunking**: Configure chunks based on available memory
2. **Lazy Loading**: Use xarray's lazy evaluation for large datasets
3. **Selective Loading**: Load only required bands and time ranges
4. **Memory Monitoring**: Monitor memory usage during processing

### Processing

1. **Time Series First**: Process time dimension first for efficiency
2. **Quality Control**: Always check DQF before using data
3. **Validation**: Validate data before processing
4. **Error Handling**: Implement robust error handling

### Integration

1. **Pipeline Integration**: Use with regridding and storage components
2. **Configuration**: Use configuration files for reproducibility
3. **Logging**: Enable logging for debugging and monitoring
4. **Testing**: Test with small datasets before full processing

This class provides a robust, CF-compliant interface for accessing and processing GOES ABI L2 data, making it easy to integrate into larger processing pipelines.
