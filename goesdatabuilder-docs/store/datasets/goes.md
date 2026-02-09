# GOESZarrStore

## Overview

The `GOESZarrStore` class provides a sophisticated, CF-compliant Zarr store implementation specifically engineered for GOES ABI L2+ satellite imagery data. It extends the base `ZarrStoreBuilder` class with domain-specific functionality for storing regridded GOES data with comprehensive CF metadata, ACDD-1.3 compliance, complete provenance tracking, and extended Data Quality Flag (DQF) handling. This class represents the culmination of the GOES Data Builder pipeline, transforming processed satellite data into analysis-ready, cloud-optimized datasets.

### Design Philosophy

The `GOESZarrStore` is built around these satellite data management principles:

- **CF Compliance Excellence**: Strict adherence to CF-1.13 conventions for climate and forecast data interoperability
- **ACDD-1.3 Compliance**: Full Attribute Convention for Data Discovery compliance for enhanced data discoverability
- **GOES Domain Expertise**: Tailored specifically for GOES ABI L2+ data with band-specific metadata and characteristics
- **Multi-Platform Architecture**: Seamless handling of GOES-East, GOES-West, and GOES-Test satellite platforms
- **Provenance First**: Complete tracking of data processing history, source files, and transformation steps
- **Quality Integration**: Advanced DQF flag handling including interpolated values for regridded data
- **Production Ready**: Enterprise-grade features for large-scale data processing and distribution

### Core Capabilities

The store provides these specialized GOES data management capabilities:

1. **CF-Compliant Storage**: All data structures, coordinates, and metadata follow CF-1.13 conventions
2. **Multi-Platform Organization**: Separate regions for different GOES satellites with consistent structure
3. **Band-Specific Metadata**: Detailed metadata for all 16 GOES ABI bands with scientific parameters
4. **Extended Quality Flags**: Support for interpolated DQF values (flag 5) in regridded data
5. **Batch Processing**: Efficient multi-observation append operations with location tracking
6. **Temporal Management**: Proper time coordinate handling with CF-compliant time encoding
7. **Spatial Coordinates**: Accurate latitude/longitude coordinate systems with proper bounds
8. **Processing Provenance**: Complete tracking of software versions, processing parameters, and data lineage

## Architecture and Data Organization

### Store Hierarchy Design

The `GOESZarrStore` implements a hierarchical organization optimized for GOES data access patterns:

```
GOES Dataset Zarr Store
├── Global Attributes (CF/ACDD Compliance)
│   ├── Conventions: "CF-1.13, ACDD-1.3"
│   ├── title, summary, institution
│   ├── processing history
│   └── platform information
│
├── GOES-East/ (Regional Group)
│   ├── Coordinates (CF Compliant)
│   │   ├── time (CF time coordinate)
│   │   ├── lat (latitude coordinate)
│   │   └── lon (longitude coordinate)
│   │
│   ├── Auxiliary Variables
│   │   ├── platform_id (GOES satellite identifier)
│   │   ├── scan_mode (scan mode information)
│   │   └── observation_id (unique observation identifiers)
│   │
│   └── Data Arrays (16 ABI Bands)
│       ├── CMI_C01 through CMI_C16 (Cloud & Moisture Imagery)
│       └── DQF_C01 through DQF_C16 (Data Quality Flags)
│
├── GOES-West/ (Similar Structure)
│   └── [Complete regional organization]
│
└── GOES-Test/ (Similar Structure)
    └── [Complete regional organization]
```

### Data Model Specifications

#### Coordinate Systems
**Time Coordinate:**
- **Standard Name**: `time`
- **Units**: `seconds since 1970-01-01T00:00:00`
- **Calendar**: `gregorian`
- **Encoding**: CF-compliant 64-bit integer timestamps
- **Bounds**: Optional time bounds for temporal intervals

**Spatial Coordinates:**
- **Latitude (`lat`)**:
  - **Standard Name**: `latitude`
  - **Units**: `degrees_north`
  - **Axis**: `Y`
  - **Bounds**: Regular grid from target regridding parameters
  
- **Longitude (`lon`)**:
  - **Standard Name**: `longitude` 
  - **Units**: `degrees_east`
  - **Axis**: `X`
  - **Bounds**: Regular grid from target regridding parameters

#### Data Array Specifications
**Cloud and Moisture Imagery (CMI):**
- **Naming**: `CMI_C##` where ## is band number (01-16)
- **Dimensions**: `(time, lat, lon)` - CF-compliant ordering
- **Data Type**: `float32` for efficient storage and computation
- **Compression**: Zstandard with bitshuffle for optimal performance
- **Chunking**: Time-optimized chunking `(1, lat_chunk, lon_chunk)`

**Data Quality Flags (DQF):**
- **Naming**: `DQF_C##` where ## is band number (01-16)
- **Dimensions**: `(time, lat, lon)` - matching CMI arrays
- **Data Type**: `uint8` for efficient storage
- **Flag Values**: 0-5 with extended interpolated flag support
- **Compression**: Lossless compression for quality preservation

## Class Structure

### Initialization

```python
from goesdatabuilder.store.datasets.goes import GOESZarrStore

# Create store with GOES-specific configuration
store = GOESZarrStore(config_path='./goes_config.yaml')

# Open existing GOES store
store = GOESZarrStore.from_existing(
    store_path='./goes_data.zarr',
    config_path='./goes_config.yaml'
)
```

**Configuration Requirements:**
```yaml
# GOES-specific configuration
goes:
  # Platform definitions
  platforms: ['GOES-East', 'GOES-West', 'GOES-Test']
  
  # Bands to process (1-16)
  bands: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  
  # Band metadata (optional, has defaults)
  band_metadata:
    1:
      wavelength: 0.47
      long_name: "ABI Cloud and Moisture Imagery reflectance factor - Blue"
      standard_name: "toa_bidirectional_reflectance"
      units: "1"
      valid_range: [0.0, 1.5]
      products: ["Cloud detection", "Aerosol detection"]
    2:
      wavelength: 0.64
      long_name: "ABI Cloud and Moisture Imagery reflectance factor - Red"
      # ... (continues for all 16 bands)
  
  # Processing metadata
  processing:
    software_name: "GOES Data Builder"
    software_version: "1.0.0"
    software_url: "https://github.com/your-org/goesdatabuilder"
    processing_level: "L2+"
    environment: "Production"
```

### Constructor Parameters

```python
GOESZarrStore(config_path: str | Path)
```

**Parameters:**
- `config_path`: Path to YAML configuration file

**Inherited from ZarrStoreBuilder:**
- Configuration validation and loading
- Store lifecycle management
- Group and array operations
- Metadata management

## Core Methods

### Store Initialization

```python
# Initialize store with GOES-specific global attributes
store.initialize_store('./goes_data.zarr')

# Initialize all regions with full metadata
for region in store.REGIONS:
    store.initialize_region(region)
```

### Region Management

#### Region Initialization

```python
# Initialize specific region with regridding provenance
store.initialize_region(
    region='GOES-East',
    lat=lat_grid,
    lon=lon_grid,
    bands=[1, 2, 3, 7, 14],
    include_dqf=True,
    regridder=regridder_instance
)
```

**Parameters:**
- `region` (str): Platform/region identifier
- `lat` (np.ndarray): 1D latitude array (degrees_north, monotonic)
- `lon` (np.ndarray): 1D longitude array (degrees_east, monotonic)
- `bands` (Optional[list]): Bands to create (default: all 16)
- `include_dqf` (bool): Whether to create DQF arrays (default: True)
- `regridder` (Optional[GeostationaryRegridder]): For provenance tracking

#### Region Validation

```python
# Validate region exists
if store.region_exists('GOES-East'):
    print("GOES-East region already initialized")

# Validate observation shapes
try:
    store._validate_observation_shapes('GOES-East', cmi_data, dqf_data)
except ValueError as e:
    print(f"Shape validation failed: {e}")
```

### Data Insertion

#### Single Observation

```python
# Append single observation
obs_data = {
    'timestamp': np.datetime64('2024-01-01T12:00:00'),
    'platform_id': 'G18',
    'scan_mode': 'Full Disk',
    'cmi_data': {
        1: cmi_band1_array,
        2: cmi_band2_array,
        # ... for all bands
    },
    'dqf_data': {
        1: dqf_band1_array,
        2: dqf_band2_array,
        # ... for all bands
    }
}

time_idx = store.append_observation('GOES-East', obs_data)
print(f"Appended observation at time index {time_idx}")
```

#### Batch Observations

```python
# Append multiple observations efficiently
observations = [
    {'timestamp': t1, 'platform_id': 'G18', 'cmi_data': {...}, 'dqf_data': {...}},
    {'timestamp': t2, 'platform_id': 'G18', 'cmi_data': {...}, 'dqf_data': {...}},
    # ... more observations
]

start_idx, end_idx = store.append_batch('GOES-East', observations)
print(f"Appended {len(observations)} observations at indices {start_idx}-{end_idx}")
```

### Query Interface

#### Time Range Queries

```python
# Get time range for region
time_range = store.get_time_range('GOES-East')
if time_range:
    print(f"Time range: {time_range[0]} to {time_range[1]}")
else:
    print("No observations found")

# Get observation count
count = store.get_observation_count('GOES-East')
print(f"Total observations: {count}")
```

#### Spatial Extent

```python
# Get spatial bounds
extent = store.get_spatial_extent('GOES-East')
print(f"Spatial extent: {extent}")
# Output: {'lat_min': -90.0, 'lat_max': 90.0, 'lon_min': -180.0, 'lon_max': 180.0}
```

#### Band and Platform Queries

```python
# Get available bands
bands = store.get_bands('GOES-East')
print(f"Available bands: {bands}")

# Get bands for specific product
fire_bands = store.get_bands_for_product('Fire/hotspot characterization')
print(f"Fire detection bands: {fire_bands}")

# Get products for specific band
products = store.get_products_for_band(7)
print(f"Band 7 products: {products}")

# Get all products
all_products = store.list_all_products()
print(f"All products: {all_products}")

# Get platforms
platforms = store.get_platforms('GOES-East')
print(f"Platforms: {platforms}")
```

### Metadata Management

#### Temporal Coverage Updates

```python
# Update temporal coverage after data insertion
store.update_temporal_coverage('GOES-East')

# Add processing history
store.add_processing_history('Added 100 new observations via batch processing')
```

#### Source File Tracking

```python
# Add source files for provenance
source_files = [
    '/data/goes18/2024/01/01/OR_ABI-L2-MCMIPF-M6_G18_s20240101100212_e20240101139512_c20240101139514.nc',
    '/data/goes18/2024/01/01/OR_ABI-L2-MCMIPF-M6_G18_s20240101100342_e20240101139542_c20240101139544.nc'
]

store.add_source_files('GOES-East', source_files)
```

#### Dataset Finalization

```python
# Finalize dataset with complete metadata
store.finalize_dataset()
```

## Configuration Schema

### Complete GOES Configuration

```yaml
# Store configuration (inherited from ZarrStoreBuilder)
store:
  path: "./goes_data.zarr"
  storage_type: "local"
  attributes:
    title: "GOES ABI L2+ Dataset"
    description: "Regridded GOES ABI imagery on regular lat/lon grid"
    conventions: "CF-1.13, ACDD-1.3"

# GOES-specific configuration
goes:
  # Platform definitions
  platforms: ['GOES-East', 'GOES-West', 'GOES-Test']
  
  # Bands to process
  bands: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  
  # Band metadata with defaults
  band_metadata:
    # Reflectance bands (1-6)
    1:
      wavelength: 0.47
      long_name: "ABI Cloud and Moisture Imagery reflectance factor - Blue"
      standard_name: "toa_bidirectional_reflectance"
      units: "1"
      valid_range: [0.0, 1.5]
      products: ["Cloud detection", "Aerosol detection"]
      
    2:
      wavelength: 0.64
      long_name: "ABI Cloud and Moisture Imagery reflectance factor - Red"
      standard_name: "toa_bidirectional_reflectance"
      units: "1"
      valid_range: [0.0, 1.5]
      products: ["Cloud detection", "Vegetation index"]
      
    # Brightness temperature bands (7-16)
    7:
      wavelength: 3.90
      long_name: "ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Shortwave Window"
      standard_name: "toa_brightness_temperature"
      units: "K"
      valid_range: [0.0, 400.0]
      products: ["Cloud top properties", "Fire detection", "Sea surface temperature"]
      
    8:
      wavelength: 6.19
      long_name: "ABI Cloud and Moisture Imagery brightness temperature at top of atmosphere - Upper-Level Water Vapor"
      standard_name: "toa_brightness_temperature"
      units: "K"
      valid_range: [0.0, 400.0]
      products: ["Atmospheric motion", "Precipitable water"]
  
  # Processing metadata
  processing:
    software_name: "GOES Data Builder"
    software_version: "1.0.0"
    software_url: "https://github.com/your-org/goesdatabuilder"
    processing_level: "L2+"
    environment: "Production"
    institution: "University of Toronto"
    
  # Regridding configuration
  regridding:
    dqf_interpolated_flag: 5
    dqf_no_input_flag: 6
    direct_hit_threshold: 0.999

# Zarr configuration (inherited)
zarr:
  zarr_format: 3
  compression:
    default:
      compressor:
        codec: blosc
        cname: zstd
        clevel: 5
        shuffle: bitshuffle
      chunks:
        time: 1
        lat: 512
        lon: 512
      fill_value: NaN
    secondary:
      compressor:
        codec: blosc
        cname: zstd
        clevel: 3
        shuffle: bitshuffle
      fill_value: 0
```

## Band Metadata

### Default Band Metadata

The class includes comprehensive default metadata for all 16 GOES ABI bands:

**Reflectance Bands (1-6):**
- Wavelength: 0.47-2.3 μm
- Units: dimensionless (reflectance factor)
- Products: Cloud detection, aerosol, vegetation

**Brightness Temperature Bands (7-16):**
- Wavelength: 3.9-13.3 μm
- Units: Kelvin
- Products: Cloud properties, fire detection, atmospheric motion

### Band Products

**Common Products:**
- Cloud detection and monitoring
- Fire/hotspot characterization
- Vegetation index
- Atmospheric motion
- Sea surface temperature
- Snow and ice detection
- Volcanic ash detection
- Air quality monitoring

## Performance Optimization

### Chunking Strategy

**Recommended Chunks for GOES Data:**
```yaml
chunks:
  time: 1        # One time step per chunk for temporal access
  lat: 512       # Spatial chunks for efficient compression
  lon: 512       # Spatial chunks for efficient compression
```

### Compression Strategy

**Data Arrays (CMI):**
- Use `default` preset with zstd compression
- Good balance of compression ratio and speed

**Quality Arrays (DQF):**
- Use `secondary` preset with higher compression
- Smaller data size, higher compression acceptable

## Error Handling

### Validation Errors

```python
# Region validation
try:
    store.initialize_region('GOES-East', lat_grid, lon_grid)
except ValueError as e:
    print(f"Region validation failed: {e}")

# Shape validation
try:
    store.append_observation('GOES-East', observation_data)
except ValueError as e:
    print(f"Shape validation failed: {e}")

# Band validation
try:
    store.append_observation('GOES-East', observation_data)
except KeyError as e:
    print(f"Band validation failed: {e}")
```

### Configuration Errors

```python
# Invalid region
try:
    store.initialize_region('INVALID_REGION', lat_grid, lon_grid)
except ValueError as e:
    print(f"Invalid region: {e}")

# Invalid band
try:
    store.initialize_region('GOES-East', lat_grid, lon_grid, bands=[99])
except ValueError as e:
    print(f"Invalid band: {e}")
```

## Integration Examples

### Basic Usage

```python
from goesdatabuilder.store.datasets.goes import GOESZarrStore
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder
import numpy as np

# Initialize store
store = GOESZarrStore('./goes_config.yaml')
store.initialize_store('./goes_data.zarr')

# Create regridder
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02
)

# Initialize region
lat_grid = np.linspace(-90, 90, 1800)
lon_grid = np.linspace(-180, 180, 3600)
store.initialize_region(
    region='GOES-East',
    lat=lat_grid,
    lon=lon_grid,
    bands=[1, 2, 3, 7, 14],
    regridder=regridder
)

# Process observation
observation = {
    'timestamp': np.datetime64('2024-01-01T12:00:00'),
    'platform_id': 'G18',
    'scan_mode': 'Full Disk',
    'cmi_data': regridder.regrid_to_observation_dict(obs),
    'dqf_data': regridder.regrid_to_observation_dict(obs, include_dqf=True)
}

# Append to store
time_idx = store.append_observation('GOES-East', observation)
print(f"Appended observation at time index {time_idx}")
```

### Batch Processing

```python
# Process multiple files
observations = []
for file_path in goes_files:
    obs = GOESMultiCloudObservation(file_path)
    regridder = GeostationaryRegridder.from_weights('./weights/GOES-East/')
    
    obs_dict = regridder.regrid_to_observation_dict(obs)
    observations.append(obs_dict)

# Batch append
start_idx, end_idx = store.append_batch('GOES-East', observations)
print(f"Appended {len(observations)} observations")
```

### Multi-Region Dataset

```python
# Initialize multiple regions
regions = ['GOES-East', 'GOES-West', 'GOES-Test']
for region in regions:
    store.initialize_region(region, lat_grid, lon_grid, bands=[1, 7, 14])

# Process data for each region
for region in regions:
    observation = process_observation(region)
    store.append_observation(region, observation)
```

## API Reference

### Constructor

```python
GOESZarrStore(config_path: str | Path)
```

### Store Initialization

```python
initialize_store(store_path: Union[str, Path], overwrite: bool = False) -> None
initialize_region(region: str, lat: np.ndarray, lon: np.ndarray, 
               bands: Optional[list] = None, include_dqf: bool = True,
               regridder: Optional[GeostationaryRegridder] = None) -> None
```

### Data Insertion

```python
append_observation(region: str, observation: dict) -> int
append_batch(region: str, observations: list) -> tuple[int, int]
```

### Query Interface

```python
get_time_range(region: str) -> Optional[tuple]
get_observation_count(region: str) -> int
get_spatial_extent(region: str) -> dict
get_bands(region: str) -> list
get_bands_for_product(product_name: str) -> list
get_products_for_band(band: int) -> list
list_all_products() -> list
get_platforms(region: str) -> list
```

### Metadata Management

```python
update_temporal_coverage(region: str) -> None
add_processing_history(message: str) -> None
add_source_files(region: str, file_paths: list[str]) -> None
finalize_dataset() -> None
```

### Band Metadata Helpers

```python
get_bands_for_product(product_name: str) -> list
get_products_for_band(band: int) -> list
list_all_products() -> list
```

### Properties

```python
REGIONS: list[str]                    # Supported platform regions
BANDS: list[int]                     # Configured band numbers
BAND_METADATA: dict[int, dict]        # Band metadata configuration
```

### Constants

```python
REFLECTANCE_BANDS: list[int]    # Bands 1-6
BRIGHTNESS_TEMP_BANDS: list[int]  # Bands 7-16
CELL_METHODS: str                    # CF cell methods string
```

## Best Practices

### Data Organization
- Use consistent band numbering (01-16) for array naming
- Include DQF arrays for data quality tracking
- Maintain provenance information for reproducibility
- Use appropriate chunking for access patterns

### Performance Optimization
- Use batch append for multiple observations
- Choose appropriate compression for data types
- Consider memory usage for large datasets

### Metadata Management
- Update temporal coverage after data insertion
- Track all source files for provenance
- Add processing history for audit trail
- Validate CF compliance before finalization
