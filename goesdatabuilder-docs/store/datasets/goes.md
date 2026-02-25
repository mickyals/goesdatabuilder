# GOESZarrStore

## Overview

The `GOESZarrStore` class provides a CF-compliant Zarr store implementation for GOES ABI L2+ satellite imagery. It extends `ZarrStoreBuilder` with domain-specific functionality for storing regridded GOES data with comprehensive CF metadata and extended DQF handling.

### Key Features

- **CF compliance** with CF-1.13 and ACDD-1.3 standards
- **Multi-platform support** for GOES-East, GOES-West, GOES-Test, GOES-Storage
- **Band-specific metadata** for all 16 GOES ABI bands
- **Extended DQF flags** including interpolated values
- **Batch processing** for efficient data insertion
- **Provenance tracking** for processing history

## Data Organization

### Store Structure

```
GOES Dataset Zarr Store
├── Global Attributes (CF/ACDD compliance)
├── GOES-East/
│   ├── Coordinates (time, lat, lon)
│   ├── Auxiliary Variables (platform_id, scan_mode)
│   └── Data Arrays (CMI_C01-16, DQF_C01-16)
├── GOES-West/ (similar structure)
└── GOES-Test/ (similar structure)
```

### Data Specifications

**Coordinates:**
- `time`: CF-compliant timestamps with UTC timezone
- `lat`: latitude in degrees_north (must be monotonic)
- `lon`: longitude in degrees_east (must be monotonic, supports antimeridian crossing)

**Data Arrays:**
- `CMI_C##`: Cloud & Moisture Imagery (float32)
- `DQF_C##`: Data Quality Flags (uint8, extended flags 0-5)

## Class Structure

### Initialization

```python
from goesdatabuilder.store.datasets.goes import GOESZarrStore

# Create store
store = GOESZarrStore(config_path='./goes_config.yaml')

# Open existing store
store = GOESZarrStore.from_existing(
    store_path='./goes_data.zarr',
    config_path='./goes_config.yaml'
)
```

### Configuration

```yaml
goes:
  platforms: ['GOES-East', 'GOES-West', 'GOES-Test', 'GOES-Storage']
  bands: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  
  band_metadata:
    1:
      wavelength: 0.47
      long_name: "ABI Cloud and Moisture Imagery reflectance factor - Blue"
      standard_name: "toa_bidirectional_reflectance"
      units: "1"
      valid_range: [0.0, 1.0]
```

## Core Methods

### Store Initialization

```python
# Initialize store
store.initialize_store('./goes_data.zarr')

# Initialize regions
for region in store.REGIONS:
    store.initialize_region(region, lat_grid, lon_grid)
```

### Region Management

```python
# Initialize region with longitude validation
store.initialize_region(
    region='GOES-East',
    lat=lat_grid,
    lon=lon_grid,  # Automatically validates monotonicity including antimeridian crossing
    bands=[1, 2, 3, 7, 14],
    regridder=regridder_instance
)

# Check region exists
if store.region_exists('GOES-East'):
    print('Region exists')
```

### Data Insertion

```python
# Single observation
obs_data = {
    'timestamp': np.datetime64('2024-01-01T12:00:00'),
    'platform_id': 'G18',
    'scan_mode': 'Full Disk',
    'cmi_data': {1: cmi_band1, 2: cmi_band2},
    'dqf_data': {1: dqf_band1, 2: dqf_band2}
}

time_idx = store.append_observation('GOES-East', obs_data)

# Batch observations
start_idx, end_idx = store.append_batch('GOES-East', observations)
```

### Query Interface

```python
# Time queries
time_range = store.get_time_range('GOES-East')
count = store.get_observation_count('GOES-East')

# Spatial queries
extent = store.get_spatial_extent('GOES-East')

# Band queries
bands = store.get_bands('GOES-East')
products = store.get_products_for_band(7)
fire_bands = store.get_bands_for_product('Fire detection')

# Platform queries
platforms = store.get_platforms('GOES-East')
```

### Metadata Management

```python
from datetime import datetime, timezone

# Update coverage
store.update_temporal_coverage('GOES-East')

# Add processing history
store.add_processing_history('Processed 100 observations')

# Track source files
store.add_source_files('GOES-East', file_paths)

# Finalize dataset
store.finalize_dataset()
```

## Band Metadata

### Default Metadata

**Reflectance Bands (1-6):**
- Wavelength: 0.47-2.24 μm
- Units: dimensionless reflectance
- Standard name: `toa_bidirectional_reflectance`

**Brightness Temperature Bands (7-16):**
- Wavelength: 3.90-13.28 μm
- Units: Kelvin
- Standard name: `toa_brightness_temperature`

### Band Products

Common applications:
- Cloud detection and monitoring
- Fire/hotspot characterization
- Vegetation monitoring
- Atmospheric motion tracking
- Sea surface temperature

## Performance Considerations

### Chunking Strategy

Recommended chunks for GOES data:
```yaml
chunks:
  time: 1     # One timestep per chunk
  lat: 512    # Spatial chunks for compression
  lon: 512    # Spatial chunks for compression
```

### Compression

- **CMI data**: Use zstd with bitshuffle
- **DQF data**: Use higher compression ratio
- **Balance**: Compression vs access speed

## Error Handling

```python
# Region validation
try:
    store.initialize_region('GOES-East', lat_grid, lon_grid)
except ValueError as e:
    print(f'Region validation failed: {e}')

# Data validation
try:
    store.append_observation('GOES-East', obs_data)
except ValueError as e:
    print(f'Data validation failed: {e}')

# Invalid band
try:
    store.initialize_region('GOES-East', lat_grid, lon_grid, bands=[99])
except ValueError as e:
    print(f'Invalid band: {e}')
```

## Usage Examples

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
    regridder=regridder
)

# Append observation
obs_dict = regridder.regrid_to_observation_dict(obs)
time_idx = store.append_observation('GOES-East', obs_dict)
```

### Batch Processing

```python
# Process multiple observations
observations = []
for file_path in goes_files:
    obs = GOESMultiCloudObservation(file_path)
    obs_dict = regridder.regrid_to_observation_dict(obs)
    observations.append(obs_dict)

# Batch append
start_idx, end_idx = store.append_batch('GOES-East', observations)
```

## API Reference

### Constructor
```python
GOESZarrStore(config_path: str | Path)
```

### Store Management
```python
initialize_store(store_path: Union[str, Path], overwrite: bool = False) -> None
initialize_region(region: str, lat: np.ndarray, lon: np.ndarray, 
               bands: Optional[list] = None, include_dqf: bool = True,
               regridder: Optional[GeostationaryRegridder] = None) -> None
```

### Data Operations
```python
append_observation(region: str, observation: dict) -> int
append_batch(region: str, observations: list) -> tuple[int, int]
```

### Query Methods
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

### Metadata Methods
```python
update_temporal_coverage(region: str) -> None
add_processing_history(message: str) -> None
add_source_files(region: str, file_paths: list[str]) -> None
finalize_dataset() -> None
```

### Properties
```python
REGIONS: list[str]                    # Supported platforms
BANDS: list[int]                     # Configured bands
BAND_METADATA: dict[int, dict]        # Band metadata
```

### Constants
```python
REFLECTANCE_BANDS: list[int]    # Bands 1-6
BRIGHTNESS_TEMP_BANDS: list[int]  # Bands 7-16
CELL_METHODS: str                    # CF cell methods
```
