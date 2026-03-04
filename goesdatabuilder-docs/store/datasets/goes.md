# GOESZarrStore

## Overview

The `GOESZarrStore` class provides a CF-compliant Zarr store implementation for GOES ABI L2+ satellite imagery. It extends `ZarrStoreBuilder` with domain-specific functionality for storing regridded GOES data with comprehensive CF metadata and extended DQF handling.

### Key Features

- **CF compliance** with CF-1.13 and ACDD-1.3 standards
- **Multi-platform support** for GOES-East, GOES-West, GOES-Test, GOES-Storage
- **Band-specific metadata** for all 16 GOES ABI bands with configurable overrides
- **Extended DQF flags** (0-6) including interpolated and NaN-source indicators
- **Batch and single-observation insertion** with shape and band validation
- **Provenance tracking** for processing history and source files
- **Configurable array pipeline presets** for coordinate and data arrays

## Data Organization

### Store Structure

```
GOES Dataset Zarr Store
├── Global Attributes (CF-1.13, ACDD-1.3)
├── GOES-East/
│   ├── Region Attributes (geospatial bounds, regridding provenance)
│   ├── Coordinates
│   │   ├── lat(lat)          float64, dimension_names=['lat']
│   │   ├── lon(lon)          float64, dimension_names=['lon']
│   │   └── time(time)        datetime64[ns], dimension_names=['time'], extensible
│   ├── Auxiliary Coordinates
│   │   ├── platform_id(time) U3, dimension_names=['time'], extensible
│   │   └── scan_mode(time)   U10, dimension_names=['time'], extensible
│   └── Data Arrays
│       ├── CMI_C01(time, lat, lon)  float32, preset='default'
│       ├── DQF_C01(time, lat, lon)  uint8, preset='secondary'
│       ├── ...
│       ├── CMI_C16(time, lat, lon)
│       └── DQF_C16(time, lat, lon)
├── GOES-West/ (similar structure)
└── GOES-Test/ (similar structure)
```

### Data Specifications

**Dimension Coordinates:**
- `time`: Extensible, `datetime64[ns]`, `standard_name='time'`, `long_name='observation time'`
- `lat`: Static, `float64`, `standard_name='latitude'`, `units='degrees_north'`
- `lon`: Static, `float64`, `standard_name='longitude'`, `units='degrees_east'`

**Auxiliary Coordinates (aligned with time dimension):**
- `platform_id`: Satellite identifier (e.g., 'G16', 'G18', 'G19'), dtype `U3`
- `scan_mode`: ABI scan mode (e.g., '3', '4', '6'), dtype `U10`

**Data Arrays:**
- `CMI_C##`: Cloud and Moisture Imagery, `float32`, bands 1-16
- `DQF_C##`: Data Quality Flags, `uint8`, extended flags 0-6

**DQF Flag Values:**

| Flag | Name | Meaning |
|------|------|---------|
| 0 | GOOD | good_pixels_qf |
| 1 | CONDITIONALLY_USABLE | conditionally_usable_pixels_qf |
| 2 | OUT_OF_RANGE | out_of_range_pixels_qf |
| 3 | NO_VALUE | no_value_pixels_qf |
| 4 | FOCAL_PLANE_TEMP_EXCEEDED | focal_plane_temperature_threshold_exceeded_qf |
| 5 | INTERPOLATED | interpolated_qf (added for regridding pipeline) |
| 6 | NAN_SOURCE | nan_source (added for regridding pipeline) |

## Class Structure

### Initialization

```python
from goesdatabuilder.store.datasets.goesmulticloudzarr import GOESZarrStore

# Create new store
store = GOESZarrStore(config_path='./goes_config.yaml')
store.initialize_store('./goes_data.zarr')

# Open existing store
store = GOESZarrStore.from_existing(
    store_path='./goes_data.zarr',
    config_path='./goes_config.yaml',
    mode='r+'
)
```

### Configuration

```yaml
store:
  type: "local"
  path: "./goes_data.zarr"

zarr:
  zarr_format: 3
  default:
    compressor:
      codec: 'zarr.codecs:BloscCodec'
      kwargs:
        cname: zstd
        clevel: 5
        shuffle: bitshuffle
    serializer:
      codec: null
    filter:
      codec: null
    chunks: auto
    fill_value: null
  secondary:
    compressor:
      codec: 'zarr.codecs:BloscCodec'
      kwargs:
        cname: zstd
        clevel: 5
        shuffle: bitshuffle
    serializer:
      codec: null
    filter:
      codec: null
    chunks: auto
    fill_value: null

goes:
  bands: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

  band_metadata:  # Optional overrides, falls back to multicloudconstants defaults
    1:
      wavelength: 0.47
      long_name: "ABI Cloud and Moisture Imagery reflectance factor - Blue"
      standard_name: "toa_bidirectional_reflectance"
      units: "1"
      valid_range: [0.0, 1.0]

  global_metadata:  # Optional overrides for CF/ACDD global attrs
    title: "GOES ABI L2+ Cloud and Moisture Imagery"
    institution: "University of Toronto"

  processing:  # Optional software provenance
    software_name: "GOESDataBuilder"
    software_version: "0.1.0"
```

Regions are defined in `multicloudconstants.REGIONS`, not in config. Band metadata keys in YAML can be integers or strings (automatically converted to int).

## Core Methods

### Store Initialization

```python
# Initialize store with CF global attributes
store.initialize_store('./goes_data.zarr', overwrite=False)

# Initialize a region with coordinate arrays and all band arrays
store.initialize_region(
    region='GOES-East',
    lat=lat_grid,
    lon=lon_grid,
    bands=[1, 2, 3, 7, 14],      # Optional, defaults to all configured bands
    include_dqf=True,              # Whether to create DQF arrays
    regridder=regridder_instance,  # Optional, for provenance metadata
)
```

### Data Insertion

```python
# Single observation (individual parameters)
time_idx = store.append_observation(
    region='GOES-East',
    timestamp=np.datetime64('2024-10-10T20:45:00'),
    platform_id='G18',
    cmi_data={1: cmi_band1_2d, 2: cmi_band2_2d, ...},
    dqf_data={1: dqf_band1_2d, 2: dqf_band2_2d, ...},  # Optional
    scan_mode='6',  # Optional
)

# Batch observations (list of dicts, single resize per array)
observations = [
    {
        'timestamp': np.datetime64('2024-10-10T20:45:00'),
        'platform_id': 'G18',
        'scan_mode': '6',
        'cmi_data': {1: array_2d, 2: array_2d, ...},
        'dqf_data': {1: array_2d, 2: array_2d, ...},
    },
    ...
]
start_idx, end_idx = store.append_batch('GOES-East', observations)
```

`append_batch` validates that all observations share the same band set and spatial dimensions before writing. Coordinate array references are cached internally to minimize store access overhead.

### Query Interface

```python
# Time queries
time_range = store.get_time_range('GOES-East')     # (start, end) or None
count = store.get_observation_count('GOES-East')

# Spatial queries
extent = store.get_spatial_extent('GOES-East')
# {'lat_min': ..., 'lat_max': ..., 'lon_min': ..., 'lon_max': ...}

# Band queries
bands = store.get_bands('GOES-East')                        # [1, 2, ..., 16]
products = store.get_products_for_band(7)                    # ['Fire detection', ...]
fire_bands = store.get_bands_for_product('Fire detection')   # [7, ...]
all_products = store.list_all_products()

# Platform queries
platforms = store.get_platforms('GOES-East')  # ['G18', 'G19']
```

### Provenance and Metadata

```python
# Update temporal coverage from current data
store.update_temporal_coverage('GOES-East')

# Append timestamped processing history
store.add_processing_history('Processed 100 observations')

# Track source files (stored as newline-separated string)
store.add_source_files('GOES-East', [
    'OR_ABI-L2-MCMIPF-M6_G18_s20242842040203.nc',
    'OR_ABI-L2-MCMIPF-M6_G18_s20242842050203.nc',
])

# Finalize dataset (updates temporal coverage for all regions, adds history)
store.finalize_dataset()
```

`finalize_dataset` logs a warning for any configured region not found in the store.

## Band Metadata

### Default Metadata

Band metadata defaults are defined in `multicloudconstants.DEFAULT_BAND_METADATA` and can be overridden via config.

**Reflectance Bands (1-6):**
- Wavelength: 0.47-2.24 um
- Units: dimensionless reflectance (1)
- Standard name: `toa_bidirectional_reflectance`

**Brightness Temperature Bands (7-16):**
- Wavelength: 3.90-13.28 um
- Units: Kelvin (K)
- Standard name: `toa_brightness_temperature`

### Band Products

Common applications tracked in metadata:
- Cloud detection and monitoring
- Fire/hotspot characterization
- Vegetation monitoring
- Atmospheric motion tracking
- Sea surface temperature

## Array Pipeline Presets

Coordinate and data arrays use different pipeline presets from the zarr config:

- **`default`**: Used for CMI data arrays (primary scientific data)
- **`secondary`**: Used for DQF arrays and all coordinate/auxiliary arrays (lat, lon, time, platform_id, scan_mode)

Coordinate creation methods accept a `preset` parameter to override the default:

```python
# These are called internally by initialize_region, but the preset is configurable
store._create_lat_coord('GOES-East', lat, preset='coordinate')
store._create_time_coord('GOES-East', chunks=(1024,), preset='coordinate')
```

## Error Handling

```python
# Region validation
try:
    store.initialize_region('GOES-East', lat_grid, lon_grid)
except ValueError as e:
    print(f'Validation failed: {e}')  # Non-monotonic lat/lon, invalid region

# Shape mismatch
try:
    store.append_observation('GOES-East', timestamp, 'G18', cmi_data)
except ValueError as e:
    print(f'Shape mismatch: {e}')

# Heterogeneous bands in batch
try:
    store.append_batch('GOES-East', observations)
except ValueError as e:
    print(f'Band mismatch: {e}')  # Observations have different band sets

# Missing band arrays
try:
    store.append_observation('GOES-East', timestamp, 'G18', {99: data})
except KeyError as e:
    print(f'Band not found: {e}')
```

## Usage Examples

### Basic Usage

```python
from goesdatabuilder.store.datasets.goesmulticloudzarr import GOESZarrStore
import numpy as np

with GOESZarrStore('./goes_config.yaml') as store:
    store.initialize_store('./goes_data.zarr')

    lat_grid = np.linspace(10, 55, 900)
    lon_grid = np.linspace(-135, -60, 1500)

    store.initialize_region(
        region='GOES-East',
        lat=lat_grid,
        lon=lon_grid,
        regridder=regridder,
    )

    # Append observations
    for timestamp, cmi, dqf, platform in processed_data:
        store.append_observation(
            'GOES-East', timestamp, platform, cmi, dqf
        )

    store.finalize_dataset()
```

### Batch Processing

```python
observations = []
for obs in processed_observations:
    observations.append({
        'timestamp': obs.time,
        'platform_id': obs.platform,
        'scan_mode': obs.scan_mode,
        'cmi_data': obs.cmi,
        'dqf_data': obs.dqf,
    })

start_idx, end_idx = store.append_batch('GOES-East', observations)
store.update_temporal_coverage('GOES-East')
```

## API Reference

### Constructor
```python
GOESZarrStore(config_path: str | Path)
```

### Class Method
```python
from_existing(store_path: str | Path, config_path: str | Path, mode: str = "r+") -> GOESZarrStore
```

### Store Initialization
```python
initialize_store(store_path: Union[str, Path], overwrite: bool = False) -> None
initialize_region(region: str, lat: np.ndarray, lon: np.ndarray,
                  bands: Optional[list] = None, include_dqf: bool = True,
                  regridder: Optional[GeostationaryRegridder] = None) -> None
```

### Data Operations
```python
append_observation(region: str, timestamp, platform_id: str, cmi_data: dict,
                   dqf_data: Optional[dict] = None, scan_mode: Optional[str] = None) -> int
append_batch(region: str, observations: list) -> tuple[int, int]
```

### Query Methods
```python
get_time_range(region: str) -> Optional[tuple]
get_observation_count(region: str) -> int
get_spatial_extent(region: str) -> dict
get_bands(region: str) -> list[int]
get_bands_for_product(product_name: str) -> list[int]
get_products_for_band(band: int) -> list[str]
list_all_products() -> list[str]
get_platforms(region: str) -> list[str]
```

### Metadata Methods
```python
update_temporal_coverage(region: str) -> None
add_processing_history(message: str) -> None
add_source_files(region: str, file_paths: list[str]) -> None
finalize_dataset() -> None
```

### Instance Attributes
```python
REGIONS: list[str]              # From multicloudconstants.REGIONS
BANDS: list[int]                # Configured bands (default: 1-16)
BAND_METADATA: dict[int, dict]  # Per-band metadata (config with defaults fallback)
CELL_METHODS: str               # CF cell methods string (class constant)
```