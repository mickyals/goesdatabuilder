The existing doc (document 34) is comprehensive. Let me check against the code (document 33):

1. `rebuild_region_cache` is in the code but not in the doc
2. `_region_shapes` and `_region_bands` caches are in the code but not documented
3. `append_observation` now uses fast-path cache validation instead of store lookups - the doc's error handling section still implies store-based validation
4. `append_batch` also uses cache-based validation now
5. The doc mentions `from_existing` is inherited from `ZarrStoreBuilder` but doesn't mention the need to call `rebuild_region_cache` afterward
6. `_validate_region`, `_validate_observation_shapes`, `_validate_bands_exist` still exist in code as fallback methods but are no longer called by append_observation/append_batch
7. The doc's store structure shows `GOES-Storage` but the REGIONS list includes it

Updates needed are focused on the cache system and rebuild_region_cache.

# GOESZarrStore

## Overview

The `GOESZarrStore` class provides a CF-compliant Zarr store implementation for GOES ABI L2+ satellite imagery. It extends `ZarrStoreBuilder` with domain-specific functionality for storing regridded GOES data with comprehensive CF metadata and extended DQF handling.

### Key Features

- CF compliance with CF-1.13 and ACDD-1.3 standards
- Multi-platform support for GOES-East, GOES-West, GOES-Test, GOES-Storage
- Band-specific metadata for all 16 GOES ABI bands with configurable overrides
- Extended DQF flags (0-6) including interpolated and NaN-source indicators
- Batch and single-observation insertion with cached validation (no per-append store lookups)
- Provenance tracking for processing history and source files
- Configurable array pipeline presets for coordinate and data arrays
- Per-region shape and band caches for fast-path validation during append operations

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
- `time`: Extensible, `datetime64[ns]`, `standard_name='time'`, `long_name='observation time'`, chunks `(512,)`
- `lat`: Static, `float64`, `standard_name='latitude'`, `units='degrees_north'`, single chunk
- `lon`: Static, `float64`, `standard_name='longitude'`, `units='degrees_east'`, single chunk

**Auxiliary Coordinates (aligned with time dimension):**
- `platform_id`: Satellite identifier (e.g., 'G16', 'G18', 'G19'), dtype `U3`, chunks `(512,)`
- `scan_mode`: ABI scan mode (e.g., '3', '4', '6'), dtype `U10`, chunks `(512,)`

**Data Arrays:**
- `CMI_C##`: Cloud and Moisture Imagery, `float32`, bands 1-16, preset `default`
- `DQF_C##`: Data Quality Flags, `uint8`, extended flags 0-6, preset `secondary`

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
from goesdatabuilder.store.datasets import GOESZarrStore

# Create new store
store = GOESZarrStore(config_path='./goes_config.yaml')
store.initialize_store('./goes_data.zarr')

# Open existing store
store = GOESZarrStore.from_existing(
    store_path='./goes_data.zarr',
    config_path='./goes_config.yaml',
    mode='r+'
)
# Must rebuild caches when using from_existing
store.rebuild_region_cache('GOES-East')
```

### What Happens at Construction

1. `ZarrStoreBuilder.__init__` loads and validates the config, sets `_store`, `_root`, `_store_path` to `None`
2. `_load_goes_config` reads GOES-specific config: regions from `multicloudconstants.REGIONS`, bands from config (fallback to `multicloudconstants.BANDS`), band metadata with per-band fallback to `multicloudconstants.DEFAULT_BAND_METADATA`
3. Per-region caches `_region_shapes` and `_region_bands` are initialized as empty dicts

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
    shards: null
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
      products: [...]

  global_metadata:  # Optional overrides for CF/ACDD global attrs
    title: "GOES ABI L2+ Cloud and Moisture Imagery"
    institution: "University of Toronto"

  processing:  # Optional software provenance
    software_name: "geolab"
    software_version: "0.1.0"
```

Regions are defined in `multicloudconstants.REGIONS`, not in config. Band metadata keys in YAML can be integers or strings (automatically converted to int).

## Region Cache System

`GOESZarrStore` maintains per-region caches to avoid repeated store lookups during append operations:

- `_region_shapes: dict[str, tuple[int, int]]`: Maps region name to `(n_lat, n_lon)` shape
- `_region_bands: dict[str, set[int]]`: Maps region name to set of initialized band numbers

**Cache population:**
- `initialize_region` populates both caches at the end of region setup
- `rebuild_region_cache` populates caches from an existing store (for `from_existing` workflows)

**Cache usage:**
- `append_observation` checks `_region_shapes` for shape validation and `_region_bands` for band existence, with no store lookups
- `append_batch` uses the same cached validation

**Fallback validation methods** (`_validate_region`, `_validate_observation_shapes`, `_validate_bands_exist`) still exist for external callers or workflows that bypass the cache, but are not called by the append methods.

```python
# After from_existing, caches are empty. Must rebuild before appending.
store = GOESZarrStore.from_existing(store_path, config_path, mode='r+')
store.rebuild_region_cache('GOES-East')

# Now append_observation will use cached shapes/bands
store.append_observation('GOES-East', timestamp, 'G18', cmi_data, dqf_data)
```

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
# Caches are populated automatically after initialize_region

# Rebuild caches for from_existing workflows
store.rebuild_region_cache('GOES-East')
```

`initialize_region` validates that latitude is monotonic and longitude is monotonic (checked in 0-360 space for antimeridian-crossing grids via `validate_longitude_monotonic`).

### Data Insertion

```python
# Single observation
time_idx = store.append_observation(
    region='GOES-East',
    timestamp=np.datetime64('2024-10-10T20:45:00'),
    platform_id='G18',
    cmi_data={1: cmi_band1_2d, 2: cmi_band2_2d, ...},
    dqf_data={1: dqf_band1_2d, 2: dqf_band2_2d, ...},  # Optional
    scan_mode='6',  # Optional, defaults to 'unknown'
)

# Batch observations (single resize per array, more efficient)
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

Both methods use cached region metadata for validation. `append_batch` validates that all observations share the same band set and spatial dimensions before any writes, then performs a single resize per array followed by bulk writes.

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
products = store.get_products_for_band(7)                    # ['Fire/hotspot characterization', ...]
fire_bands = store.get_bands_for_product('Fire/hotspot characterization')
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

`finalize_dataset` iterates over all entries in `REGIONS`, updating temporal coverage for any region that exists in the store and logging a warning for configured regions not found.

## Band Metadata

### Default Metadata

Band metadata defaults are defined in `multicloudconstants.DEFAULT_BAND_METADATA` and can be overridden per-band via config. The `_load_goes_config` method merges config overrides with defaults on a per-band basis: if band N is in config, use config; otherwise fall back to `DEFAULT_BAND_METADATA[N]`.

**Reflectance Bands (1-6):**
- Wavelength: 0.47-2.24 um
- Units: dimensionless reflectance (1)
- Standard name: `toa_bidirectional_reflectance`

**Brightness Temperature Bands (7-16):**
- Wavelength: 3.90-13.28 um
- Units: Kelvin (K)
- Standard name: `toa_brightness_temperature`

### CMI Array Attributes

Each CMI array gets CF attributes including: `long_name`, `standard_name`, `units`, `radiation_wavelength`, `radiation_wavelength_units` (um), `cell_methods` (`time: point latitude,longitude: mean`), `coordinates` (`time lat lon`), `ancillary_variables` (linking to corresponding DQF array), and optionally `description`, `products`, and `valid_range` from band metadata.

### DQF Array Attributes

DQF arrays get: `long_name`, `standard_name` (`status_flag`), `units` (1), `flag_values`, `flag_meanings`, `valid_range`, `coordinates`, and a `comment` describing the extended flags 5-6.

## Array Pipeline Presets

Coordinate and data arrays use different pipeline presets from the zarr config:

- **`default`**: Used for CMI data arrays (float32, 3D)
- **`secondary`**: Used for DQF arrays (uint8, 3D) and all coordinate/auxiliary arrays (1D)

Coordinate creation methods pass `preset='secondary'` and override `chunks` to match the coordinate array length (single chunk for lat/lon, 512 for time/auxiliary). This prevents 3D chunk/shard configs from the preset being applied to 1D arrays.

## Error Handling

```python
# Region validation
try:
    store.initialize_region('InvalidRegion', lat_grid, lon_grid)
except ValueError as e:
    print(e)  # "Invalid region 'InvalidRegion'. Must be one of [...]"

# Non-monotonic coordinates
try:
    store.initialize_region('GOES-East', scrambled_lat, lon_grid)
except ValueError as e:
    print(e)  # "Latitude array must be monotonic"

# Region not initialized (cache miss)
try:
    store.append_observation('GOES-East', timestamp, 'G18', cmi_data)
except KeyError as e:
    print(e)  # "Region 'GOES-East' not initialized in store"

# Shape mismatch
try:
    store.append_observation('GOES-East', timestamp, 'G18', {1: wrong_shape_array})
except ValueError as e:
    print(e)  # "CMI band 1 shape (100, 100) does not match expected (4251, 6001)"

# Missing band
try:
    store.append_observation('GOES-East', timestamp, 'G18', {99: data})
except KeyError as e:
    print(e)  # "Band 99 CMI array not found in region 'GOES-East'"

# Batch band mismatch
try:
    store.append_batch('GOES-East', mixed_band_observations)
except ValueError as e:
    print(e)  # "Observation 3 has bands {1, 2} expected {1, 2, 3}"

# Rebuild cache for missing region
try:
    store.rebuild_region_cache('NonexistentRegion')
except KeyError as e:
    print(e)  # "Region 'NonexistentRegion' not found in store"
```

## Usage Examples

### Basic Usage

```python
from goesdatabuilder.store.datasets import GOESZarrStore
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

    for timestamp, cmi, dqf, platform in processed_data:
        store.append_observation(
            'GOES-East', timestamp, platform, cmi, dqf
        )

    store.finalize_dataset()
```

### Resuming from Existing Store

```python
store = GOESZarrStore.from_existing(
    store_path='./goes_data.zarr',
    config_path='./goes_config.yaml',
    mode='r+',
)
store.rebuild_region_cache('GOES-East')

# Now safe to append
store.append_observation('GOES-East', timestamp, 'G18', cmi_data, dqf_data)
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
GOESZarrStore(config_path: Union[str, Path])
```

### Inherited Class Method
```python
from_existing(store_path: str | Path, config_path: str | Path, mode: str = "r+") -> GOESZarrStore
```

### Store Initialization
```python
initialize_store(store_path: Union[str, Path], overwrite: bool = False) -> None
initialize_region(region: str, lat: np.ndarray, lon: np.ndarray,
                  bands: Optional[list] = None, include_dqf: bool = True,
                  regridder: Optional[GeostationaryRegridder] = None) -> None
rebuild_region_cache(region: str) -> None
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
BANDS: list[int]                # Configured bands (default: all 16)
BAND_METADATA: dict[int, dict]  # Per-band metadata (config with defaults fallback)
CELL_METHODS: str               # CF cell methods string (class constant)
_region_shapes: dict[str, tuple[int, int]]  # Cached (n_lat, n_lon) per region
_region_bands: dict[str, set[int]]          # Cached band sets per region
```

## Dependencies

- **numpy**: Array operations
- **ZarrStoreBuilder**: Base class (store lifecycle, group/array management, metadata, codecs)
- **multicloudconstants**: `REGIONS`, `BANDS`, `DEFAULT_BAND_METADATA`, `DQF_FLAGS`, `REFLECTANCE_BANDS`
- **grid_utils.validate_longitude_monotonic**: Antimeridian-safe longitude monotonicity check
- **GeostationaryRegridder** (TYPE_CHECKING only): For `regridding_provenance()` type hints