# GeostationaryRegridder

## Overview

The `GeostationaryRegridder` class provides high-performance regridding of GOES ABI geostationary imager data from native x/y radian coordinates to regular latitude/longitude grids. It uses Delaunay triangulation with barycentric interpolation, featuring weight computation and caching for efficient repeated processing.

## Key Features

- **Delaunay Triangulation**: Robust spatial interpolation using scipy's Delaunay triangulation
- **Weight Caching**: One-time weight computation (~40 minutes) with instant subsequent loads
- **Reference Band Strategy**: Ensures consistent output grid sizes across all bands
- **Extended DQF Handling**: New interpolated flag (5) for regridded data quality
- **Batch Processing**: Efficient multi-band and multi-time processing
- **CF-Compliant Metadata**: Full provenance tracking for downstream processing

## Architecture

### Interpolation Strategy

The regridder uses a **reference band approach** to ensure consistent output grids:

1. **Reference Band**: Band 7 (shortwave window) used by default
2. **Grid Consistency**: Different bands have slightly different source grids (10-20 pixel differences)
3. **Single Weight Set**: Prevents grid size mismatches across bands

### Data Quality Flag (DQF) Handling

Extended DQF flag values for regridded data:

| Flag | Value | Description |
|------|-------|-------------|
| 0 | good_pixels_qf | Original good data preserved |
| 1 | conditionally_usable_pixels_qf | Original conditionally usable data preserved |
| 2 | out_of_range_pixels_qf | Original out-of-range data preserved |
| 3 | no_value_pixels_qf | Original no-value data preserved |
| 4 | focal_plane_temperature_threshold_exceeded_qf | Original focal plane temperature exceeded data preserved |
| 5 | interpolated_qf | **NEW**: Value computed via barycentric interpolation |

### Interpolation Logic

- **Direct Hit** (max weight > 0.999): Preserve source DQF
- **Interpolated** (distributed weights): Set DQF = 5
- **Outside Hull**: Set DQF = 4

## Class Structure

### Initialization

```python
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02,
    weights_dir='./regrid_weights/GOES-East/',
    reference_band=7
)
```

**Parameters:**
- `source_x` (np.ndarray): 1D array of x coordinates (radians)
- `source_y` (np.ndarray): 1D array of y coordinates (radians)
- `projection` (dict): Geostationary projection parameters
- `target_resolution` (float): Target resolution in degrees (default: 0.02)
- `target_lat` (Optional[np.ndarray]): Explicit latitude array (overrides resolution)
- `target_lon` (Optional[np.ndarray]): Explicit longitude array (overrides resolution)
- `weights_dir` (Optional[Path]): Directory for cached weights
- `load_cached` (bool): Load existing weights if available (default: True)
- `reference_band` (int): Band for weight computation (default: 7)

### Alternative Initialization

```python
# Load from cached weights
regridder = GeostationaryRegridder.from_weights('./regrid_weights/GOES-East/')
```

## Core Methods

### Data Regridding

#### Continuous Data (CMI)

```python
# Single band
cmi_regridded = regridder.regrid(obs.get_cmi(8).values)

# Multiple bands
cmi_data = {band: obs.get_cmi(band).values for band in [1, 2, 3]}
cmi_regridded = regridder.regrid_batch(cmi_data)
```

**Input Shapes:**
- 2D: (y, x) - Single observation
- 3D: (time, y, x) - Multiple observations

**Output Shapes:**
- 2D: (lat, lon) - Single observation
- 3D: (time, lat, lon) - Multiple observations

#### Categorical Data (DQF)

```python
# Single band
dqf_regridded = regridder.regrid_dqf(obs.get_dqf(8).values)

# Multiple bands
dqf_data = {band: obs.get_dqf(band).values for band in [1, 2, 3]}
dqf_regridded = regridder.regrid_dqf_batch(dqf_data)
```

### Full Observation Processing

```python
# Process complete observation
regridded_cmi, regridded_dqf = regridder.regrid_observation(cmi_data, dqf_data)

# Package for GOESZarrStore
obs_dict = regridder.regrid_to_observation_dict(
    obs=observation,
    time_idx=0,
    bands=[1, 2, 3, 7, 14]
)
```

Returns dictionary compatible with `GOESZarrStore.append_observation()`:

```python
{
    'timestamp': datetime64,
    'platform_id': str,
    'scan_mode': str,
    'cmi_data': {band: (lat, lon) array},
    'dqf_data': {band: (lat, lon) array},
}
```

## Weight Management

### Caching System

**Files Generated:**
- `vertices.npy`: Delaunay triangulation vertices
- `weights.npy`: Barycentric interpolation weights
- `mask.npy`: Convex hull coverage mask
- `metadata.json`: Grid information and statistics

### Weight Computation Process

1. **Coordinate Conversion**: Convert x/y radians to lat/lon degrees
2. **Source Grid**: Filter out off-earth pixels (NaN values)
3. **Target Grid**: Create regular lat/lon grid from bounds
4. **Triangulation**: Build Delaunay triangulation from source points
5. **Weight Calculation**: Compute barycentric weights for target points
6. **Mask Generation**: Identify points outside convex hull

### Performance Characteristics

**First-Time Computation:**
- Time: ~40 minutes for full disk GOES data
- Memory: ~2-4 GB depending on grid size
- Output: ~100-200 MB of cached weights

**Subsequent Loads:**
- Time: <1 second
- Memory: ~100-200 MB
- No recomputation required

## Properties and Diagnostics

### Grid Properties

```python
print(f"Source shape: {regridder.source_shape}")
print(f"Target shape: {regridder.target_shape}")
print(f"Coverage: {regridder.coverage_fraction:.2%}")
print(f"Cached: {regridder.has_cached_weights}")
```

### Interpolation Statistics

```python
print(f"Direct hits: {regridder.direct_hit_fraction:.2%}")
print(f"Interpolated: {regridder.interpolated_fraction:.2%}")
print(f"Valid points: {regridder.n_valid_points:,}")
print(f"Total points: {regridder.n_target_points:,}")
```

### Diagnostic Maps

```python
# Coverage map (True = valid data)
coverage = regridder.coverage_map()

# Interpolation map (0=direct, 1=interpolated, 2=no_coverage)
interp_map = regridder.interpolation_map()

# Weight statistics
stats = regridder.weight_statistics()
```

## Coordinate Transformation

### Geostationary to Geographic

The regridder converts GOES-R ABI fixed grid projection coordinates:

**Input:**
- x, y in radians (scanning angles)
- Geostationary projection parameters

**Output:**
- Latitude, longitude in degrees

**Projection Parameters Used:**
- `longitude_of_projection_origin`: Satellite longitude
- `perspective_point_height`: Satellite altitude + Earth radius
- `semi_major_axis`: Earth equatorial radius
- `semi_minor_axis`: Earth polar radius

**Mathematical Transformation:**
1. Convert scanning angles to Cartesian coordinates
2. Apply inverse perspective projection
3. Convert Cartesian to geographic coordinates

## CF Metadata Integration

### DQF Attributes

```python
dqf_attrs = GeostationaryRegridder.dqf_attrs()
```

Returns CF-compliant attributes:
```python
{
    'standard_name': 'status_flag',
    'flag_values': [0, 1, 2, 3, 4, 5],
    'flag_meanings': 'good conditionally_usable out_of_range no_value no_input interpolated',
    'valid_range': [0, 5],
    'comment': 'Flag 5 (interpolated) indicates value was computed via barycentric interpolation from neighboring source pixels. Flag 4 (no_input) indicates target location is outside source data convex hull.'
}
```

### Regridding Provenance

```python
provenance = regridder.regridding_provenance()
```

Returns provenance dictionary for GOESZarrStore:
```python
{
    'method': 'barycentric',
    'source_projection': 'geostationary',
    'triangulation': 'delaunay',
    'direct_hit_threshold': 0.999,
    'coverage_fraction': 0.95,
    'direct_hit_fraction': 0.85,
    'interpolated_fraction': 0.10,
    'reference_band': 7,
    'weights_path': './regrid_weights/GOES-East/'
}
```

## Pipeline Integration

### Processing Flow

```
GOESMultiCloudObservation → GeostationaryRegridder → GOESZarrStore
```

### Typical Workflow

```python
# 1. Load observation
obs = GOESMultiCloudObservation(file_path)

# 2. Initialize regridder (first time)
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02,
    weights_dir='./weights/GOES-East/'
)

# 3. Regrid data
obs_dict = regridder.regrid_to_observation_dict(obs, bands=[1, 2, 3, 7])

# 4. Store in Zarr
store = GOESZarrStore(config_path='./config.yaml')
store.append_observation(obs_dict)
```

## Performance Optimization

### Memory Management

**Large Grid Considerations:**
- Use `target_resolution` > 0.01 for memory efficiency
- Process bands in batches for limited memory
- Monitor memory usage during weight computation

**Caching Strategy:**
- Store weights on fast storage (SSD preferred)
- Use consistent target grids across processing runs
- Share weights between bands and time periods

### Computational Efficiency

**Batch Processing:**
```python
# Efficient multi-band processing
bands = [1, 2, 3, 7, 14]
cmi_data = {band: obs.get_cmi(band).values for band in bands}
dqf_data = {band: obs.get_dqf(band).values for band in bands}

cmi_regridded, dqf_regridded = regridder.regrid_observation(cmi_data, dqf_data)
```

**Time Series Processing:**
```python
# Process multiple time steps efficiently
for time_idx in range(len(obs.time)):
    obs_dict = regridder.regrid_to_observation_dict(
        obs, time_idx=time_idx, bands=bands
    )
    store.append_observation(obs_dict)
```

## Configuration Examples

### High Resolution

```python
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.005,  # ~500m resolution
    weights_dir='./weights/high_res/'
)
```

### Custom Grid

```python
# Define custom target grid
target_lat = np.linspace(25, 50, 500)  # 500 points from 25°N to 50°N
target_lon = np.linspace(-125, -65, 600)  # 600 points from 125°W to 65°W

regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_lat=target_lat,
    target_lon=target_lon,
    weights_dir='./weights/custom_grid/'
)
```

### Regional Processing

```python
# CONUS region example
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02,
    weights_dir='./weights/conus/'
)

# Filter to CONUS bounds
conus_mask = (
    (regridder.target_lat >= 25) & 
    (regridder.target_lat <= 50) &
    (regridder.target_lon >= -125) & 
    (regridder.target_lon <= -65)
)
```

## Troubleshooting

### Common Issues

**Memory Errors:**
- Reduce target resolution
- Process fewer bands at once
- Use machine with more RAM

**Slow Performance:**
- Ensure weights are cached
- Use SSD storage for weights directory
- Reduce number of concurrent processes

**Poor Coverage:**
- Check source data validity
- Verify projection parameters
- Consider larger target bounds

**Interpolation Artifacts:**
- Increase target resolution
- Check source data quality
- Validate DQF flags

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable detailed logging
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02
)
```

## API Reference

### Constructor

```python
GeostationaryRegridder(
    source_x: np.ndarray,
    source_y: np.ndarray,
    projection: dict,
    target_resolution: float = 0.02,
    target_lat: Optional[np.ndarray] = None,
    target_lon: Optional[np.ndarray] = None,
    weights_dir: Optional[Union[str, Path]] = None,
    load_cached: bool = True,
    reference_band: int = 7
)
```

### Class Methods

```python
GeostationaryRegridder.from_weights(weights_dir: Union[str, Path]) -> 'GeostationaryRegridder'
GeostationaryRegridder.dqf_attrs() -> dict
```

### Instance Methods

```python
regrid(data: np.ndarray) -> np.ndarray
regrid_batch(data: dict[int, np.ndarray]) -> dict[int, np.ndarray]
regrid_dqf(dqf: np.ndarray) -> np.ndarray
regrid_dqf_batch(dqf: dict[int, np.ndarray]) -> dict[int, np.ndarray]
regrid_observation(cmi_data: dict[int, np.ndarray], dqf_data: dict[int, np.ndarray]) -> tuple
regrid_to_observation_dict(obs: 'GOESMultiCloudObservation', time_idx: int = 0, bands: Optional[list[int]] = None) -> dict
save_weights(weights_dir: Optional[Union[str, Path]] = None) -> None
load_weights(weights_dir: Union[str, Path]) -> None
weight_statistics() -> dict
coverage_map() -> np.ndarray
interpolation_map() -> np.ndarray
regridding_provenance() -> dict
```

### Properties

```python
target_lat: np.ndarray
target_lon: np.ndarray
target_shape: tuple[int, int]
source_shape: tuple[int, int]
n_target_points: int
n_valid_points: int
coverage_fraction: float
has_cached_weights: bool
weights_dir: Optional[Path]
direct_hit_fraction: float
interpolated_fraction: float
```

### Constants

```python
DQF_GOOD = 0
DQF_CONDITIONALLY_USABLE = 1
DQF_OUT_OF_RANGE = 2
DQF_NO_VALUE = 3
DQF_NO_INPUT = 4
DQF_INTERPOLATED = 5
DIRECT_HIT_THRESHOLD = 0.999
VERTICES_FILE = 'vertices.npy'
WEIGHTS_FILE = 'weights.npy'
MASK_FILE = 'mask.npy'
METADATA_FILE = 'metadata.json'
```
