# GeostationaryRegridder

## Overview

The `GeostationaryRegridder` class transforms GOES ABI geostationary data from x/y radian coordinates to regular latitude/longitude grids. It uses Delaunay triangulation with barycentric interpolation and includes intelligent weight caching for efficient repeated processing.

### Key Features

- **Delaunay triangulation** with barycentric interpolation
- **Weight caching** for fast repeated processing
- **DQF handling** with extended flag for interpolated values
- **Dask support** for parallel time processing
- **Reference band strategy** for grid consistency

## Interpolation Method

### Reference Band Strategy

Uses band 7 (3.9 μm) as reference for consistent grid:
- Optimal balance of resolution and availability
- Minimal atmospheric absorption
- Ensures grid consistency across all bands

### Delaunay Triangulation

Process:
1. Build triangulation from source grid points
2. Locate target points within triangles
3. Calculate barycentric coordinates
4. Apply weights to source values
5. Assign quality flags based on interpolation

## Data Quality Flags

### Extended DQF Values

| Flag | Value | Description |
|------|-------|-------------|
| 0 | good_pixels_qf | Original good data |
| 1 | conditionally_usable_pixels_qf | Original conditionally usable |
| 2 | out_of_range_pixels_qf | Original out-of-range |
| 3 | no_value_pixels_qf | Original no-value |
| 4 | focal_plane_temperature_threshold_exceeded_qf | Original focal plane temp exceeded |
| 5 | interpolated_qf | Value computed via barycentric interpolation |

### Interpolation Logic

- **Direct hit** (weight > 0.999): Preserve source DQF
- **Interpolated to integer** (all sources same): Preserve that DQF
- **Interpolated to float** (mixed sources): Set DQF = 5
- **Outside convex hull**: Set DQF = 3

## Class Structure

### Initialization

```python
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder

regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02,
    weights_dir='./weights',
    reference_band=7
)
```

### Parameters

- `source_x`: X coordinates in radians
- `source_y`: Y coordinates in radians
- `projection`: Geostationary projection parameters
- `target_resolution`: Target grid resolution in degrees
- `weights_dir`: Directory for cached weights
- `reference_band`: Band for weight computation (default: 7)
- `decimals`: Precision for coordinate rounding (default: 4)

## Core Methods

### Regridding Operations

```python
# Continuous data (CMI)
cmi_regridded = regridder.regrid(data)  # NumPy or xarray

# Quality flags (DQF)
dqf_regridded = regridder.regrid_dqf(dqf_data)

# Batch processing
cmi_dict = regridder.regrid_batch({band: data for band, data in ...})
dqf_dict = regridder.regrid_dqf_batch({band: dqf for band, dqf in ...})

# Full observation
obs_dict = regridder.regrid_to_observation_dict(obs, time_idx=0)
```

### Weight Management

```python
# Save weights
regridder.save_weights('./weights')

# Load existing weights
regridder.load_weights('./weights')

# Check if cached
if regridder.has_cached_weights:
    print("Weights available")
```

## Weight Caching

### Cache Files

- `vertices.npy`: Delaunay triangulation vertices
- `weights.npy`: Barycentric interpolation weights
- `mask.npy`: Convex hull coverage mask
- `target_lat.npy`: Target latitude coordinate array (antimeridian-safe)
- `target_lon.npy`: Target longitude coordinate array (antimeridian-safe)
- `metadata.json`: Grid information

### Performance

**First computation:** ~40 minutes, 2-4 GB memory
**Subsequent loads:** <1 second, ~100-200 MB memory

## Properties and Diagnostics

### Grid Information

```python
print(f"Source shape: {regridder.source_shape}")
print(f"Target shape: {regridder.target_shape}")
print(f"Coverage: {regridder.coverage_fraction:.2%}")
```

### Statistics

```python
stats = regridder.weight_statistics()
print(f"Direct hits: {stats['direct_hit_fraction']:.2%}")
print(f"Interpolated: {stats['interpolated_fraction']:.2%}")
```

### Diagnostic Maps

```python
coverage = regridder.coverage_map()  # Boolean coverage map
interp_map = regridder.interpolation_map()  # Interpolation types
```

## Coordinate Transformation

Converts from geostationary scanning angles (radians) to geographic coordinates (degrees):
- Input: x/y radians with projection parameters
- Output: latitude/longitude degrees
- Uses inverse perspective projection

## CF Compliance

### DQF Attributes

```python
dqf_attrs = GeostationaryRegridder.dqf_attrs()
```

Returns CF-compliant attributes for DQF variables including flag values and meanings.

### Provenance

```python
provenance = regridder.regridding_provenance()
```

Returns processing history for metadata tracking.

## Pipeline Integration

### Typical Workflow

```python
# 1. Initialize regridder
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.02,
    weights_dir='./weights'
)

# 2. Process observation
obs_dict = regridder.regrid_to_observation_dict(obs, bands=[1, 2, 3, 7])

# 3. Store results
store.append_observation(obs_dict)
```

## Performance Considerations

### Memory Management

- Use target_resolution ≥ 0.01 for memory efficiency
- Process bands in batches for limited memory
- Store weights on fast storage (SSD)

### Computational Efficiency

- Batch process multiple bands together
- Use cached weights across time periods
- Parallel processing works with Dask arrays

## Configuration Examples

### High Resolution

```python
regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_resolution=0.005,  # ~500m
    weights_dir='./weights/high_res'
)
```

### Custom Grid

```python
target_lat = np.linspace(25, 50, 500)
target_lon = np.linspace(-125, -65, 600)

regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_lat=target_lat,
    target_lon=target_lon
)
```

## Troubleshooting

### Common Issues

- **Memory errors**: Reduce target resolution or process fewer bands
- **Slow performance**: Ensure weights are cached and use SSD storage
- **Poor coverage**: Check source data and projection parameters
- **Interpolation artifacts**: Increase target resolution or check data quality

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
    reference_band: int = 7,
    decimals: int = 4
)
```

### Class Methods
```python
from_weights(weights_dir: Union[str, Path]) -> 'GeostationaryRegridder'
dqf_attrs() -> dict
```

### Instance Methods
```python
regrid(data: Union[np.ndarray, xr.DataArray]) -> Union[np.ndarray, xr.DataArray]
regrid_batch(data: dict[int, Union[np.ndarray, xr.DataArray]]) -> dict[int, Union[np.ndarray, xr.DataArray]]
regrid_dqf(dqf: Union[np.ndarray, xr.DataArray]) -> Union[np.ndarray, xr.DataArray]
regrid_dqf_batch(dqf: dict[int, Union[np.ndarray, xr.DataArray]]) -> dict[int, Union[np.ndarray, xr.DataArray]]
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
```

### Constants
```python
DQF_GOOD = 0
DQF_CONDITIONALLY_USABLE = 1
DQF_OUT_OF_RANGE = 2
DQF_NO_VALUE = 3
DQF_FOCAL_PLANE_TEMP_EXCEEDED = 4
DQF_INTERPOLATED = 5
DIRECT_HIT_THRESHOLD = 0.999
```
