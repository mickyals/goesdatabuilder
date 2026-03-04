# GeostationaryRegridder

## Overview

The `GeostationaryRegridder` transforms GOES ABI geostationary data from fixed grid x/y radian coordinates to a regular latitude/longitude grid using Delaunay triangulation with barycentric interpolation. Interpolation weights are computed once per source/target grid pair and cached to disk for reuse.

### Pipeline Position

```
GOESMultiCloudObservation -> GeostationaryRegridder -> GOESZarrStore
```

The regridder sits between observation loading and store writing. It accepts 2D or 3D arrays (NumPy or xarray DataArrays, including Dask-backed) and produces regridded output on a regular lat/lon grid.

## Interpolation Method

### Reference Band Strategy

The regridder uses a single band (default: band 7, 3.9 um shortwave window) to build the Delaunay triangulation. All bands share the same ABI fixed grid geometry, so weights computed from the reference band apply to every band. Band 7 is the default because it has good availability and minimal atmospheric absorption.

### Weight Computation Steps

1. Convert source x/y (radians) to lat/lon via inverse geostationary projection (`_radians_to_latlon`)
2. Filter out off-disk points (NaN coordinates) using `_source_coord_mask`
3. Build Delaunay triangulation from valid source lat/lon points (`scipy.spatial.Delaunay`)
4. For each target grid point, find the enclosing triangle via `find_simplex`
5. Compute barycentric weights from the triangle's transform matrix
6. Points outside the convex hull get `simplex == -1` and are masked

The barycentric weights for each target point sum to 1.0 and index into three source vertices.

### DQF Classification Logic

DQF (Data Quality Flag) values are categorical, not continuous. Barycentric interpolation of categorical data is handled by `_classify_dqf_2d` with these rules:

- **Direct hit** (max weight >= 0.999): preserve source DQF from the dominant vertex
- **Interpolated, same quality** (weighted average rounds to integer within epsilon 1e-6): preserve that DQF
- **Interpolated, mixed quality** (weighted average is non-integer): DQF = 5 (interpolated)
- **Interpolated, NaN source** (weighted average is NaN, inside hull): DQF = 6 (nan\_source)
- **Outside hull**: DQF = 3 (no\_value)

DQF flag values are sourced from `multicloudconstants` named constants (the single source of truth for all flag definitions):

```python
dqf_out = np.full(..., multicloudconstants.DQF_NO_VALUE, dtype=np.uint8)
# ...
dqf_out[float_indices] = multicloudconstants.DQF_INTERPOLATED
dqf_out[nan_hull_indices] = multicloudconstants.DQF_NAN_SOURCE
```

### Extended DQF Values

Flags 0-4 are from the original ABI L2 CMI product. Flags 5-6 are added by the regridding pipeline. All defined in `multicloudconstants` as both the `DQF_FLAGS` dict and named integer constants.

| Flag | Constant | CF Meaning |
|------|----------|------------|
| 0 | `DQF_GOOD` | good\_pixels\_qf |
| 1 | `DQF_CONDITIONALLY_USABLE` | conditionally\_usable\_pixels\_qf |
| 2 | `DQF_OUT_OF_RANGE` | out\_of\_range\_pixels\_qf |
| 3 | `DQF_NO_VALUE` | no\_value\_pixels\_qf |
| 4 | `DQF_FOCAL_PLANE_TEMP_EXCEEDED` | focal\_plane\_temperature\_threshold\_exceeded\_qf |
| 5 | `DQF_INTERPOLATED` | interpolated\_qf |
| 6 | `DQF_NAN_SOURCE` | nan\_source |

## Initialization

### Standard (with source data)

```python
from goesdatabuilder.regrid.geostationary import GeostationaryRegridder

regridder = GeostationaryRegridder(
    source_x=obs.x.values,           # 1D, radians (E/W scan angle)
    source_y=obs.y.values,           # 1D, radians (N/S elevation angle)
    projection=obs.projection,       # dict from goes_imager_projection attrs
    target_resolution=0.02,          # degrees (used if target_lat/lon not given)
    weights_dir='./weights/GOES-East/',
    load_cached=True,                # load from disk if available
    decimals=4,                      # coordinate rounding precision
    reference_band=7                 # stored in metadata only
)
```

If `target_lat` and `target_lon` are not provided, the constructor auto-computes them from source grid bounds at the given `target_resolution`. Longitude arrays are built via `grid_utils.build_longitude_array` (antimeridian-safe).

If `load_cached=True` and valid cached weights exist in `weights_dir`, they are loaded. Otherwise, weights are computed from scratch and saved.

`_decimals` is set unconditionally from the `decimals` parameter regardless of whether explicit target arrays are provided or auto-computed.

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_x` | np.ndarray | required | 1D x coordinates in radians |
| `source_y` | np.ndarray | required | 1D y coordinates in radians |
| `projection` | dict | required | Geostationary projection parameters (from `goes_imager_projection` attrs) |
| `target_resolution` | float | 0.02 | Target grid spacing in degrees (ignored if target\_lat/lon provided) |
| `target_lat` | np.ndarray or None | None | Explicit 1D target latitude array |
| `target_lon` | np.ndarray or None | None | Explicit 1D target longitude array |
| `weights_dir` | str, Path, or None | None | Directory for cached weight files |
| `load_cached` | bool | True | Load existing cached weights if available |
| `decimals` | int | 4 | Decimal precision for coordinate rounding |
| `reference_band` | int | 7 | Band number stored in metadata (no effect on computation) |

### From Cached Weights (no source data)

```python
regridder = GeostationaryRegridder.from_weights('./weights/GOES-East/')
```

Creates an instance from a cached weights directory without requiring source arrays. Loads `target_lat.npy` and `target_lon.npy` directly from disk (antimeridian-safe, no reconstruction from metadata min/max). All cache files must be present (see Weight Caching below). Raises `FileNotFoundError` if the directory or any required file is missing.

### Custom Target Grid

```python
target_lat = np.arange(25.0, 50.0, 0.01)
target_lon = np.arange(-125.0, -65.0, 0.01)

regridder = GeostationaryRegridder(
    source_x=obs.x.values,
    source_y=obs.y.values,
    projection=obs.projection,
    target_lat=target_lat,
    target_lon=target_lon,
    weights_dir='./weights/custom/'
)
```

## Regridding API

### Continuous Data (CMI)

```python
# 2D NumPy: (y, x) -> (lat, lon)
result = regridder.regrid(cmi_2d)

# 3D NumPy: (time, y, x) -> (time, lat, lon)
result = regridder.regrid(cmi_3d)

# xarray DataArray (Dask-backed): parallelizes across time via apply_ufunc
result = regridder.regrid(cmi_da)

# Disable automatic spatial rechunking (raises if spatially chunked)
result = regridder.regrid(cmi_da, rechunk=False)

# Multiple bands at once
cmi_dict = regridder.regrid_batch({7: band7_data, 13: band13_data})
```

`regrid()` accepts 2D `(y, x)` or 3D `(time, y, x)` inputs. For xarray input, spatial dims y and x must not be chunked; if they are, the method rechunks to full spatial extent automatically (controlled by the `rechunk` parameter, default True).

Points outside the source convex hull are set to NaN in the output.

### Categorical Data (DQF)

```python
# Same input types and shapes as regrid()
dqf_out = regridder.regrid_dqf(dqf_2d)
dqf_out = regridder.regrid_dqf(dqf_da, rechunk=True)

# Multiple bands
dqf_dict = regridder.regrid_dqf_batch({7: dqf_band7, 13: dqf_band13})
```

Output dtype is always `uint8`.

### Combined CMI + DQF

```python
# Regrid both dicts together
regridded_cmi, regridded_dqf = regridder.regrid_observation(cmi_data, dqf_data)
```

`regrid_observation` is a convenience wrapper that calls `regrid_batch` + `regrid_dqf_batch`.

### Full Observation Extraction

```python
obs_dict = regridder.regrid_to_observation_dict(obs, time_idx=0, bands=[7, 8, 13])
```

Extracts a single timestep from a `GOESMultiCloudObservation`, calls `.values` on all CMI/DQF arrays (triggering Dask computation), regrids everything, and returns a dict ready for `GOESZarrStore.append_observation`:

```python
{
    'timestamp': np.datetime64,
    'platform_id': str,
    'scan_mode': str,
    'cmi_data': {band: np.ndarray(lat, lon), ...},
    'dqf_data': {band: np.ndarray(lat, lon), ...},
}
```

For lazy processing, use `regrid()` directly on DataArrays instead.

## Weight Caching

### Cache Files

| File | Shape | Description |
|------|-------|-------------|
| `vertices.npy` | (N\_target, 3) | Triangle vertex indices into valid source points |
| `weights.npy` | (N\_target, 3) | float32 barycentric weights per target point |
| `hull_mask.npy` | (N\_target,) | bool, True where target is outside source hull |
| `source_coord_mask.npy` | (N\_source\_flat,) | bool, True where source lat/lon are valid (not off-disk) |
| `target_lat.npy` | (N\_lat,) | 1D target latitude array |
| `target_lon.npy` | (N\_lon,) | 1D target longitude array |
| `metadata.json` | -- | Grid shape, resolution, coverage stats, reference band, timestamp |

### Save and Load

```python
regridder.save_weights('./weights/')    # saves all files above + metadata.json
regridder.load_weights('./weights/')    # loads arrays + updates _cached flag
```

`save_weights` raises `ValueError` if no `weights_dir` is specified (either as argument or from init). Creates the directory if it does not exist.

### Cache Validation

`_validate_cached_weights` checks:
1. All 7 required files exist in the directory
2. `metadata.json` parses without error
3. `target_shape` in metadata matches the shapes of the cached coordinate arrays

Returns False (triggering recomputation) if any check fails.

### Performance

- First computation: ~40 minutes, 2-4 GB memory
- Subsequent loads: <1 second, ~100-200 MB memory

### metadata.json Contents

```json
{
  "source_shape": [5424, 5424],
  "target_shape": [4251, 6001],
  "target_lat_min": -81.33,
  "target_lat_max": 81.33,
  "target_lon_min": -156.06,
  "target_lon_max": -36.06,
  "target_lat_resolution": 0.02,
  "target_lon_resolution": 0.02,
  "decimals": 4,
  "n_target_points": 25510251,
  "n_valid_points": 18234567,
  "coverage_fraction": 0.7148,
  "direct_hit_fraction": 0.3421,
  "interpolated_fraction": 0.3727,
  "reference_band": 7,
  "created_at": "2024-06-15T14:30:00.000000+00:00Z"
}
```

## Properties

| Property | Type | Description |
|----------|------|-------------|
| `target_lat` | np.ndarray | 1D target latitude array |
| `target_lon` | np.ndarray | 1D target longitude array |
| `target_shape` | tuple[int, int] | (n\_lat, n\_lon) |
| `source_shape` | tuple[int, int] | (n\_y, n\_x) |
| `n_target_points` | int | Total points in target grid (n\_lat * n\_lon) |
| `n_valid_points` | int | Target points inside source convex hull |
| `coverage_fraction` | float | n\_valid / n\_total |
| `direct_hit_fraction` | float | Fraction of valid points with max weight >= 0.999 |
| `interpolated_fraction` | float | Fraction of valid points with max weight < 0.999 |
| `has_cached_weights` | bool | True if weights were loaded from cache |
| `weights_dir` | Path or None | Cache directory path |

## Diagnostics

### Weight Statistics

```python
stats = regridder.weight_statistics()
# Returns:
#   max_weight_mean, max_weight_std   (for valid points only)
#   min_weight_mean, min_weight_std   (for valid points only)
#   direct_hit_fraction, interpolated_fraction, coverage_fraction
```

### Diagnostic Maps

```python
# (lat, lon) bool: True where source covers target
coverage = regridder.coverage_map()

# (lat, lon) uint8: 0=direct hit, 1=interpolated, 2=no coverage
interp_map = regridder.interpolation_map()
```

## CF Metadata Helpers

### dqf\_attrs (static method)

```python
attrs = GeostationaryRegridder.dqf_attrs()
# Returns: standard_name, flag_values, flag_meanings, valid_range, comment
# Sources flag definitions from multicloudconstants.DQF_FLAGS
```

### regridding\_provenance (instance method)

```python
provenance = regridder.regridding_provenance()
# Returns: method, source_projection, triangulation, direct_hit_threshold,
#   integer_epsilon, coverage_fraction, direct_hit_fraction,
#   interpolated_fraction, reference_band, weights_path (if set)
```

This dict is consumed by `GOESZarrStore.initialize_region` to attach regridding provenance as region group attributes.

## Class Constants

```python
DIRECT_HIT_THRESHOLD = 0.999    # Max-weight threshold for preserving source value
INTEGER_EPSILON = 1e-6           # Tolerance for integer DQF detection
```

DQF flag constants are defined in `multicloudconstants` (not on this class): `DQF_GOOD`, `DQF_CONDITIONALLY_USABLE`, `DQF_OUT_OF_RANGE`, `DQF_NO_VALUE`, `DQF_FOCAL_PLANE_TEMP_EXCEEDED`, `DQF_INTERPOLATED`, `DQF_NAN_SOURCE`.

Cache file name constants: `VERTICES_FILE`, `WEIGHTS_FILE`, `HULL_MASK_FILE`, `SOURCE_COORD_MASK_FILE`, `TARGET_LAT_FILE`, `TARGET_LON_FILE`, `METADATA_FILE`.

## Known Issues

1. **`_compute_native_pixel_weights` is a TODO stub**: The viewing zenith angle per-pixel quality weight computation is not implemented. The method body is `pass`.

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
    decimals: int = 4,
    reference_band: int = 7
)
```

### Class Methods / Static Methods
```python
@classmethod
from_weights(weights_dir: Union[str, Path]) -> GeostationaryRegridder

@staticmethod
dqf_attrs() -> dict
```

### Instance Methods
```python
regrid(data: Union[np.ndarray, xr.DataArray], rechunk: bool = True) -> Union[np.ndarray, xr.DataArray]
regrid_dqf(dqf: Union[np.ndarray, xr.DataArray], rechunk: bool = True) -> Union[np.ndarray, xr.DataArray]
regrid_batch(data: dict[int, Union[np.ndarray, xr.DataArray]]) -> dict[int, Union[np.ndarray, xr.DataArray]]
regrid_dqf_batch(dqf: dict[int, Union[np.ndarray, xr.DataArray]]) -> dict[int, Union[np.ndarray, xr.DataArray]]
regrid_observation(cmi_data: dict, dqf_data: dict) -> tuple[dict, dict]
regrid_to_observation_dict(obs: GOESMultiCloudObservation, time_idx: int = 0, bands: Optional[list[int]] = None) -> dict
save_weights(weights_dir: Optional[Union[str, Path]] = None) -> None
load_weights(weights_dir: Union[str, Path]) -> None
weight_statistics() -> dict
coverage_map() -> np.ndarray
interpolation_map() -> np.ndarray
regridding_provenance() -> dict
```