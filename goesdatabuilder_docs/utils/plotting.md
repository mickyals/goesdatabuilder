# Plotting Utilities

## Overview

The `goesdatabuilder.utils.plotting` module provides visualization functions for geospatial data, including scalar fields, RGB composites, DQF categorical maps, and Delaunay triangulation diagnostics.

All functions accept plain numpy arrays with no dependency on any particular storage backend (Zarr, NetCDF, etc.).

### Key Features

- **Scalar field visualization** with pcolormesh and custom normalization
- **RGB composite rendering** for multi-band imagery products
- **DQF categorical mapping** with the standard six-class GOES ABI scheme
- **Delaunay triangulation overlays** for grid structure visualization
- **Geostationary disk and convex hull plots** for data coverage analysis
- **Cartopy integration** with coastlines, borders, and lake overlays
- **Consistent save/display interface** across all plot types

## Coordinate Convention

All functions expect coordinates in degrees on a PlateCarree (equirectangular) grid:

- **lon, lat** (2-D arrays): Coordinate grids for `plot_scalar`, `plot_dqf`, `plot_rgb`
- **lons, lats** (1-D arrays): Flattened coordinates for triangulation and hull plots
- **RGB arrays**: (M, N, 3) float32/64 in [0, 1]
- **Latitude direction**: South-to-north (`origin='lower'` for imshow)

## Common Parameters

The following parameters appear on most `plot_*` functions and share the same meaning throughout:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `figsize` | tuple | (12, 9) | Figure dimensions in inches |
| `feature_color` | str | "yellow" | Edge color for coastline, border, and lake overlays |
| `savepath` | str or None | None | File path for saving; None leaves the figure open |
| `dpi` | int | 150 | Resolution in dots per inch when saving |

## Module-Level Defaults

```python
from goesdatabuilder.utils.plotting import IR_CMAP, WATER_VAPOR_BANDS, DQF_FLAGS

# Default IR colormap for brightness temperature fields
IR_CMAP = "magma"

# Bands rendered with grey colormap instead of IR colormap
WATER_VAPOR_BANDS = {8, 9, 10}

# Six-class DQF color scheme
DQF_FLAGS = {
    0: ("#2ecc71", "Good"),
    1: ("#f1c40f", "Conditionally usable"),
    2: ("#e67e22", "Out of range"),
    3: ("#e74c3c", "No value"),
    4: ("#9b59b6", "Focal plane temp exceeded"),
    5: ("#3498db", "Interpolated"),
}
```

## Normalization and Color Helpers

### `make_ir_norm`

Builds a piecewise-linear normalization that compresses the warmest pixels into a small fraction of the colormap, keeping land/ocean as a uniform dark tone while allocating most color variation to clouds.

```python
from goesdatabuilder.utils.plotting import make_ir_norm

norm = make_ir_norm(bt_data, warm_pct=30.0, cmap_share=0.10)
```

**Parameters:**
- `data` (np.ndarray): 2-D brightness temperature field in Kelvin
- `warm_pct` (float): Percentage of pixels (by value) considered "warm" (default: 30.0)
- `cmap_share` (float): Fraction of colormap allocated to the warm cluster (default: 0.10)

**Returns:**
- `matplotlib.colors.FuncNorm`: Ready to pass as `norm=` to pcolormesh

**Behavior:**
- The warmest `warm_pct`% of pixels are squeezed into the top `cmap_share` fraction of the colormap
- The remaining cooler pixels spread across `1 - cmap_share` of the colormap
- Guards against degenerate cases where all values are identical

### `rescale`

Clips an array to [vmin, vmax] then linearly scales to [0, 1].

```python
from goesdatabuilder.utils.plotting import rescale

# Scale brightness temperature channel to [0, 1] for RGB composition
r = rescale(c13, 243.6, 302.4)
```

**Parameters:**
- `arr` (np.ndarray): Input array in physical units
- `vmin`, `vmax` (float): Clipping bounds

**Returns:**
- `np.ndarray`: Array with values in [0, 1]

### `apply_gamma`

Applies gamma correction: `out = arr^(1/gamma)`, clamped to [0, 1].

```python
from goesdatabuilder.utils.plotting import apply_gamma

# Brighten the cirrus channel with gamma < 1
r = apply_gamma(c04_data, gamma=0.66)
```

**Parameters:**
- `arr` (np.ndarray): Input array, expected in [0, 1]
- `gamma` (float): Gamma value. Values < 1 darken, > 1 brighten

### `stack_rgb`

Stacks three 2-D channels into an (M, N, 3) array clipped to [0, 1].

```python
from goesdatabuilder.utils.plotting import stack_rgb

rgb = stack_rgb(red_channel, green_channel, blue_channel)
```

## Plotting Functions

### `plot_scalar`

Plots any 2-D scalar field with pcolormesh.

```python
from goesdatabuilder.utils.plotting import plot_scalar, make_ir_norm

# Reflectance band with grey colormap
plot_scalar(lon, lat, c02_data, cmap="Greys_r",
            title="CMI_C02 (Red)", label="Reflectance Factor",
            savepath="C02.png")

# Brightness temperature band with IR norm
norm = make_ir_norm(c13_data)
plot_scalar(lon, lat, c13_data, cmap="magma",
            title="CMI_C13 (Clean LW Window)",
            label="Brightness Temperature (K)",
            norm=norm, savepath="C13.png")

# Band difference product
diff = c13_data - c07_data
plot_scalar(lon, lat, diff, cmap="Greys",
            title="Night Fog (C13 - C07)",
            label="BT Difference (K)",
            savepath="Diff_NightFog.png")
```

**Parameters:**
- `lon`, `lat` (2-D arrays): Coordinate grids, same shape as data
- `data` (2-D array): Values to render
- `cmap` (str or Colormap): Matplotlib colormap
- `title` (str): Axes title
- `label` (str): Colorbar label
- `norm` (Normalize, optional): Custom normalization

### `plot_dqf`

Plots a Data Quality Flag array with the standard six-class categorical colormap.

```python
from goesdatabuilder.utils.plotting import plot_dqf

plot_dqf(lon, lat, dqf_c02, title="DQF_C02", savepath="DQF_C02.png")
```

**Parameters:**
- `lon`, `lat` (2-D arrays): Coordinate grids
- `data` (2-D array): Integer DQF values (0 through 5)
- `title` (str): Axes title

### `plot_rgb`

Plots a pre-composed (M, N, 3) RGB image with imshow. Assumes latitude runs south-to-north.

```python
from goesdatabuilder.utils.plotting import plot_rgb, stack_rgb, rescale

# Simple Water Vapor RGB
r = 1.0 - rescale(c13, 202.29, 278.96)
g = 1.0 - rescale(c08, 214.66, 242.67)
b = 1.0 - rescale(c10, 245.12, 261.03)

plot_rgb(lon, lat, stack_rgb(r, g, b),
         title="Simple Water Vapor RGB",
         savepath="RGB_SimpleWaterVapor.png")
```

**Parameters:**
- `lon`, `lat` (2-D or 1-D arrays): Only min/max used for image extent
- `rgb` ((M, N, 3) array): Float RGB image in [0, 1]
- `title` (str): Axes title

## Triangulation

### `build_triangulation`

Single entry point for all Delaunay triangulation in the module. Supports optional spatial filtering and subsampling.

```python
from goesdatabuilder.utils.plotting import build_triangulation

# Sparse overlay for full-disk visualization (~500 vertices)
tri, _ = build_triangulation(lons, lats, n_points=500)

# Dense mesh for zoomed nadir view (all points in bounding box)
bounds = (-139.2, -135.2, -2.0, 2.0)
tri, mask = build_triangulation(lons, lats, bounds=bounds)
# Use mask to subset co-indexed data: data[mask]

# Combined: sparse mesh within a region
tri, mask = build_triangulation(lons, lats, n_points=200, bounds=bounds)
```

**Parameters:**
- `lons`, `lats` (1-D arrays): Geographic coordinates in degrees
- `n_points` (int, optional): Subsample to approximately this many vertices
- `bounds` ((lon_min, lon_max, lat_min, lat_max), optional): Spatial filter

**Returns:**
- `tri` (matplotlib.tri.Triangulation): Ready for triplot/tripcolor
- `mask` (1-D bool array): Index into original arrays for data subsetting

## Geostationary and Hull Plots

### `plot_geostationary_disk`

Renders data point coverage on the native geostationary projection, optionally with a sparse Delaunay mesh overlay.

```python
from goesdatabuilder.utils.plotting import plot_geostationary_disk

plot_geostationary_disk(
    goes_lons, goes_lats,
    nadir_lon=-137.2,
    show_triangulation=True,
    n_tri_points=500,
    title="GOES-West Geostationary Disk",
    savepath="goes_west_disk.png",
)
```

**Parameters:**
- `lons`, `lats` (1-D arrays): Flattened coordinates of every valid pixel
- `nadir_lon` (float): Sub-satellite longitude (projection center)
- `scatter_color` (str): Color for raw data point scatter (default: "green")
- `tri_color` (str): Edge color for triangle overlay (default: "cyan")
- `n_tri_points` (int): Approximate vertex count for sparse mesh (default: 500)
- `show_triangulation` (bool): Whether to overlay the mesh (default: True)

### `plot_convex_hull`

Plots the convex hull of the data footprint in PlateCarree, showing how the geostationary disk maps onto a flat equirectangular grid.

```python
from goesdatabuilder.utils.plotting import plot_convex_hull

plot_convex_hull(
    goes_lons, goes_lats,
    nadir_lon=-137.2,
    central_longitude=-180,  # avoid antimeridian wrap for GOES-West
    extent=[-220, -45, -90, 90],
    show_triangulation=True,
    title="GOES-West Convex Hull",
    savepath="goes_west_hull.png",
)
```

**Parameters:**
- `nadir_lon` (float): Sub-satellite longitude for the nadir marker
- `central_longitude` (float): Center meridian for PlateCarree. Set to -180 for GOES-West to avoid antimeridian artifacts
- `extent` ((lon_min, lon_max, lat_min, lat_max), optional): Map view bounds
- `hull_color` (str): Fill and edge color for hull polygon (default: "cyan")
- `hull_alpha` (float): Fill opacity (default: 0.15)

### `plot_nadir_tessellation`

Zoomed view of the full-resolution Delaunay mesh around the nadir point. Unlike the sparse overlays in `plot_geostationary_disk` and `plot_convex_hull`, this triangulates every point inside the bounding box so individual triangles are visible.

```python
from goesdatabuilder.utils.plotting import plot_nadir_tessellation

# Wireframe only
plot_nadir_tessellation(
    goes_lons, goes_lats,
    nadir_lon=-137.2, nadir_lat=0.0,
    half_extent=2.0,
    title="Tessellation Near Nadir (GOES-West)",
    savepath="nadir_tessellation.png",
)

# With data coloring
plot_nadir_tessellation(
    goes_lons, goes_lats,
    data=geos_data,
    nadir_lon=-137.2,
    data_cmap="Greys_r",
    savepath="nadir_tessellation_data.png",
)
```

**Parameters:**
- `data` (1-D array, optional): Per-vertex values for tripcolor fill. Same length as lons/lats. Pass None for wireframe only.
- `nadir_lon`, `nadir_lat` (float): Center of zoom region and nadir marker location
- `half_extent` (float): Half-width of the square bounding box in degrees (default: 2.0)
- `mesh_color` (str): Wireframe edge color (default: "white")
- `data_cmap` (str): Colormap for data fill (default: "Greys_r")

## Usage Examples

### Scalar Field Examples

```python
import numpy as np
from goesdatabuilder.utils.plotting import (
    plot_scalar, plot_dqf, make_ir_norm, IR_CMAP, WATER_VAPOR_BANDS,
)

# Assume lon, lat are 2-D coordinate grids and data is a 2-D array
# loaded by whatever backend the caller uses (Zarr, NetCDF, etc.)

# Reflectance band
plot_scalar(lon, lat, reflectance_data, "Greys_r",
            "CMI_C02 (Red)", "Reflectance Factor",
            savepath="C02.png")

# Water vapor band
plot_scalar(lon, lat, wv_data, "Greys",
            "CMI_C08 (Upper-Level Water Vapor)", "Brightness Temperature (K)",
            savepath="C08.png")

# IR band with custom norm
plot_scalar(lon, lat, bt_data, IR_CMAP,
            "CMI_C13 (Clean LW Window)", "Brightness Temperature (K)",
            norm=make_ir_norm(bt_data), savepath="C13.png")

# Band difference
diff = c13_data - c07_data
plot_scalar(lon, lat, diff, "Greys",
            "Night Fog (C13 - C07)", "BT Difference (K)",
            savepath="Diff_NightFog.png")

# DQF array
plot_dqf(lon, lat, dqf_data, title="DQF_C02", savepath="DQF_C02.png")
```

### Geostationary Coverage Analysis

```python
from goesdatabuilder.utils.plotting import (
    plot_geostationary_disk, plot_convex_hull, plot_nadir_tessellation,
)

# Full disk view with sparse triangulation
plot_geostationary_disk(
    goes_lons, goes_lats,
    nadir_lon=-137.2,
    title="GOES-West Disk with Triangulation",
    savepath="disk.png",
)

# Convex hull in PlateCarree
plot_convex_hull(
    goes_lons, goes_lats,
    nadir_lon=-137.2,
    central_longitude=-180,
    extent=[-220, -45, -90, 90],
    savepath="hull.png",
)

# Dense tessellation near nadir
plot_nadir_tessellation(
    goes_lons, goes_lats,
    data=geos_data,
    nadir_lon=-137.2,
    half_extent=2.0,
    savepath="nadir.png",
)
```

## Integration with goes_composites Module

The plotting module works with the `goes_composites` module for recipe-driven visualization:

```python
from goesdatabuilder.utils.goes_composites import get_rgb, bands_for
from goesdatabuilder.utils.plotting import plot_rgb, rescale, stack_rgb

# Look up the Ash RGB recipe
recipe = get_rgb("ash")
needed_bands = bands_for("ash")  # [11, 13, 14, 15]

# Assume c11, c13, c14, c15 are 2-D arrays already loaded by the caller
channels = recipe["channels"]
r = rescale(c15 - c13, channels["R"]["clip"]["min_K"], channels["R"]["clip"]["max_K"])
g = rescale(c14 - c11, channels["G"]["clip"]["min_K"], channels["G"]["clip"]["max_K"])
b = rescale(c13, channels["B"]["clip"]["min_K"], channels["B"]["clip"]["max_K"])

plot_rgb(lon, lat, stack_rgb(r, g, b),
         title=recipe["name"], savepath="ash_rgb.png")
```

## Technical Details

### Projection Handling

- All data coordinates use `ccrs.PlateCarree()` as the transform CRS
- Geostationary disk plots use `ccrs.Geostationary(central_longitude=nadir_lon)`
- Convex hull and nadir plots use `ccrs.PlateCarree(central_longitude=-180)` for GOES-West to avoid antimeridian artifacts
- Map features (coastlines, borders, lakes) use Natural Earth 50m resolution shapefiles

### Triangulation Architecture

All triangulation flows through `build_triangulation`, which supports two modes via its parameters:

- **Sparse mode** (`n_points=500`): Subsamples the full coordinate set for lightweight overlays on disk/hull plots
- **Dense mode** (`bounds=(...)` without `n_points`): Triangulates all points within a bounding box for detailed nadir views
- **Combined** (both parameters): Sparse mesh within a spatial region

The returned `mask` array enables data subsetting without recomputing bounding box logic.

### Performance Considerations

- `pcolormesh` is used for scalar fields (handles irregular grids, rasterizes efficiently)
- `imshow` is used for RGB composites (requires regular grid, faster rendering)
- Sparse triangulation with ~500 points renders quickly; full-resolution nadir views should be spatially bounded to avoid overwhelming the renderer
- 50m Natural Earth features are cached after first download

## Error Handling

```python
from goesdatabuilder.utils.plotting import build_triangulation

# Empty arrays
try:
    tri, mask = build_triangulation(np.array([]), np.array([]))
except ValueError as e:
    print(f"Error: {e}")  # "Input coordinate arrays cannot be empty"

# No points within bounds
try:
    tri, mask = build_triangulation(lons, lats, bounds=(0, 1, 0, 1))
except ValueError as e:
    print(f"Error: {e}")  # "No data points found within bounds: ..."

# Too few points after subsampling
try:
    tri, mask = build_triangulation(lons[:2], lats[:2])
except ValueError as e:
    print(f"Error: {e}")  # "Need at least 3 points for triangulation, got 2"
```

## Best Practices

1. **Use `plot_scalar` for all single-field visualizations**, whether CMI bands, band differences, or derived products. The caller controls cmap and norm.
2. **Build RGB composites externally** using `rescale`, `apply_gamma`, and `stack_rgb`, then pass the result to `plot_rgb`. This keeps recipe logic separate from rendering.
3. **Use `make_ir_norm`** for brightness temperature fields to compress warm surface pixels and spread cloud features across the colormap.
4. **Set `central_longitude=-180`** when plotting GOES-West data in PlateCarree to avoid antimeridian wrap artifacts.
5. **Pass `savepath`** to write and close figures automatically in batch workflows. Omit it for interactive notebook use.
6. **Bound nadir tessellations** to small regions (a few degrees) to keep triangle counts manageable for rendering.