"""
plotting.py
-----------
Visualization utilities for geospatial scalar fields, RGB composites,
DQF categorical maps, and Delaunay triangulation diagnostics.

All plotting functions accept plain numpy arrays for coordinates and data,
with no dependency on any particular storage backend (Zarr, NetCDF, etc.).

Coordinate convention:
    lon, lat arrays in degrees on a PlateCarree (equirectangular) grid.
    RGB arrays are (M, N, 3) float32/64 in [0, 1].

Common plotting parameters
--------------------------
The following parameters appear on most plot_* functions and share
the same meaning throughout:

figsize : tuple of int
    Figure dimensions in inches, e.g. (12, 9).
feature_color : str
    Edge color for coastline, border, and lake overlays.
savepath : str or None
    If provided, write the figure to this path and close it.
    If None, the figure is left open for interactive use.
dpi : int
    Resolution in dots per inch when saving.
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors
import matplotlib.tri as mtri
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.geoaxes import GeoAxes
from scipy.spatial import Delaunay, ConvexHull
from typing import Optional, Tuple
from matplotlib.figure import Figure

# The CRS used for all data coordinates passed into plotting functions.
DATA_CRS = ccrs.PlateCarree()

# --------------------------------------------------------------------------
# Module-level defaults
# --------------------------------------------------------------------------

DQF_FLAGS = {
    0: ("#2ecc71", "Good"),
    1: ("#f1c40f", "Conditionally usable"),
    2: ("#e67e22", "Out of range"),
    3: ("#e74c3c", "No value"),
    4: ("#9b59b6", "Focal plane temp exceeded"),
    5: ("#3498db", "Interpolated"),
}

# Default IR colormap applied to brightness-temperature fields.
IR_CMAP = "magma"

# ABI bands whose imagery is best rendered with a grey colormap.
WATER_VAPOR_BANDS = {8, 9, 10}


# ==========================================================================
# Internal helpers
# ==========================================================================

def _add_map_features(
    ax: GeoAxes,
    feature_color: str = "yellow",
    include_lakes: bool = False,
) -> None:
    """Overlay coastlines, borders, and optionally lakes onto *ax*.

    Parameters
    ----------
    ax : GeoAxes
        Target axes with a cartopy projection.
    feature_color : str
        Edge color for all geographic features.
    include_lakes : bool
        Whether to draw lake outlines.
    """
    ax.add_feature(
        cfeature.COASTLINE.with_scale("50m"),
        linewidth=0.5, edgecolor=feature_color,
    )
    ax.add_feature(
        cfeature.BORDERS.with_scale("50m"),
        linewidth=0.3, edgecolor=feature_color, linestyle="--",
    )
    if include_lakes:
        ax.add_feature(
            cfeature.LAKES.with_scale("50m"),
            linewidth=0.2, edgecolor=feature_color, facecolor="none",
        )


def _add_nadir_marker(
    ax: GeoAxes,
    nadir_lon: float,
    nadir_lat: float = 0.0,
    color: str = "red",
) -> None:
    """Draw a crosshair at the sub-satellite (nadir) point.

    Parameters
    ----------
    ax : GeoAxes
        Target axes.
    nadir_lon, nadir_lat : float
        Geographic coordinates of the nadir point in degrees.
    color : str
        Marker color.
    """
    ax.plot(
        nadir_lon, nadir_lat, marker="+", color=color,
        markersize=12, markeredgewidth=2, transform=DATA_CRS,
    )


def _save_and_close(fig: Figure, savepath: Optional[str], dpi: int = 150) -> None:
    """Write *fig* to disk and close it. No-op when *savepath* is None."""
    if savepath:
        fig.savefig(savepath, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved {savepath}")


def _make_fig_ax(
    projection=None,
    figsize: Tuple[int, int] = (12, 9),
    feature_color: str = "yellow",
    include_lakes: bool = False,
) -> Tuple[Figure, GeoAxes]:
    """Create a figure and GeoAxes with standard map overlays.

    Parameters
    ----------
    projection : cartopy.crs.Projection, optional
        Map projection for the axes. Defaults to PlateCarree.
    figsize : tuple
        Figure dimensions in inches.
    feature_color : str
        Edge color for coastlines, borders, and lakes.
    include_lakes : bool
        Whether to draw lake outlines.

    Returns
    -------
    fig : Figure
    ax : GeoAxes
    """
    proj = projection or DATA_CRS
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={"projection": proj})
    ax: GeoAxes  # type narrowing: plt.subplots returns Axes but projection makes it GeoAxes
    _add_map_features(ax, feature_color=feature_color, include_lakes=include_lakes)
    return fig, ax


def _make_dqf_cmap() -> Tuple[mcolors.ListedColormap, mcolors.BoundaryNorm, list]:
    """Build the categorical colormap, norm, and legend patches for DQF data.

    Returns
    -------
    cmap : ListedColormap
    norm : BoundaryNorm
    patches : list[mpatches.Patch]
    """
    colors = [DQF_FLAGS[i][0] for i in range(6)]
    cmap = mcolors.ListedColormap(colors)
    norm = mcolors.BoundaryNorm([0, 1, 2, 3, 4, 5, 6], cmap.N)
    patches = [
        mpatches.Patch(color=DQF_FLAGS[i][0], label=f"{i}: {DQF_FLAGS[i][1]}")
        for i in range(6)
    ]
    return cmap, norm, patches


# ==========================================================================
# Normalization and color helpers
# ==========================================================================

def make_ir_norm(
    data: np.ndarray,
    warm_pct: float = 30.0,
    cmap_share: float = 0.10,
) -> mcolors.FuncNorm:
    """Piecewise-linear norm that compresses warm pixels.

    The warmest *warm_pct* percent of finite values are squeezed into
    the top *cmap_share* fraction of the colormap, while the remaining
    cooler pixels spread across the rest. This keeps land/ocean as a
    uniform dark tone and allocates most color variation to clouds.

    Parameters
    ----------
    data : 2-D array
        Brightness-temperature field in Kelvin.
    warm_pct : float
        Percentage of pixels (by value) considered "warm."
    cmap_share : float
        Fraction of the colormap allocated to the warm cluster.
        E.g. 0.10 gives the warmest pixels the top 10 % of the cmap.

    Returns
    -------
    matplotlib.colors.FuncNorm
    """
    finite = data[np.isfinite(data)]
    vmin = float(np.nanmin(finite))
    vmax = float(np.nanmax(finite))
    bp = float(np.percentile(finite, 100.0 - warm_pct))

    # Guard against degenerate cases where all values are identical
    # or the breakpoint collapses to an endpoint.
    if bp <= vmin:
        bp = vmin + 0.01 * (vmax - vmin)
    if bp >= vmax:
        bp = vmax - 0.01 * (vmax - vmin)

    def forward(x):
        x = np.asarray(x, dtype=float)
        result = np.empty_like(x)
        cold = x <= bp
        result[cold] = (x[cold] - vmin) / (bp - vmin) * (1.0 - cmap_share)
        result[~cold] = (
            (1.0 - cmap_share)
            + (x[~cold] - bp) / (vmax - bp) * cmap_share
        )
        return result

    def inverse(y):
        y = np.asarray(y, dtype=float)
        result = np.empty_like(y)
        cold = y <= (1.0 - cmap_share)
        result[cold] = y[cold] / (1.0 - cmap_share) * (bp - vmin) + vmin
        result[~cold] = (
            (y[~cold] - (1.0 - cmap_share)) / cmap_share * (vmax - bp) + bp
        )
        return result

    return mcolors.FuncNorm((forward, inverse), vmin=vmin, vmax=vmax)


def rescale(arr: np.ndarray, vmin: float, vmax: float) -> np.ndarray:
    """Clip *arr* to [vmin, vmax] then linearly scale to [0, 1].

    Useful for mapping physical-unit channels into RGB-ready values.

    Parameters
    ----------
    arr : np.ndarray
        Input array in physical units.
    vmin, vmax : float
        Clipping bounds. Values outside are clamped before scaling.

    Returns
    -------
    np.ndarray
        Array with values in [0, 1].
    """
    return (np.clip(arr, vmin, vmax) - vmin) / (vmax - vmin)


def apply_gamma(arr: np.ndarray, gamma: float) -> np.ndarray:
    """Apply gamma correction: out = arr^(1/gamma), clamped to [0, 1].

    Parameters
    ----------
    arr : np.ndarray
        Input array, expected in [0, 1] (values outside are clipped).
    gamma : float
        Gamma value. Values < 1 darken, values > 1 brighten.

    Returns
    -------
    np.ndarray
        Gamma-corrected array in [0, 1].
    """
    return np.power(np.clip(arr, 0, 1), 1.0 / gamma)


def stack_rgb(
    r: np.ndarray, g: np.ndarray, b: np.ndarray,
) -> np.ndarray:
    """Stack three 2-D channels into an (M, N, 3) array clipped to [0, 1].

    Parameters
    ----------
    r, g, b : np.ndarray
        2-D arrays of identical shape representing each color channel.

    Returns
    -------
    np.ndarray
        (M, N, 3) array suitable for imshow.
    """
    return np.clip(np.dstack([r, g, b]), 0, 1)


# ==========================================================================
# Triangulation
# ==========================================================================

def build_triangulation(
    lons: np.ndarray,
    lats: np.ndarray,
    n_points: Optional[int] = None,
    bounds: Optional[Tuple[float, float, float, float]] = None,
) -> Tuple[mtri.Triangulation, np.ndarray]:
    """Build a Delaunay triangulation with optional spatial filtering
    and subsampling.

    This is the single entry point for all triangulation in the module.
    Sparse overlays pass *n_points*; dense zoomed views pass *bounds*
    without *n_points*; both can be combined.

    Parameters
    ----------
    lons, lats : 1-D arrays
        Geographic coordinates in degrees (same length).
    n_points : int, optional
        If provided, subsample to approximately this many vertices.
        If None, use all points (after any spatial filtering).
    bounds : (lon_min, lon_max, lat_min, lat_max), optional
        If provided, only triangulate points within this bounding box.

    Returns
    -------
    tri : matplotlib.tri.Triangulation
        Ready for triplot / tripcolor.
    mask : 1-D bool array
        Boolean index into the original *lons*/*lats* arrays showing
        which points passed the spatial filter. When no bounds are
        given this is all True. Use ``data[mask]`` to extract the
        corresponding subset of any co-indexed data array.

    Raises
    ------
    ValueError
        If arrays are empty, mismatched, no points fall within bounds,
        or fewer than 3 vertices remain after filtering/subsampling.

    Examples
    --------
    Sparse overlay for a full-disk plot::

        tri, _ = build_triangulation(lons, lats, n_points=500)
        ax.triplot(tri, transform=DATA_CRS)

    Dense mesh for a nadir zoom with data coloring::

        tri, mask = build_triangulation(lons, lats, bounds=(-139, -135, -2, 2))
        ax.tripcolor(tri, data[mask], cmap="Greys_r", transform=DATA_CRS)
    """
    if len(lons) == 0 or len(lats) == 0:
        raise ValueError("Input coordinate arrays cannot be empty")
    if len(lons) != len(lats):
        raise ValueError("Longitude and latitude arrays must have the same length")

    # -- Spatial filtering --
    if bounds is not None:
        lon_min, lon_max, lat_min, lat_max = bounds
        mask = (
            (lats >= lat_min) & (lats <= lat_max)
            & (lons >= lon_min) & (lons <= lon_max)
        )
        if not np.any(mask):
            raise ValueError(f"No data points found within bounds: {bounds}")
        sub_lons, sub_lats = lons[mask], lats[mask]
    else:
        mask = np.ones(len(lons), dtype=bool)
        sub_lons, sub_lats = lons, lats

    # -- Optional subsampling --
    if n_points is not None:
        step = max(1, len(sub_lons) // n_points)
        idx = np.arange(0, len(sub_lons), step)
        sub_lons, sub_lats = sub_lons[idx], sub_lats[idx]

    if len(sub_lons) < 3:
        raise ValueError(
            f"Need at least 3 points for triangulation, got {len(sub_lons)}"
        )

    # -- Delaunay triangulation and conversion to matplotlib format --
    tri = Delaunay(np.vstack((sub_lons, sub_lats)).T)
    return mtri.Triangulation(sub_lons, sub_lats, tri.simplices), mask


# ==========================================================================
# Scalar / categorical / RGB plotting
#
# All plot_* functions accept the common parameters documented in the
# module docstring: figsize, feature_color, savepath, dpi.
# ==========================================================================

def plot_scalar(
    lon: np.ndarray,
    lat: np.ndarray,
    data: np.ndarray,
    cmap: str,
    title: str,
    label: str = "",
    norm=None,
    figsize: Tuple[int, int] = (12, 9),
    feature_color: str = "yellow",
    savepath: Optional[str] = None,
    dpi: int = 150,
) -> Tuple[Figure, GeoAxes]:
    """Plot any 2-D scalar field with pcolormesh.

    Parameters
    ----------
    lon, lat : 2-D arrays
        Coordinate grids (same shape as *data*).
    data : 2-D array
        Values to render.
    cmap : str or Colormap
        Matplotlib colormap name or instance.
    title : str
        Axes title.
    label : str
        Colorbar label.
    norm : matplotlib.colors.Normalize, optional
        Custom normalization (e.g. from ``make_ir_norm``).

    Returns
    -------
    fig : Figure
    ax : GeoAxes

    See Also
    --------
    Module docstring for common plotting parameters.
    """
    fig, ax = _make_fig_ax(
        figsize=figsize, feature_color=feature_color, include_lakes=True,
    )
    pcm = ax.pcolormesh(lon, lat, data, cmap=cmap, norm=norm, transform=DATA_CRS)
    fig.colorbar(pcm, ax=ax, label=label, shrink=0.7)
    ax.set_title(title)
    _save_and_close(fig, savepath, dpi)
    return fig, ax


def plot_dqf(
    lon: np.ndarray,
    lat: np.ndarray,
    data: np.ndarray,
    title: str = "",
    figsize: Tuple[int, int] = (12, 9),
    feature_color: str = "yellow",
    savepath: Optional[str] = None,
    dpi: int = 150,
) -> Tuple[Figure, GeoAxes]:
    """Plot a Data Quality Flag array with a categorical colormap.

    Uses the six-class GOES ABI DQF scheme defined in ``DQF_FLAGS``.

    Parameters
    ----------
    lon, lat : 2-D arrays
        Coordinate grids (same shape as *data*).
    data : 2-D array
        Integer DQF values (0 through 5).
    title : str
        Axes title.

    Returns
    -------
    fig : Figure
    ax : GeoAxes

    See Also
    --------
    Module docstring for common plotting parameters.
    """
    dqf_cmap, dqf_norm, legend_patches = _make_dqf_cmap()
    fig, ax = _make_fig_ax(
        figsize=figsize, feature_color=feature_color, include_lakes=True,
    )
    ax.pcolormesh(lon, lat, data, cmap=dqf_cmap, norm=dqf_norm, transform=DATA_CRS)
    ax.legend(handles=legend_patches, loc="lower left", fontsize=8, framealpha=0.9)
    ax.set_title(title)
    _save_and_close(fig, savepath, dpi)
    return fig, ax


def plot_rgb(
    lon: np.ndarray,
    lat: np.ndarray,
    rgb: np.ndarray,
    title: str,
    figsize: Tuple[int, int] = (12, 9),
    feature_color: str = "yellow",
    savepath: Optional[str] = None,
    dpi: int = 150,
) -> Tuple[Figure, GeoAxes]:
    """Plot a pre-composed (M, N, 3) RGB image with imshow.

    Assumes lat runs south-to-north (``origin='lower'``).

    Parameters
    ----------
    lon, lat : 2-D or 1-D arrays
        Coordinate arrays. Only min/max are used to set the image extent.
    rgb : (M, N, 3) array
        Float RGB image with values in [0, 1].
    title : str
        Axes title.

    Returns
    -------
    fig : Figure
    ax : GeoAxes

    See Also
    --------
    Module docstring for common plotting parameters.
    """
    fig, ax = _make_fig_ax(
        figsize=figsize, feature_color=feature_color, include_lakes=True,
    )
    ax.imshow(
        rgb,
        extent=[lon.min(), lon.max(), lat.min(), lat.max()],
        origin="lower",
        transform=DATA_CRS,
    )
    ax.set_title(title)
    _save_and_close(fig, savepath, dpi)
    return fig, ax


# ==========================================================================
# Geostationary disk, convex hull, and nadir tessellation plots
#
# All plot_* functions accept the common parameters documented in the
# module docstring: figsize, feature_color, savepath, dpi.
# ==========================================================================

def plot_geostationary_disk(
    lons: np.ndarray,
    lats: np.ndarray,
    nadir_lon: float = -137.2,
    scatter_color: str = "green",
    tri_color: str = "cyan",
    feature_color: str = "red",
    n_tri_points: int = 500,
    show_triangulation: bool = True,
    figsize: Tuple[int, int] = (10, 10),
    title: str = "Geostationary disk",
    savepath: Optional[str] = None,
    dpi: int = 150,
) -> Tuple[Figure, GeoAxes]:
    """Render data point coverage on the native geostationary projection.

    Optionally overlays a sparse Delaunay mesh to illustrate how triangle
    density varies from nadir (compact) to limb (stretched).

    Parameters
    ----------
    lons, lats : 1-D arrays
        Flattened geographic coordinates of every valid pixel.
    nadir_lon : float
        Sub-satellite longitude used as the projection center.
    scatter_color : str
        Color for the raw data point scatter.
    tri_color : str
        Edge color for the sparse triangle overlay.
    n_tri_points : int
        Approximate vertex count for the sparse mesh.
    show_triangulation : bool
        If True, overlay a sparse triangle mesh.
    title : str
        Axes title.

    Returns
    -------
    fig : Figure
    ax : GeoAxes

    See Also
    --------
    Module docstring for common plotting parameters.
    """
    proj = ccrs.Geostationary(central_longitude=nadir_lon)
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={"projection": proj})
    ax: GeoAxes  # plt.subplots returns Axes; projection makes it GeoAxes at runtime
    _add_map_features(ax, feature_color=feature_color)

    ax.scatter(lons, lats, s=0.01, c=scatter_color, transform=DATA_CRS)

    if show_triangulation:
        tri, _ = build_triangulation(lons, lats, n_points=n_tri_points)
        ax.triplot(tri, linewidth=0.4, color=tri_color, alpha=0.6, transform=DATA_CRS)  # type: ignore[arg-type]

    _add_nadir_marker(ax, nadir_lon, color=feature_color)

    ax.set_global()
    ax.set_title(title)
    _save_and_close(fig, savepath, dpi)
    return fig, ax


def plot_convex_hull(
    lons: np.ndarray,
    lats: np.ndarray,
    nadir_lon: float = -137.2,
    central_longitude: float = -180,
    extent: Optional[Tuple[float, float, float, float]] = None,
    hull_color: str = "cyan",
    hull_alpha: float = 0.15,
    tri_color: str = "cyan",
    feature_color: str = "red",
    n_tri_points: int = 500,
    show_triangulation: bool = True,
    figsize: Tuple[int, int] = (14, 10),
    title: str = "Convex hull in geographic coordinates",
    savepath: Optional[str] = None,
    dpi: int = 150,
) -> Tuple[Figure, GeoAxes]:
    """Plot the convex hull of the data footprint in PlateCarree.

    Shows how the geostationary disk maps onto a flat equirectangular
    grid, with an optional sparse triangle overlay for distortion context.

    Parameters
    ----------
    lons, lats : 1-D arrays
        Flattened geographic coordinates of every valid pixel.
    nadir_lon : float
        Sub-satellite longitude for the nadir marker.
    central_longitude : float
        Center meridian for the PlateCarree projection. Set to -180 for
        GOES-West to avoid antimeridian wrap-around artifacts.
    extent : (lon_min, lon_max, lat_min, lat_max), optional
        If provided, crop the map view to this bounding box.
    hull_color : str
        Fill and edge color for the hull polygon.
    hull_alpha : float
        Fill opacity for the hull polygon.
    tri_color : str
        Edge color for the sparse triangle overlay.
    n_tri_points : int
        Approximate vertex count for the sparse mesh.
    show_triangulation : bool
        If True, overlay a sparse triangle mesh.
    title : str
        Axes title.

    Returns
    -------
    fig : Figure
    ax : GeoAxes

    See Also
    --------
    Module docstring for common plotting parameters.
    """
    # Compute the convex hull of the full point cloud
    hull = ConvexHull(np.vstack((lons, lats)).T)
    hull_lons = lons[hull.vertices]
    hull_lats = lats[hull.vertices]

    proj = ccrs.PlateCarree(central_longitude=central_longitude)
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={"projection": proj})
    ax: GeoAxes  # plt.subplots returns Axes; projection makes it GeoAxes at runtime
    _add_map_features(ax, feature_color=feature_color, include_lakes=True)

    # Draw the hull as a semi-transparent polygon
    ax.fill(
        np.append(hull_lons, hull_lons[0]),
        np.append(hull_lats, hull_lats[0]),
        facecolor=hull_color, alpha=hull_alpha, edgecolor=hull_color,
        linewidth=1.0, transform=DATA_CRS,
    )

    if show_triangulation:
        tri, _ = build_triangulation(lons, lats, n_points=n_tri_points)
        ax.triplot(tri, linewidth=0.4, color=tri_color, alpha=0.4, transform=DATA_CRS)  # type: ignore[arg-type]

    _add_nadir_marker(ax, nadir_lon, color=feature_color)

    if extent:
        ax.set_extent(extent, crs=DATA_CRS)
    ax.set_title(title)
    _save_and_close(fig, savepath, dpi)
    return fig, ax


def plot_nadir_tessellation(
    lons: np.ndarray,
    lats: np.ndarray,
    data: Optional[np.ndarray] = None,
    nadir_lon: float = -137.2,
    nadir_lat: float = 0.0,
    half_extent: float = 2.0,
    mesh_color: str = "white",
    data_cmap: str = "Greys_r",
    feature_color: str = "red",
    figsize: Tuple[int, int] = (10, 10),
    title: str = "Delaunay tessellation near nadir",
    savepath: Optional[str] = None,
    dpi: int = 150,
) -> Tuple[Figure, GeoAxes]:
    """Zoomed view of the full-resolution Delaunay mesh around nadir.

    Unlike the sparse overlays in ``plot_geostationary_disk`` and
    ``plot_convex_hull``, this function triangulates every point inside
    the bounding box so individual triangles are visible.

    Parameters
    ----------
    lons, lats : 1-D arrays
        Flattened geographic coordinates of every valid pixel.
    data : 1-D array, optional
        Per-vertex values (same length as *lons*/*lats*) to color-fill
        triangles via tripcolor. Pass None for wireframe only.
    nadir_lon, nadir_lat : float
        Center of the zoom region and location of the nadir marker.
    half_extent : float
        Half-width of the square bounding box in degrees.
    mesh_color : str
        Color of the triangle wireframe edges.
    data_cmap : str
        Colormap used when *data* is provided.
    title : str
        Axes title.

    Returns
    -------
    fig : Figure
    ax : GeoAxes

    See Also
    --------
    Module docstring for common plotting parameters.
    """
    bounds = (
        nadir_lon - half_extent, nadir_lon + half_extent,
        nadir_lat - half_extent, nadir_lat + half_extent,
    )

    # Dense triangulation (no n_points) within the bounding box.
    # The returned mask indexes into the original lons/lats arrays,
    # so data[mask] gives the matching subset.
    tri, mask = build_triangulation(lons, lats, bounds=bounds)

    proj = ccrs.PlateCarree(central_longitude=-180)
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={"projection": proj})
    ax: GeoAxes  # plt.subplots returns Axes; projection makes it GeoAxes at runtime
    _add_map_features(ax, feature_color=feature_color)

    # Optionally fill triangles with co-indexed data values
    if data is not None:
        ax.tripcolor(  # type: ignore[arg-type]
            tri, data[mask], cmap=data_cmap, alpha=0.5, transform=DATA_CRS, # type: ignore
        )

    ax.triplot(tri, linewidth=0.3, color=mesh_color, transform=DATA_CRS)  # type: ignore[arg-type]
    _add_nadir_marker(ax, nadir_lon, nadir_lat, color=feature_color)
    ax.set_extent(
        [bounds[0], bounds[1], bounds[2], bounds[3]], crs=DATA_CRS,
    )
    ax.set_title(title)
    _save_and_close(fig, savepath, dpi)
    return fig, ax