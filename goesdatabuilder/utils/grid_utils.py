# grid_utils.py
# Place in: geolab/utils/grid_utils.py (or wherever shared utilities live)
#
# Used by:
#   - GeostationaryRegridder.__init__ (auto-compute target grid)
#   - GOESPipelineOrchestrator.initialize_regridder (explicit bounds)
#   - GOESZarrStore.initialize_region (monotonicity validation)

import numpy as np


def build_longitude_array(
    lon_min: float,
    lon_max: float,
    resolution: float,
    decimals: int = 4,
) -> np.ndarray:
    """
    Build a 1D longitude array that handles antimeridian crossing.

    When lon_min > lon_max (e.g., 165 to -115 for GOES-West), the range
    crosses the antimeridian (180/-180 boundary). The array is constructed
    in 0-360 space, then converted back to -180/180.

    The resulting array is monotonically increasing in 0-360 space but
    contains a single discontinuity at +/-180 in -180/180 convention.
    Use `is_antimeridian_crossing` to detect this case for downstream
    validation or CF metadata.

    Parameters
    ----------
    lon_min : float
        Western bound in degrees (-180 to 180)
    lon_max : float
        Eastern bound in degrees (-180 to 180)
    resolution : float
        Grid spacing in degrees
    decimals : int
        Decimal places for np.round (default 4)

    Returns
    -------
    np.ndarray
        1D longitude array in -180/180 convention.
    """
    if lon_min <= lon_max:
        return np.round(
            np.arange(lon_min, lon_max + resolution, resolution),
            decimals,
        )

    # Antimeridian crossing: work in 0-360
    lon_min_360 = lon_min % 360
    lon_max_360 = lon_max % 360

    lon_360 = np.round(
        np.arange(lon_min_360, lon_max_360 + resolution, resolution),
        decimals,
    )

    # Convert back to -180/180
    return np.round(((lon_360 + 180) % 360) - 180, decimals)


def is_antimeridian_crossing(lon: np.ndarray) -> bool:
    """
    Detect if a longitude array crosses the antimeridian.

    A crossing is indicated by a large negative jump (> 180 degrees)
    in consecutive values, which occurs when values go from ~+180 to ~-180.

    Parameters
    ----------
    lon : np.ndarray
        1D longitude array in -180/180 convention

    Returns
    -------
    bool
        True if the array crosses the antimeridian.
    """
    if len(lon) < 2:
        return False
    diffs = np.diff(lon)
    return bool(np.any(diffs < -180))


def validate_longitude_monotonic(lon: np.ndarray) -> bool:
    """
    Validate that a longitude array is monotonic, allowing antimeridian crossing.

    For arrays that cross the antimeridian, monotonicity is checked in
    0-360 space instead of -180/180 space.

    Parameters
    ----------
    lon : np.ndarray
        1D longitude array in -180/180 convention

    Returns
    -------
    bool
        True if monotonically increasing (in 0-360 if crossing).
    """
    if is_antimeridian_crossing(lon):
        lon_360 = lon % 360
        return bool(np.all(np.diff(lon_360) > 0))
    return bool(np.all(np.diff(lon) > 0) or np.all(np.diff(lon) < 0))


def radians_to_latlon(x: np.ndarray, y: np.ndarray, projection: dict) -> tuple[np.ndarray, np.ndarray]:
    """
    Convert GOES-R ABI fixed grid coordinates to lat/lon.

    This function takes in the x and y coordinates of the ABI fixed grid in radians
    and the projection parameters of the GOES-R ABI instrument as input. It then
    computes the latitude and longitude of the points in the ABI fixed grid.

    The computation is done by first computing the intermediate variables a_var, b_var,
    and c_var. These variables are then used to compute the radial distance r_s
    from the center of the Earth to the point of interest. The x, y, and z coordinates
    of the point of interest in the ABI fixed grid are then computed using r_s and the
    ABI fixed grid coordinates. Finally, the latitude and longitude of the point of
    interest are computed using the x, y, and z coordinates.

    :return: A tuple of two NumPy arrays containing the latitude and longitude of the points
    in the ABI fixed grid.
    """
    # Get the longitude of the projection origin
    lon_origin = projection["longitude_of_projection_origin"]

    # Get the height of the perspective point above the ellipsoid
    H = projection["perspective_point_height"] + projection["semi_major_axis"]

    # Get the semi-major and semi-minor axes of the ellipsoid
    r_eq = projection["semi_major_axis"]
    r_pol = projection["semi_minor_axis"]

    # Create a 2D grid of the x and y coordinates
    x_2d, y_2d = np.meshgrid(x, y)

    # Compute the longitude of the point of interest
    with np.errstate(all="ignore"):
        lambda_0 = (lon_origin * np.pi) / 180.0
        a_var = np.power(np.sin(x_2d), 2.0) + (
            np.power(np.cos(x_2d), 2.0)
            * (np.power(np.cos(y_2d), 2.0) + (((r_eq * r_eq) / (r_pol * r_pol)) * np.power(np.sin(y_2d), 2.0)))
        )
        b_var = -2.0 * H * np.cos(x_2d) * np.cos(y_2d)
        c_var = (H**2.0) - (r_eq**2.0)
        # ignore warning caused by applying np functions to negative numbers
        r_s = (-1.0 * b_var - np.sqrt((b_var**2) - (4.0 * a_var * c_var))) / (2.0 * a_var)
        s_x = r_s * np.cos(x_2d) * np.cos(y_2d)
        s_y = -r_s * np.sin(x_2d)
        s_z = r_s * np.cos(x_2d) * np.sin(y_2d)

        # Ignore all floating point warnings
        abi_lat = (180.0 / np.pi) * (
            np.arctan(((r_eq * r_eq) / (r_pol * r_pol)) * (s_z / np.sqrt(((H - s_x) * (H - s_x)) + (s_y * s_y))))
        )
        abi_lon = (lambda_0 - np.arctan(s_y / (H - s_x))) * (180.0 / np.pi)

    return abi_lat, abi_lon
