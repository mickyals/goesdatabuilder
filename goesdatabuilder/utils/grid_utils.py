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
