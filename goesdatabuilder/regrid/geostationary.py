import json
import logging
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import xarray as xr
from scipy.spatial import Delaunay

from goesdatabuilder.data.goes import multicloudconstants
from goesdatabuilder.utils.grid_utils import build_longitude_array

if TYPE_CHECKING:
    from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation

logger = logging.getLogger(__name__)


class GeostationaryRegridder:
    """
    Regrids geostationary imager data (x/y radians) to regular lat/lon grid.

    Uses Delaunay triangulation with barycentric interpolation.
    Weights computed once per source/destination grid pair and cached.

    Dask Support:
    ------------
    Accepts both NumPy arrays and xarray DataArrays (including Dask-backed).
    When given Dask arrays, automatically parallelizes across time dimension
    using xr.apply_ufunc. Spatial dimensions (y, x) must NOT be chunked as
    regridding requires full spatial extent.



    Usage:
    ------
        # With NumPy arrays
        regridder = GeostationaryRegridder(...)
        cmi_regridded = regridder.regrid(obs.get_cmi(8).values)

        # With Dask-backed xarray (parallelized automatically)
        obs = GOESMultiCloudObservation(config)  # Dask chunks
        obs.band = 8
        cmi_lazy = obs.cmi  # xr.DataArray with Dask
        cmi_regridded = regridder.regrid(cmi_lazy)  # Still lazy!

    Pipeline:
        GOESMultiCloudObservation → GeostationaryRegridder → GOESZarrStore
    """

    ############################################################################################
    # CLASS CONSTANTS
    ############################################################################################

    # Weight threshold for "direct hit" (no interpolation)
    DIRECT_HIT_THRESHOLD = 0.999

    # Epsilon for integer detection in DQF interpolation
    INTEGER_EPSILON = 1e-6

    # File names for cached weights (updated naming for clarity) at Native resolution
    VERTICES_FILE = "vertices.npy"
    WEIGHTS_FILE = "weights.npy"
    HULL_MASK_FILE = "hull_mask.npy"  # Renamed from mask.npy for clarity
    SOURCE_COORD_MASK_FILE = "source_coord_mask.npy"  # Mask for valid source coordinates (not outer space)
    TARGET_LAT_FILE = "target_lat.npy"  # Target latitude array (antimeridian-safe)
    TARGET_LON_FILE = "target_lon.npy"  # Target longitude array (antimeridian-safe)
    METADATA_FILE = "metadata.json"

    # TODO
    # # valid source coordinates for IDW path
    # SOURCE_LAT_VALID_FILE = "source_lat_valid.npy"
    # SOURCE_LON_VALID_FILE = "source_lon_valid.npy"
    #
    # # subdirectory for derived (coarser) resolution weights
    # COARSE_WEIGHTS_DIR = "derived"
    # COARSE_WEIGHTS_FILE = "coarse_weights.npz"

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(
        self,
        source_x: np.ndarray,
        source_y: np.ndarray,
        projection: dict,
        target_resolution: float = 0.02,
        target_lat: np.ndarray | None = None,
        target_lon: np.ndarray | None = None,
        weights_dir: str | Path | None = None,
        decimals: int = 4,
        reference_band: int = 7,
    ) -> None:
        """
        Initialize the regridder with the source grid and target specification.

        This method takes in the source x and y coordinates, the projection parameters,
        the target resolution, the target latitude and longitude arrays, and the directory
        to save cached weights to.

        :param source_x: 1D array of x coordinates (radians)
        :param source_y: 1D array of y coordinates (radians)
        :param projection: dict with geostationary projection parameters
        :param target_resolution: resolution in degrees (default 0.02)
        :param target_lat: optional explicit lat array (overrides resolution)
        :param target_lon: optional explicit lon array (overrides resolution)
        :param weights_dir: directory to save cached weights after they have been calculated
        :param reference_band: band used to compute weights (default 7)
        """
        self._weights_dir = Path(weights_dir) if weights_dir else None
        self._reference_band = reference_band
        self._cached = False
        self._decimals = decimals
        # TODO self._use_idw = False

        # Convert source x/y to lat/lon
        logger.info("Converting geostationary coordinates to lat/lon...")
        self._source_lat_2d, self._source_lon_2d = self._radians_to_latlon(source_x, source_y, projection)

        # Flatten and store
        self._source_lat_flat = self._source_lat_2d.flatten()
        self._source_lon_flat = self._source_lon_2d.flatten()
        self._source_shape = self._source_lat_2d.shape
        self._source_coord_mask = ~np.isnan(self._source_lat_flat) & ~np.isnan(self._source_lon_flat)

        # Build target grid
        if target_lat is not None and target_lon is not None:
            self._target_lat = target_lat
            self._target_lon = target_lon
        else:
            # Find valid points in the source grid
            valid_lats = self._source_lat_flat[self._source_coord_mask]
            valid_lons = self._source_lon_flat[self._source_coord_mask]

            # Build the target grid
            self._target_lat = np.round(
                np.arange(valid_lats.min(), valid_lats.max() + target_resolution, target_resolution), decimals
            )
            self._target_lon = build_longitude_array(
                float(valid_lons.min()), float(valid_lons.max()), target_resolution, decimals=decimals
            )

        logger.info("Computing interpolation weights (this may take ~40 minutes)...")
        self._vertices, self._weights, self._mask = self._compute_weights()
        logger.info(f"Coverage: {(~self._mask).sum()}/{len(self._mask)} points ({self.coverage_fraction:.2%})")

        if self._weights_dir:
            self.save_weights(self._weights_dir)

    ############################################################################################
    # CLASSMETHODS
    ############################################################################################

    @classmethod
    def from_weights(cls, weights_dir: str | Path) -> "GeostationaryRegridder":
        """
        Load a GeostationaryRegridder instance from a cached weights directory.

        This method is used when the source data is not available, but the
        regridder still needs to be loaded from a cached weights directory.

        It checks if the weights directory exists, and raises a FileNotFoundError if not.
        It then checks if the metadata file exists in the weights directory, and raises a FileNotFoundError if not.
        It loads the metadata from the metadata file, and uses it to set the source shape and target latitude and longitude arrays of the instance.
        It loads the weights from the weights directory into the instance, and logs a message when done.

        :param weights_dir: The path to the cached weights directory
        :type weights_dir: Union[str, Path]
        :return: The loaded GeostationaryRegridder instance
        :rtype: GeostationaryRegridder
        """
        weights_dir = Path(weights_dir)

        # Check if weights directory exists
        if not weights_dir.exists():
            raise FileNotFoundError(f"Weights directory not found: {weights_dir}")

        # Check if metadata file exists in the weights directory
        metadata_path = weights_dir / cls.METADATA_FILE
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

        # Load metadata from the metadata file
        with open(metadata_path) as f:
            metadata = json.load(f)

        # Create a new GeostationaryRegridder instance without source data
        instance = cls.__new__(cls)
        instance._weights_dir = weights_dir
        instance._cached = True

        # TODO  instance._use_idw = False

        # Set the source shape from the metadata
        instance._source_shape = tuple(metadata["source_shape"])
        instance._decimals = metadata.get("decimals", 4)

        # Set reference band
        instance._reference_band = metadata.get("reference_band", 7)

        # Load target coordinate arrays (preferred, antimeridian-safe)
        target_lat_path = weights_dir / cls.TARGET_LAT_FILE
        target_lon_path = weights_dir / cls.TARGET_LON_FILE

        if target_lat_path.exists() and target_lon_path.exists():
            instance._target_lat = np.load(str(target_lat_path))
            instance._target_lon = np.load(str(target_lon_path))
        else:
            raise FileNotFoundError(
                f"Target coordinate files not found in {weights_dir}. "
                f"Re-run save_weights to regenerate the cache with "
                f"{cls.TARGET_LAT_FILE} and {cls.TARGET_LON_FILE}."
            )

            # Load the weights from the weights directory into the instance
        instance.load_weights(weights_dir)
        logger.info(f"Loaded regridder from {weights_dir}")

        return instance

    # @classmethod
    # def from_weights_at_resolution(
    #         cls,
    #         weights_dir: str | Path,
    #         target_lat_resolution: float,
    #         target_lon_resolution: float | None = None,
    #         power: float = 2.0,
    #         search_radius_scale: float = 1.5,
    # ) -> "GeostationaryRegridder":
    #     """
    #     Load a regridder at a coarser resolution without re-triangulating.
    #
    #     Reuses the source grid geometry from an existing native-resolution
    #     weights directory. Builds a sparse IDW weight matrix for the
    #     requested coarser target grid. The first call for a given resolution
    #     runs the KD-tree query and caches the result in
    #     weights_dir/derived/{lat_res}x{lon_res}/. Subsequent calls load
    #     the cached sparse matrix instantly.
    #
    #     The user only needs to specify the desired output resolution at nadir.
    #     The search radius is derived automatically from the cell footprint.
    #
    #     Parameters
    #     ----------
    #     weights_dir : str or Path
    #         Directory containing weights from a prior regridder run.
    #     target_lat_resolution : float
    #         Desired output latitude resolution in degrees. Must be >=
    #         native resolution.
    #     target_lon_resolution : float, optional
    #         Desired output longitude resolution in degrees. Defaults to
    #         target_lat_resolution (isotropic).
    #     power : float
    #         IDW distance power. p=2 is standard inverse-distance-squared.
    #         Higher values give more weight to the nearest source point.
    #         Default 2.0.
    #     search_radius_scale : float
    #         Multiplier on the half-cell-diagonal search radius. 1.5 ensures
    #         all source points within the cell footprint are captured with a
    #         small buffer. Increase to 2.0 for large coarsening factors at
    #         the hull boundary. Default 1.5.
    #     """
    #     import scipy.sparse as sp
    #
    #     weights_dir = Path(weights_dir)
    #     target_lon_resolution = target_lon_resolution or target_lat_resolution
    #
    #     with open(weights_dir / cls.METADATA_FILE) as f:
    #         metadata = json.load(f)
    #
    #     native_lat_res = metadata["target_lat_resolution"]
    #     native_lon_res = metadata["target_lon_resolution"]
    #
    #     if target_lat_resolution < native_lat_res - 1e-9:
    #         raise ValueError(
    #             f"Requested lat resolution {target_lat_resolution} deg is finer than "
    #             f"native {native_lat_res} deg. Use from_weights() for native resolution or generate a new weight file at the requested resolution."
    #         )
    #     if target_lon_resolution < native_lon_res - 1e-9:
    #         raise ValueError(
    #             f"Requested lon resolution {target_lon_resolution} deg is finer than "
    #             f"native {native_lon_res} deg. Use from_weights() for native resolution or generate a new weight file at the requested resolution."
    #         )
    #
    #     # Build instance shell
    #     instance = cls.__new__(cls)
    #     instance._weights_dir = weights_dir
    #     instance._cached = True
    #     instance._use_idw = True
    #     instance._source_shape = tuple(metadata["source_shape"])
    #     instance._decimals = metadata.get("decimals", 4)
    #     instance._reference_band = metadata.get("reference_band", 7)

    #
    #     # Source coord mask needed by _interpolate_2d_idw
    #     instance._source_coord_mask = np.load(str(weights_dir / cls.SOURCE_COORD_MASK_FILE))
    #
    #     # Build coarse target grid from native extent
    #     decimals = metadata.get("decimals", 4)
    #     instance._target_lat = np.round(
    #         np.arange(
    #             metadata["target_lat_min"],
    #             metadata["target_lat_max"] + target_lat_resolution,
    #             target_lat_resolution,
    #         ),
    #         decimals,
    #     )
    #     instance._target_lon = np.round(
    #         np.arange(
    #             metadata["target_lon_min"],
    #             metadata["target_lon_max"] + target_lon_resolution,
    #             target_lon_resolution,
    #         ),
    #         decimals,
    #     )
    #
    #     # Check derived cache
    #     cache_key = f"{target_lat_resolution:.6f}x{target_lon_resolution:.6f}"
    #     derived_dir = weights_dir / cls.COARSE_WEIGHTS_DIR / cache_key
    #
    #     if derived_dir.exists() and (derived_dir / cls.COARSE_WEIGHTS_FILE).exists():
    #         instance._coarse_weights = sp.load_npz(str(derived_dir / cls.COARSE_WEIGHTS_FILE))
    #         instance._coarse_mask = np.load(str(derived_dir / cls.HULL_MASK_FILE))
    #         logger.info(f"Loaded derived weights from {derived_dir}")
    #         return instance
    #
    #     # First call at this resolution -- build IDW weight matrix
    #     logger.info(
    #         f"Building IDW weights for {target_lat_resolution} x {target_lon_resolution} deg resolution. "
    #         f"This will be cached to {derived_dir} for future use..."
    #     )
    #
    #     source_lat_valid = np.load(str(weights_dir / cls.SOURCE_LAT_VALID_FILE))
    #     source_lon_valid = np.load(str(weights_dir / cls.SOURCE_LON_VALID_FILE))
    #
    #     coarse_weights, coarse_mask = cls._build_idw_weights(
    #         source_lat_valid=source_lat_valid,
    #         source_lon_valid=source_lon_valid,
    #         target_lat=instance._target_lat,
    #         target_lon=instance._target_lon,
    #         lat_resolution=target_lat_resolution,
    #         lon_resolution=target_lon_resolution,
    #         power=power,
    #         search_radius_scale=search_radius_scale,
    #     )
    #
    #     instance._coarse_weights = coarse_weights
    #     instance._coarse_mask = coarse_mask
    #
    #     # Cache derived weights
    #     derived_dir.mkdir(parents=True, exist_ok=True)
    #     sp.save_npz(str(derived_dir / cls.COARSE_WEIGHTS_FILE), coarse_weights)
    #     np.save(str(derived_dir / cls.HULL_MASK_FILE), coarse_mask)
    #     np.save(str(derived_dir / cls.TARGET_LAT_FILE), instance._target_lat)
    #     np.save(str(derived_dir / cls.TARGET_LON_FILE), instance._target_lon)
    #
    #     derived_metadata = {
    #         "target_lat_resolution": target_lat_resolution,
    #         "target_lon_resolution": target_lon_resolution,
    #         "target_shape": list(instance.target_shape),
    #         "native_lat_resolution": native_lat_res,
    #         "native_lon_resolution": native_lon_res,
    #         "power": power,
    #         "search_radius_scale": search_radius_scale,
    #         "created_at": datetime.now(UTC).isoformat() + "Z",
    #     }
    #     with open(derived_dir / cls.METADATA_FILE, "w") as f:
    #         json.dump(derived_metadata, f, indent=2)
    #
    #     logger.info(f"Cached derived weights to {derived_dir}")
    #     return instance


    ############################################################################################
    # PROPERTIES
    ############################################################################################

    @property
    def target_lat(self) -> np.ndarray:
        """
        Returns the 1D target latitude array.

        This property returns the 1D target latitude array, which is
        the latitude array of the output grid.

        :return: A 1D numpy array of target latitude values.
        :rtype: np.ndarray
        """
        return self._target_lat

    @property
    def target_lon(self) -> np.ndarray:
        """
        Returns the 1D target longitude array.

        This property returns the 1D target longitude array, which is
        the longitude array of the output grid.

        :return: A 1D numpy array of target longitude values.
        :rtype: np.ndarray
        """
        return self._target_lon

    @property
    def target_shape(self) -> tuple[int, int]:
        """
        Returns the shape of the output grid as a tuple of two integers.

        The first element of the tuple is the number of rows (y-size) of the output grid,
        and the second element is the number of columns (x-size) of the output grid.

        :return: A tuple of two integers, representing the shape of the output grid.
        """
        return len(self._target_lat), len(self._target_lon)

    @property
    def source_shape(self) -> tuple[int, int]:
        """
        Returns the shape of the source grid as a tuple of two integers.

        The first element of the tuple is the number of rows (y-size) of the source grid,
        and the second element is the number of columns (x-size) of the source grid.

        :return: A tuple of two integers, representing the shape of the source grid.
        :rtype: tuple[int, int]
        """
        return self._source_shape

    @property
    def n_target_points(self) -> int:
        """
        Total points in target grid.

        This property returns the total number of points in the target grid.
        It is computed by multiplying the number of latitude points by the number of
        longitude points.

        Returns
        -------
            int: total number of points in the target grid
        """
        return self.target_shape[0] * self.target_shape[1]

    @property
    def n_valid_points(self) -> int:
        """
        Target points inside source convex hull.

        This property returns the number of target points that are inside the
        convex hull of the source grid points. It is computed by counting
        the number of points where the mask is False.

        Returns
        -------
            int: number of target points inside source convex hull
        """
        # TODO
        mask = self._coarse_mask if getattr(self, "_use_idw", False) else self._mask
        return int((~mask).sum())

    @property
    def coverage_fraction(self) -> float:
        """
        Fraction of target grid covered by source data.

        This property returns the fraction of target grid points that are inside
        the convex hull of the source grid points. It is computed by dividing
        the number of valid points by the total number of target points.

        Returns
        -------
            float: fraction of target points that are inside the convex hull of the source points
        """
        return self.n_valid_points / self.n_target_points

    @property
    def has_cached_weights(self) -> bool:
        """
        True if the weights are loaded from the cache, False otherwise.

        The weights are loaded from the cache if the weights directory was specified
        when initializing the instance, and the weights directory contains all
        the required files (vertices.npy, weights.npy, and hull_mask.npy, source_coord_vaLid_mask.npy).

        Returns
        -------
            bool: True if weights are loaded from cache, False otherwise
        """
        return self._cached

    @property
    def weights_dir(self) -> Path | None:
        """
        Directory where weights are cached.

        If set, the regridder will load the weights from this directory
        instead of recomputing them. This can significantly speed up the
        regridding process.

        Returns
        -------
            Optional[Path]: directory where weights are cached (or None if not set)
        """
        return self._weights_dir

    @property
    def direct_hit_fraction(self) -> float:
        """
        Fraction of target points that are direct hits.

        This property computes the fraction of target points that are direct hits.
        A direct hit is defined as a target point where the maximum weight is greater
        than the direct hit threshold.

        Returns
        -------
            float: fraction of target points that are direct hits
        """
        max_weights = self._weights.max(axis=1)
        # A direct hit is a target point where the maximum weight is greater than the direct hit threshold
        direct_hits = (max_weights >= self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        # Calculate the fraction of target points that are direct hits
        return direct_hits.sum() / self.n_valid_points if self.n_valid_points > 0 else 0.0

    @property
    def interpolated_fraction(self) -> float:
        """
        Fraction of target points that require interpolation.

        This property computes the fraction of target points that require interpolation.
        This is done by counting the number of target points where the maximum weight is
        less than or equal to the direct hit threshold and dividing by the total number of
        valid points.

        :return: The fraction of target points that require interpolation
        """
        max_weights = self._weights.max(axis=1)
        interpolated = (max_weights < self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        return interpolated.sum() / self.n_valid_points if self.n_valid_points > 0 else 0.0

    ############################################################################################
    # COORDINATE TRANSFORMS (PRIVATE)
    ############################################################################################

    def _radians_to_latlon(self, x: np.ndarray, y: np.ndarray, projection: dict) -> tuple[np.ndarray, np.ndarray]:
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

    def _compute_native_pixel_weights(self, x: np.ndarray, y: np.ndarray, projection: dict) -> None:
        # TODO: Compute per-pixel quality weights based on viewing zenith angle.
        # Pixels at nadir have ~2.0 km resolution (weight=1.0), degrading toward
        # the Earth limb in all directions (weight->0.0). Weight = cos(VZA),
        # approximated from ABI fixed grid scan angles as cos(x)*cos(y).
        # The projection dict provides sat_height, r_eq, r_pol.
        # Output should be regridded alongside CMI data and stored as a
        # static coordinate array in root or per platform.
        pass

    def _build_source_coords(self) -> np.ndarray:
        """
        Build a (N, 2) array of [lat, lon] from flattened source grid.

        This function takes the flattened source latitude and longitude arrays and
        creates a (N, 2) array of [lat, lon] from the valid points in the source
        grid. The valid points are those where both latitude and longitude are not
        NaN.

        return:
         A (N, 2) array of [lat, lon]
        """
        # Get the valid points from the flattened source grid
        source_lats = self._source_lat_flat[self._source_coord_mask]
        source_lons = self._source_lon_flat[self._source_coord_mask]

        # Build the source coordinates array
        return np.vstack((source_lats, source_lons)).T

    def _build_target_coords(self) -> np.ndarray:
        """
        Build a (M, 2) array of [lat, lon] from target grid.

        This function takes the target latitude and longitude arrays and
        creates a meshgrid of the target points. It then flattens the
        meshgrid and stacks the latitude and longitude arrays on top of each
        other to create a (M, 2) array of [lat, lon].

        :return: A (M, 2) array of [lat, lon]
        """
        lon_grid, lat_grid = np.meshgrid(self._target_lon, self._target_lat)
        # Flatten the meshgrid and stack the latitude and longitude arrays
        target_coords = np.vstack((lat_grid.flatten(), lon_grid.flatten())).T
        return target_coords

    ############################################################################################
    # WEIGHT COMPUTATION (PRIVATE)
    ############################################################################################

    def _compute_weights(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Compute Delaunay triangulation and barycentric weights.

        This function first builds the source and target coordinates from the flattened
        source grid and target grid, respectively. Then, it computes the Delaunay
        triangulation of the source points using the SciPy library. It finds the
        simplices for each target point by searching for the triangle that contains the
        target point. The vertices of the triangle are then used to compute the
        barycentric weights of the target point.

        The barycentric weights are computed using the formula:
            w_i = (x - x_i) / ((x - x_j) * (x - x_k))

        where w_i is the weight of the i-th vertex, x is the target point, and
        x_i, x_j, x_k are the vertices of the triangle.

        The weights are then normalized to sum to 1.

        The function returns the vertices of the triangles, the barycentric weights, and
        a mask indicating which target points have valid source data.

        :return: tuple of vertices, weights, and mask
        """
        # Build source and target coordinates from flattened source grid and target grid
        source_coords = self._build_source_coords()
        target_coords = self._build_target_coords()

        # Compute Delaunay triangulation
        logger.info(f"Building Delaunay triangulation with {len(source_coords)} source points...")
        triangles = Delaunay(source_coords)

        # Find simplices for each target point
        logger.info(f"Finding simplices for {len(target_coords)} target points...")
        simplex = triangles.find_simplex(target_coords)
        vertices = np.take(triangles.simplices, simplex, axis=0)

        # Compute barycentric weights
        d = source_coords.shape[1]
        temp = np.take(triangles.transform, simplex, axis=0)
        delta = target_coords - temp[:, d]
        bary = np.einsum("njk,nk->nj", temp[:, :d, :], delta)

        # Normalize weights to sum to 1
        weights = np.hstack((bary, 1 - bary.sum(axis=1, keepdims=True)))

        # Compute coverage mask
        mask = simplex == -1

        return vertices, weights.astype(np.float32), mask



        # TODO
        # ############################################################################################
        # # NEW: IDW WEIGHT CONSTRUCTION
        # ############################################################################################
        #
        # @staticmethod
        # def _build_idw_weights(
        #         source_lat_valid: np.ndarray,
        #         source_lon_valid: np.ndarray,
        #         target_lat: np.ndarray,
        #         target_lon: np.ndarray,
        #         lat_resolution: float,
        #         lon_resolution: float,
        #         power: float = 2.0,
        #         search_radius_scale: float = 1.5,
        # ) -> tuple:
        #     """
        #     Build a sparse IDW weight matrix mapping source points to a coarse target grid.
        #
        #     For each coarse target cell, finds all source points within a search
        #     radius derived from the cell footprint half-diagonal and weights them
        #     by inverse distance^power. The result is a CSR sparse matrix of shape
        #     (N_coarse, N_source_valid) where each row sums to 1 for covered points.
        #
        #     Parameters
        #     ----------
        #     source_lat_valid : np.ndarray
        #         Latitudes of valid (non-NaN) source points, shape (N_source,).
        #     source_lon_valid : np.ndarray
        #         Longitudes of valid source points, shape (N_source,).
        #     target_lat : np.ndarray
        #         1D coarse target latitude array.
        #     target_lon : np.ndarray
        #         1D coarse target longitude array.
        #     lat_resolution : float
        #         Target latitude cell size in degrees.
        #     lon_resolution : float
        #         Target longitude cell size in degrees.
        #     power : float
        #         IDW power. Default 2.0.
        #     search_radius_scale : float
        #         Multiplier on the half-cell-diagonal. Default 1.5.
        #
        #     Returns
        #     -------
        #     coarse_weights : scipy.sparse.csr_matrix
        #         Shape (N_coarse, N_source_valid).
        #     coarse_mask : np.ndarray
        #         Bool array of shape (N_coarse,). True = no source coverage.
        #     """
        #     from scipy.spatial import cKDTree
        #     import scipy.sparse as sp
        #
        #     source_coords = np.column_stack([source_lat_valid, source_lon_valid])
        #     tree = cKDTree(source_coords)
        #
        #     # Search radius: half-diagonal of the coarse cell with buffer
        #     search_radius = (
        #             np.sqrt((lat_resolution / 2) ** 2 + (lon_resolution / 2) ** 2)
        #             * search_radius_scale
        #     )
        #
        #     lon_grid, lat_grid = np.meshgrid(target_lon, target_lat)
        #     target_coords = np.column_stack([lat_grid.ravel(), lon_grid.ravel()])
        #     N_coarse = len(target_coords)
        #     N_source = len(source_lat_valid)
        #
        #     rows, cols, vals = [], [], []
        #     coarse_mask = np.ones(N_coarse, dtype=bool)
        #
        #     indices_list = tree.query_ball_point(target_coords, r=search_radius)
        #
        #     for i, neighbours in enumerate(indices_list):
        #         if not neighbours:
        #             continue
        #
        #         n_idx = np.array(neighbours)
        #         dists = np.linalg.norm(source_coords[n_idx] - target_coords[i], axis=1)
        #
        #         # Exact hit: target coincides with a source point
        #         exact = dists == 0.0
        #         if exact.any():
        #             hit_idx = n_idx[np.where(exact)[0][0]]
        #             rows.append(i)
        #             cols.append(int(hit_idx))
        #             vals.append(1.0)
        #         else:
        #             idw = 1.0 / (dists ** power)
        #             idw /= idw.sum()
        #             rows.extend([i] * len(n_idx))
        #             cols.extend(n_idx.tolist())
        #             vals.extend(idw.tolist())
        #
        #         coarse_mask[i] = False
        #
        #     coarse_weights = sp.csr_matrix(
        #         (vals, (rows, cols)),
        #         shape=(N_coarse, N_source),
        #     )
        #
        #     return coarse_weights, coarse_mask

    ############################################################################################
    # WEIGHT I/O
    ############################################################################################

    def save_weights(self, weights_dir: str | Path | None = None) -> None:
        """
        Save the precomputed weights, mask, and metadata to a weights directory.

        This function saves the precomputed weights, mask, and metadata to a
        weights directory. If the weights directory is not specified, it will
        use the weights directory that was specified when the instance was
        initialized. If no weights directory was specified when the instance
        was initialized, it will raise a ValueError.

        The weights directory should contain the following files:
            - vertices.npy: a (N, 2) array containing the coordinates of the source points
            - weights.npy: a (N, 3) array containing the barycentric weights for each target point
            - hull_mask.npy: a (M,) array containing a boolean mask indicating which target points have valid source data
            - source_coord_mask: a (M,) array containing a boolean mask indicating which source points have valid locations (i.e not outer space)

        Parameters
        ----------
            weights_dir: directory to save the weights, mask, and metadata (optional)
        """
        if weights_dir is None:
            # If no weights directory is specified, use the weights directory that was specified when the instance was initialized
            weights_dir = self._weights_dir
        if weights_dir is None:
            # If no weights directory was specified when the instance was initialized, raise a ValueError
            raise ValueError("weights_dir must be specified")

        weights_dir = Path(weights_dir)
        weights_dir.mkdir(parents=True, exist_ok=True)

        # Save the precomputed vertices, weights, and mask to the weights directory
        np.save(weights_dir / self.VERTICES_FILE, self._vertices)
        np.save(weights_dir / self.WEIGHTS_FILE, self._weights)
        np.save(weights_dir / self.HULL_MASK_FILE, self._mask)
        np.save(weights_dir / self.SOURCE_COORD_MASK_FILE, self._source_coord_mask)

        # Save target coordinate arrays (required for antimeridian-safe reconstruction)
        np.save(weights_dir / self.TARGET_LAT_FILE, self._target_lat)
        np.save(weights_dir / self.TARGET_LON_FILE, self._target_lon)

        # TODO
        # # valid source coordinates for IDW path
        # np.save(weights_dir / self.SOURCE_LAT_VALID_FILE, self._source_lat_flat[self._source_coord_mask])
        # np.save(weights_dir / self.SOURCE_LON_VALID_FILE, self._source_lon_flat[self._source_coord_mask])

        # Save the metadata to the weights directory
        self._save_metadata(weights_dir)

        logger.info(f"Saved weights to {weights_dir}")

    def load_weights(self, weights_dir: str | Path) -> None:
        """
        Load the precomputed weights from a weights directory.

        This function loads the precomputed weights from a weights directory.
        The weights directory should contain the following files:
            - vertices.npy: a (N, 2) array containing the coordinates of the source points
            - weights.npy: a (N, 3) array containing the barycentric weights for each target point
            - hull_mask.npy: a (M,) array containing a boolean mask indicating which target points have valid source data
            - source_coord_mask: a (M,) array containing a boolean mask indicating which source points have valid locations (i.e not outer space)


        Parameters
        ----------
            weights_dir: directory containing the vertices, weights, and mask files
        """
        weights_dir = Path(weights_dir)

        # Load the precomputed vertices, weights, and mask from the weights directory
        self._vertices = np.load(str(weights_dir / self.VERTICES_FILE))
        self._weights = np.load(str(weights_dir / self.WEIGHTS_FILE))
        self._mask = np.load(str(weights_dir / self.HULL_MASK_FILE))
        self._source_coord_mask = np.load(str(weights_dir / self.SOURCE_COORD_MASK_FILE))

        # Load target coordinate arrays if available (antimeridian-safe)
        target_lat_path = weights_dir / self.TARGET_LAT_FILE
        target_lon_path = weights_dir / self.TARGET_LON_FILE
        if target_lat_path.exists() and target_lon_path.exists():
            self._target_lat = np.load(str(target_lat_path))
            self._target_lon = np.load(str(target_lon_path))

        # Update the cached status and weights directory
        self._cached = True
        self._weights_dir = weights_dir

        logger.info(f"Loaded weights from {weights_dir}")

    def _save_metadata(self, weights_dir: Path) -> None:
        """
        Save metadata JSON with grid info.

        This function saves a JSON file containing information about the
        target grid, such as its shape, resolution, and coverage fraction.
        The file is saved to the specified weights directory.

        The metadata dictionary contains the following keys:
            - source_shape: the shape of the source grid
            - target_shape: the shape of the target grid
            - target_lat_min: the minimum latitude of the target grid
            - target_lat_max: the maximum latitude of the target grid
            - target_lon_min: the minimum longitude of the target grid
            - target_lon_max: the maximum longitude of the target grid
            - target_lat_resolution: the resolution of the latitude of the target grid
            - target_lon_resolution: the resolution of the longitude of the target grid
            - n_target_points: the total number of target points
            - n_valid_points: the number of target points with valid data
            - coverage_fraction: the fraction of target points with valid data
            - direct_hit_fraction: the fraction of target points that are direct hits
            - interpolated_fraction: the fraction of target points that require interpolation
            - reference_band: the band used to compute the weights
            - created_at: the timestamp of when the weights were created
        """
        metadata = {
            # The shape of the source grid
            "source_shape": list(self._source_shape),
            # The shape of the target grid
            "target_shape": list(self.target_shape),
            # The minimum latitude of the target grid
            "target_lat_min": float(self._target_lat.min()),
            # The maximum latitude of the target grid
            "target_lat_max": float(self._target_lat.max()),
            # The minimum longitude of the target grid
            "target_lon_min": float(self._target_lon.min()),
            # The maximum longitude of the target grid
            "target_lon_max": float(self._target_lon.max()),
            # The resolution of the latitude of the target grid
            "target_lat_resolution": float(np.abs(np.diff(self._target_lat).mean())),
            # The resolution of the longitude of the target grid
            "target_lon_resolution": float(np.abs(np.diff(self._target_lon).mean())),

            # TODO
            # # explicit native resolution for from_weights_at_resolution validation
            # "native_lat_resolution": float(np.abs(np.diff(self._target_lat).mean())),
            # "native_lon_resolution": float(np.abs(np.diff(self._target_lon).mean())),

            # The number of positions after the decimal to keep
            "decimals": self._decimals,
            # The total number of target points
            "n_target_points": self.n_target_points,
            # The number of target points with valid data
            "n_valid_points": self.n_valid_points,
            # The fraction of target points with valid data
            "coverage_fraction": self.coverage_fraction,
            # The fraction of target points that are direct hits
            "direct_hit_fraction": self.direct_hit_fraction,
            # The fraction of target points that require interpolation
            "interpolated_fraction": self.interpolated_fraction,
            # The band used to compute the weights
            "reference_band": self._reference_band,
            # The timestamp of when the weights were created
            "created_at": datetime.now(UTC).isoformat() + "Z",
        }

        with open(weights_dir / self.METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=2)

    def _validate_cached_weights(self, weights_dir: Path) -> bool:
        """
        Check if cached weights exist and are compatible with the current target grid.

        This function checks if all required files exist in the weights
        directory. If any of the files are missing, it returns False.

        It then attempts to load the metadata file and checks if the
        cached grid shape matches the expected shape. If the shapes don't
        match, it returns False.

        If any errors occur while loading the metadata file, it returns False.

        If all checks pass, it returns True.
        """
        required_files = [
            # The vertices file contains the coordinates of the source points
            self.VERTICES_FILE,
            # The weights file contains the barycentric weights for each target point
            self.WEIGHTS_FILE,
            # The mask file contains a boolean mask indicating which target points have valid data
            self.HULL_MASK_FILE,
            # Target coordinate arrays (antimeridian-safe reconstruction)
            self.SOURCE_COORD_MASK_FILE,
            self.TARGET_LAT_FILE,
            self.TARGET_LON_FILE,
            # The metadata file contains information about the cached grid, such as its shape and coverage fraction
            self.METADATA_FILE,
        ]

        # Check if all required files exist
        missing = [f for f in required_files if not (weights_dir / f).exists()]
        if missing:
            logger.warning(f"Missing cached weight files in {weights_dir}: {missing}")
            return False

        try:
            # Load the metadata file
            with open(weights_dir / self.METADATA_FILE) as f:
                metadata = json.load(f)

            # Check if the cached grid shape matches the expected shape
            cached_lat = np.load(str(weights_dir / self.TARGET_LAT_FILE))
            cached_lon = np.load(str(weights_dir / self.TARGET_LON_FILE))
            cached_shape = (len(cached_lat), len(cached_lon))
            if tuple(metadata["target_shape"]) != cached_shape:
                logger.warning(
                    f"Metadata target_shape {metadata['target_shape']} doesn't match cached arrays {cached_shape}"
                )
                return False

            # If all checks pass, return True
            return True

        except (json.JSONDecodeError, KeyError) as e:
            # If any errors occur while loading the metadata file, log a warning and return False
            logger.warning(f"Invalid metadata file: {e}")
            return False

    ############################################################################################
    # REGRIDDING - CMI (CONTINUOUS) - CORE NUMPY FUNCTIONS
    ############################################################################################

    def _interpolate_2d(self, data: np.ndarray) -> np.ndarray:
        """
        Interpolate single 2D array using barycentric weights.

        This is the core NumPy function called by both NumPy and Dask paths.

        Input: (y, x) array
        Output: (lat, lon) array
        """
        # Flatten the source data array into a 1D array
        # This is done to simplify the indexing and interpolation
        data_flat = data.flatten()

        # Filter the source data array to only include the valid source points
        data_valid = data_flat[self._source_coord_mask]

        # Interpolate the data using barycentric weights
        # This is done by computing a weighted sum of the values at the triangle vertices
        interpolated = np.einsum("nj,nj->n", np.take(data_valid, self._vertices), self._weights)

        # Set the interpolated values to NaN for points that are outside the hull
        interpolated[self._mask] = np.nan

        # Reshape the interpolated array to the target grid shape
        return interpolated.reshape(self.target_shape)


    # TODO
    # def _interpolate_2d_idw(self, data: np.ndarray) -> np.ndarray:
    #     """
    #     IDW interpolation. Coarse resolution path. Input (y,x), output (lat,lon).
    #
    #     Uses the precomputed sparse weight matrix from from_weights_at_resolution.
    #     """
    #     data_flat = data.flatten()
    #     data_valid = data_flat[self._source_coord_mask]
    #     interpolated = np.array(self._coarse_weights @ data_valid).flatten()
    #     interpolated[self._coarse_mask] = np.nan
    #     return interpolated.reshape(self.target_shape)

    def regrid(self, data: np.ndarray | xr.DataArray, rechunk: bool = True) -> np.ndarray | xr.DataArray:
        """
        Regrid continuous data (CMI) using barycentric interpolation.

        This function supports both NumPy arrays and xarray DataArrays (including Dask-backed).
        If the input is a Dask array, it automatically parallelizes the regridding across the time dimension.

        Parameters
        ----------
            data: (y, x) or (time, y, x) source array (NumPy or xarray)
            rechunk: If True and the data is chunked along spatial dims, automatically rechunk.
                    If False and the data is chunked along spatial dims, raise an error.

        Returns
        -------
            (lat, lon) or (time, lat, lon) regridded array (same type as input)
        """
        # TODO
        core_fn = self._interpolate_2d_idw if self._use_idw else self._interpolate_2d

        # Handle xarray DataArray
        if isinstance(data, xr.DataArray):
            # Call _regrid_xarray function if input is xarray DataArray
            # This function will check for spatial chunking and rechunk the data if necessary
            # Then, it will use xr.apply_ufunc to apply the _interpolate_2d function to the data
            # Finally, it will assign the coordinates to the regridded DataArray
            return self._regrid_xarray(data, rechunk=rechunk, core_fn=core_fn)

        # Handle NumPy array
        elif isinstance(data, np.ndarray):
            # Call _regrid_numpy function if input is NumPy array
            # This function will loop over the time dimension and call the _interpolate_2d function on each 2D slice
            # The regridded 2D slices will then be stacked along the time dimension to form the final 3D array
            return self._regrid_numpy(data, core_fn=core_fn)

        else:
            # Raise TypeError if input is not a NumPy array or xarray DataArray
            raise TypeError(f"Input must be np.ndarray or xr.DataArray, got {type(data)}")

    def _regrid_numpy(self, data: np.ndarray, core_fn= None) -> np.ndarray:
        """
        Regrid NumPy array with serial processing (i.e., no parallelization).

        This function takes in a 2D or 3D NumPy array and regrids it to the target spatial resolution.
        If the input array has 3 dimensions (time, y, x), it loops over the time dimension and calls the _interpolate_2d function on each 2D slice.
        The regridded 2D slices are then stacked along the time dimension to form the final 3D array.

        If the input array has 2 dimensions (y, x), it calls the _interpolate_2d function directly on the input array.

        The function raises a ValueError if the input array does not have 2 or 3 dimensions.
        """
        # TODO
        if core_fn is None:
            core_fn = self._interpolate_2d

        if data.ndim == 3:
            # Get number of time steps
            n_time = data.shape[0]

            # Initialize regridded array with same shape as input, but with target spatial resolution
            regridded = np.empty((n_time, *self.target_shape), dtype=data.dtype)

            # Loop over time dimension and regrid each 2D slice
            # TODO
            for t in range(n_time):
                regridded[t] = core_fn(data[t])

            # Return regridded 3D array
            return regridded
        # TODO
        elif data.ndim == 2:
            # Call _interpolate_2d function directly on input array
            return core_fn(data)

        else:
            # Raise ValueError if input array does not have 2 or 3 dimensions
            raise ValueError(f"Input must be 2D or 3D, got shape {data.shape}")

    @staticmethod
    def _is_chunked(data: xr.DataArray, dim: str) -> bool:
        """Return True iff the data array is chunked on the dim dimension."""
        return dim in data.dims and data.chunks and len(data.chunksizes.get(dim, [None])) > 1

    def _regrid_xarray(self, data: xr.DataArray, rechunk: bool = True, core_fn=None) -> xr.DataArray:
        """
        Regrid xarray DataArray from (y, x) to (lat, lon) with Dask support.

        Regridding requires full spatial extent in memory. If data is chunked
        along spatial dimensions (y, x), this method will either rechunk
        automatically or raise an error depending on the rechunk parameter.

        Parameters
        ----------
        data : xr.DataArray
            Input data with dimensions including 'y' and 'x'
        rechunk : bool, default True
            If True, automatically rechunk spatial dims to full extent.
            If False, raise ValueError when spatial chunking is detected.

        Returns
        -------
        xr.DataArray
            Regridded data with 'lat' and 'lon' replacing 'y' and 'x'
        """
        # TODO
        if core_fn is None:
            core_fn = self._interpolate_2d

        # Check if data is already regridded
        if data.shape[-2:] == self.target_shape:
            return data

        # Handle spatial chunking
        spatial_dims = {"y", "x"}
        chunked_spatial = [dim for dim in spatial_dims if self._is_chunked(data, dim)]

        if chunked_spatial:
            if rechunk:
                warnings.warn(
                    f"Rechunking spatial dimensions {chunked_spatial} to full extent. This may increase memory usage.",
                    UserWarning,
                )
                chunks = {dim: -1 if dim in spatial_dims else "auto" for dim in data.dims}
                data = data.chunk(chunks)
            else:
                raise ValueError(f"Data is chunked along {chunked_spatial}. Set rechunk=True to fix automatically.")

        # Apply regridding (parallelizes across non-spatial dims like time)
        regridded = xr.apply_ufunc(
            core_fn,
            data,
            input_core_dims=[["y", "x"]],
            output_core_dims=[["lat", "lon"]],
            exclude_dims={"y", "x"},
            dask="parallelized",
            output_dtypes=[data.dtype],
            dask_gufunc_kwargs={"output_sizes": {"lat": self.target_shape[0], "lon": self.target_shape[1]}},
        )

        # Assign target coordinates
        regridded = regridded.assign_coords({"lat": self.target_lat, "lon": self.target_lon})

        return regridded

    def regrid_batch(self, data: dict[int, np.ndarray | xr.DataArray]) -> dict[int, np.ndarray | xr.DataArray]:
        """
        Regrid multiple bands efficiently.

        This function takes in a dictionary of bands and their corresponding respective arrays (NumPy or xarray).
        It then regrids each array to the target spatial resolution and returns a new dictionary with the regridded arrays.

        The input dictionary should have integer keys (band number) and values that are either NumPy arrays or xarray DataArrays.
        The function will loop over the items in the dictionary and call the .regrid() method on each value.

        The .regrid() method will take in a single array (NumPy or xarray) and regrid it to the target spatial resolution.
        The regridded array will then be added to a new dictionary with the same key as the input dictionary.

        Parameters
        ----------
            data: dict of band number to array (NumPy or xarray)

        Returns
        -------
            dict of band number to regridded array (same type as input)
        """
        regridded_data = {}
        for band, arr in data.items():
            # Call .regrid() on each array and add to new dictionary
            regridded_data[band] = self.regrid(arr)
        return regridded_data

    ############################################################################################
    # REGRIDDING - DQF (CATEGORICAL) - CORE NUMPY FUNCTIONS
    ############################################################################################

    def _classify_dqf_2d(self, dqf: np.ndarray) -> np.ndarray:
        """
        Classify DQF for each target point.

        Regridding logic:
            - Direct hit (max weight >= DIRECT_HIT_THRESHOLD): preserve source DQF
            - Interpolated to integer (all sources same quality): preserve that DQF
            - Interpolated to float (mixed sources): DQF = 5 (interpolated)
            - Interpolated to NaN (NaN vertex source in hull): DQF = 6 (nan_source)
            - Outside hull: DQF = 3 (no_value)

        Input: (y, x) DQF array (uint8)
        Output: (lat, lon) DQF array (uint8)
        """
        dqf_flat = dqf.flatten()

        dqf_valid = dqf_flat[self._source_coord_mask]

        interpolated_dqf = np.einsum("nj,nj->n", np.take(dqf_valid, self._vertices).astype(np.float32), self._weights)

        dqf_out = np.full(len(self._mask), multicloudconstants.DQF_NO_VALUE, dtype=np.uint8)

        # Direct hits
        max_weights = self._weights.max(axis=1)
        direct_hit_mask = (max_weights >= self.DIRECT_HIT_THRESHOLD) & (~self._mask)

        dominant_vertex_idx = self._weights.argmax(axis=1)
        dominant_vertices = self._vertices[np.arange(len(self._vertices)), dominant_vertex_idx]
        with warnings.catch_warnings():
            # ignore warning that nan values cannot be cast to uint8
            # the nan values in dqf_out will be filled in lower down
            warnings.simplefilter("ignore")
            dqf_out[direct_hit_mask] = dqf_valid[dominant_vertices[direct_hit_mask]]

        # Interpolated points (not direct hits, inside hull)
        interpolated_mask = (max_weights < self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        interpolated_values = interpolated_dqf[interpolated_mask]

        # Separate NaN from valid interpolated values
        is_nan = np.isnan(interpolated_values)
        is_integer = np.abs(interpolated_values - np.round(interpolated_values)) < self.INTEGER_EPSILON

        # All sources same quality -> preserve DQF
        integer_indices = np.where(interpolated_mask)[0][~is_nan & is_integer]
        dqf_out[integer_indices] = np.round(interpolated_dqf[integer_indices]).astype(np.uint8)

        # Mixed sources -> DQF_INTERPOLATED
        float_indices = np.where(interpolated_mask)[0][~is_nan & ~is_integer]
        dqf_out[float_indices] = multicloudconstants.DQF_INTERPOLATED

        # NaN from vertex weights inside hull -> DQF_NAN_SOURCE
        nan_hull_indices = np.where(interpolated_mask)[0][is_nan]
        dqf_out[nan_hull_indices] = multicloudconstants.DQF_NAN_SOURCE

        return dqf_out.reshape(self.target_shape)

    # TODO
    # def _classify_dqf_2d_idw(self, dqf: np.ndarray) -> np.ndarray:
    #     """
    #     IDW DQF classification. Coarse resolution path.
    #
    #     Takes the distance-weighted rounded mean of contributing source flags.
    #     Conservative: the weighted mean naturally propagates higher flag values
    #     when bad-quality source points are nearby. Points outside the search
    #     radius receive dqf_no_value.
    #     """
    #     dqf_flat = dqf.flatten()
    #     dqf_valid = dqf_flat[self._source_coord_mask].astype(np.float32)
    #     interpolated = np.array(self._coarse_weights @ dqf_valid).flatten()
    #     dqf_out = np.round(interpolated).astype(np.uint8)
    #     dqf_out[self._coarse_mask] = self._dqf_no_value
    #     return dqf_out.reshape(self.target_shape)

    def regrid_dqf(self, dqf: np.ndarray | xr.DataArray, rechunk: bool = True) -> np.ndarray | xr.DataArray:
        """
        Regrid categorical DQF data to the target spatial resolution.

        This function takes in a 2D or 3D array of DQF values and regrids it to the target spatial resolution.
        The regridding logic is as follows:
            - Direct hit (max weight ≥ 0.999): preserve source DQF
            - Interpolated to integer (all sources same): preserve that DQF
            - Interpolated to float (mixed sources): DQF = 5 (interpolated)
            - Outside convex hull: DQF = 3 (no value)

        Parameters
        ----------
            dqf: (y, x) or (time, y, x) source DQF array (uint8)
            rechunk: If True, automatically rechunk spatial dims if needed

        Returns
        -------
            (lat, lon) or (time, lat, lon) regridded DQF (uint8)
        """
        # Check if input is a DataArray (xarray)
        # If so, call the _regrid_dqf_xarray function
        # This function checks for spatial chunking and rechunks if necessary
        # Then, it calls the _classify_dqf_2d function on each 2D or 3D slice

        # TODO
        core_fn = self._classify_dqf_2d_idw if self._use_idw else self._classify_dqf_2d
        if isinstance(dqf, xr.DataArray):
            return self._regrid_dqf_xarray(dqf, rechunk=rechunk, core_fn=core_fn)

        # Check if input is a NumPy array
        # If so, call the _regrid_dqf_numpy function
        # This function checks the number of dimensions and calls the _classify_dqf_2d function accordingly
        elif isinstance(dqf, np.ndarray):
            return self._regrid_dqf_numpy(dqf, core_fn=core_fn)

        else:
            raise TypeError(f"Input must be np.ndarray or xr.DataArray, got {type(dqf)}")

    def _regrid_dqf_numpy(self, dqf: np.ndarray, core_fn = None) -> np.ndarray:
        """
        Regrid a 2D or 3D NumPy array of DQF values to the target spatial resolution.

        If the input array has 3 dimensions (time, y, x), it loops over the time dimension and calls the _classify_dqf_2d function on each 2D slice.
        The regridded 2D slices are then stacked along the time dimension to form the final 3D array.

        If the input array has 2 dimensions (y, x), it calls the _classify_dqf_2d function directly on the input array.

        The function raises a ValueError if the input array does not have 2 or 3 dimensions.
        """
        # TODO
        if core_fn is None:
            core_fn = self._classify_dqf_2d

        if dqf.ndim == 3:
            # Get number of time steps
            n_time = dqf.shape[0]

            # Initialize regridded array with same shape as input, but with target spatial resolution
            regridded = np.empty((n_time, *self.target_shape), dtype=np.uint8)

            # Loop over time dimension and regrid each 2D slice
            for t in range(n_time):
                regridded[t] = core_fn(dqf[t])

            # Return regridded 3D array
            return regridded

        elif dqf.ndim == 2:
            # Call _classify_dqf_2d function directly on input array
            return core_fn(dqf)

        else:
            # Raise ValueError if input array does not have 2 or 3 dimensions
            raise ValueError(f"Input must be 2D or 3D, got shape {dqf.shape}")

    def _regrid_dqf_xarray(self, dqf: xr.DataArray, rechunk: bool = True, core_fn = None) -> xr.DataArray:
        """
        Regrid DQF xarray DataArray with Dask support.

        This function takes in a DQF DataArray and regrids it to the target spatial resolution.
        It checks for spatial chunking and rechunks the data if necessary.
        Then, it uses xr.apply_ufunc to apply the _classify_dqf_2d function to the data.
        Finally, it assigns the coordinates to the regridded DataArray.

        Parameters
        ----------
        dqf : xr.DataArray
            DQF DataArray to regrid.
        rechunk : bool, optional
            If True, rechunk the data to the full spatial extent if necessary.
            If False, raise an error if the data is chunked.

        Returns
        -------
        xr.DataArray
            Regridded DQF DataArray with the target spatial resolution.
        """
        # TODO
        if core_fn is None:
            core_fn = self._classify_dqf_2d

        # Check if data is already regridded
        if dqf.shape[-2:] == self.target_shape:
            return dqf

        # Check for spatial chunking
        spatial_dims = {"y", "x"}
        chunked_spatial = [dim for dim in spatial_dims if self._is_chunked(dqf, dim)]

        if chunked_spatial:
            # If rechunk is True, rechunk to full spatial extent
            if rechunk:
                warnings.warn(
                    f"DQF is chunked along spatial dimensions {chunked_spatial}. "
                    f"Rechunking to full spatial extent for regridding.",
                    UserWarning,
                )
                chunks = {dim: -1 if dim in spatial_dims else "auto" for dim in dqf.dims}
                dqf = dqf.chunk(chunks)
            # If rechunk is False, raise an error if data is chunked
            else:
                raise ValueError(
                    f"DQF is chunked along spatial dimensions {chunked_spatial}. Set rechunk=True to fix this."
                )

        # Apply DQF classification using apply_ufunc
        regridded = xr.apply_ufunc(
            core_fn,
            dqf,
            input_core_dims=[["y", "x"]],
            output_core_dims=[["lat", "lon"]],
            exclude_dims={"y", "x"},
            dask="parallelized",
            output_dtypes=[np.uint8],
            dask_gufunc_kwargs={"output_sizes": {"lat": self.target_shape[0], "lon": self.target_shape[1]}},
        )

        # Assign coordinates
        regridded = regridded.assign_coords({"lat": self.target_lat, "lon": self.target_lon})

        return regridded

    def regrid_dqf_batch(self, dqf: dict[int, np.ndarray | xr.DataArray]) -> dict[int, np.ndarray | xr.DataArray]:
        """
        Regrid multiple DQF bands.

        Parameters
        ----------
        dqf : dict[int, Union[np.ndarray, xr.DataArray]]
            Dictionary of DQF bands to regrid.

        Returns
        -------
        dict[int, Union[np.ndarray, xr.DataArray]]
            Dictionary of regridded DQF bands.
        """
        return {band: self.regrid_dqf(arr) for band, arr in dqf.items()}

    ############################################################################################
    # FULL OBSERVATION REGRIDDING
    ############################################################################################

    def regrid_observation(
        self, cmi_data: dict[int, np.ndarray | xr.DataArray], dqf_data: dict[int, np.ndarray | xr.DataArray]
    ) -> tuple[dict, dict]:
        """
        Regrid CMI and DQF dicts from GOESMultiCloudObservation.

        This function takes in CMI and DQF data from a single observation
        and regrids them to the target spatial resolution. The regridded data
        is then packaged into a tuple of two dictionaries, one for CMI and one
        for DQF, which are ready for insertion into a GOESZarrStore.

        Parameters
        ----------
            cmi_data: dict of CMI data from a single observation
            dqf_data: dict of DQF data from a single observation

        Returns
        -------
            tuple of two dicts: (regridded_cmi, regridded_dqf)
        """
        # Regrid CMI data
        regridded_cmi = self.regrid_batch(cmi_data)

        # Regrid DQF data
        regridded_dqf = self.regrid_dqf_batch(dqf_data)

        return regridded_cmi, regridded_dqf

    def regrid_to_observation_dict(
        self, obs: "GOESMultiCloudObservation", time_idx: int = 0, bands: list[int] | None = None
    ) -> dict:
        """
        Extract, regrid, and package single observation for GOESZarrStore.

        This method calls .values on CMI/DQF, triggering computation.
        For lazy processing, use regrid() directly on DataArrays.

        Args:
            obs (GOESMultiCloudObservation): Observation to extract from
            time_idx (int): Index of the time dimension to extract (default: 0)
            bands (list[int], optional): List of bands to extract (default: [1, 2, ..., 16])

        Returns
        -------
            dict: A dictionary containing the extracted observation data
        """
        if bands is None:
            bands = list(range(1, 17))

        obs_single = obs.isel_time(time_idx)

        # Get CMI and DQF - these trigger computation via .values
        cmi_data = {band: obs_single.get_cmi(band).values for band in bands}
        dqf_data = {band: obs_single.get_dqf(band).values for band in bands}

        # Regrid
        cmi_regridded, dqf_regridded = self.regrid_observation(cmi_data, dqf_data)

        return {
            # Timestamp
            "timestamp": obs_single.time.values[0],
            # Auxiliary coordinates
            "platform_id": obs_single.platform_id.values[0],
            "scan_mode": obs_single.scan_mode.values[0],
            # Regridded CMI and DQF
            "cmi_data": cmi_regridded,
            "dqf_data": dqf_regridded,
        }

    ############################################################################################
    # DIAGNOSTICS
    ############################################################################################

    def weight_statistics(self) -> dict:
        """
        Compute statistics on the weight distribution.

        Returns a dict with the following keys:
            - 'max_weight_mean': mean of max weights for valid target points
            - 'max_weight_std': standard deviation of max weights for valid target points
            - 'min_weight_mean': mean of min weights for valid target points
            - 'min_weight_std': standard deviation of min weights for valid target points
            - 'direct_hit_fraction': fraction of target points that are direct hits
            - 'interpolated_fraction': fraction of target points that are interpolated
            - 'coverage_fraction': fraction of target points that have valid source data
        """

        # TODO
        if self._use_idw:
            return {
                "method": "idw",
                "coverage_fraction": self.coverage_fraction,
                "n_valid_points": self.n_valid_points,
                "n_target_points": self.n_target_points,
            }

        # Compute max and min weights for valid target points
        max_weights = self._weights.max(axis=1)
        min_weights = self._weights.min(axis=1)
        valid = ~self._mask

        return {
            # Mean of max weights for valid target points
            "max_weight_mean": float(max_weights[valid].mean()),
            # Standard deviation of max weights for valid target points
            "max_weight_std": float(max_weights[valid].std()),
            # Mean of min weights for valid target points
            "min_weight_mean": float(min_weights[valid].mean()),
            # Standard deviation of min weights for valid target points
            "min_weight_std": float(min_weights[valid].std()),
            # Fraction of target points that are direct hits
            "direct_hit_fraction": self.direct_hit_fraction,
            # Fraction of target points that are interpolated
            "interpolated_fraction": self.interpolated_fraction,
            # Fraction of target points that have valid source data
            "coverage_fraction": self.coverage_fraction,
        }

    def coverage_map(self) -> np.ndarray:
        """
        Compute a (lat, lon) boolean map showing where points have valid source data.

        This is useful for debugging and visualizing the coverage of the source
        data points in the target grid.

        Returns
        -------
            (lat, lon) bool array - True where target has valid source data
        """
        # TODO
        mask = self._coarse_mask if getattr(self, "_use_idw", False) else self._mask
        # Initialize the coverage map with False
        # We'll set True where the target point has valid source data
        coverage = np.zeros(self.target_shape, dtype=bool)

        # Set True where target has valid source data
        # We'll use the ~ operator to invert the mask (i.e. ~self._mask will be True
        # where the target point has valid source data)
        coverage[~mask.reshape(self.target_shape)] = True

        return coverage

    def interpolation_map(self) -> np.ndarray:
        """
        Compute a (lat, lon) uint8 array describing the interpolation type at each target point.

        The returned array has the following values:
            0 = direct hit (max weight > DIRECT_HIT_THRESHOLD): target point is
                a direct hit, i.e. the max weight is greater than the threshold.
            1 = interpolated (max weight <= DIRECT_HIT_THRESHOLD): target point is
                an interpolated point, i.e. the max weight is less than or equal to the
                threshold.
            2 = no coverage (outside convex hull): target point is outside the convex
                hull of the source points and has no coverage.

        Returns
        -------
            (lat, lon) uint8 array
        """
        # TODO
        if self._use_idw:
            mask = self._coarse_mask.reshape(self.target_shape)
            interp_map = np.where(mask, 2, 1).astype(np.uint8)
            return interp_map

        # Compute max weights
        max_weights = self._weights.max(axis=1)

        # Initialize interpolation map with no coverage
        interp_map = np.full(len(self._mask), 2, dtype=np.uint8)

        # Direct hits (max weight >= DIRECT_HIT_THRESHOLD)
        direct = (max_weights >= self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        interp_map[direct] = 0

        # Interpolated (max weight < DIRECT_HIT_THRESHOLD)
        interpolated = (max_weights < self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        interp_map[interpolated] = 1  # Note: DQF distinguishes valid vs NaN interpolated cases

        # Reshape to target shape
        interp_map = interp_map.reshape(self.target_shape)

        return interp_map

    ############################################################################################
    # CF METADATA HELPERS
    ############################################################################################

    @staticmethod
    def dqf_attrs() -> dict:
        """
        Return CF-compliant DQF attributes with extended flag values.

        The returned dictionary has the following keys:
            - 'standard_name': standard name of the DQF variable
            - 'flag_values': list of flag values
            - 'flag_meanings': space-separated string of flag meanings
            - 'valid_range': tuple of valid flag values
            - 'comment': string describing the flag values
        """
        return {
            "standard_name": "status_flag",
            "flag_values": list(multicloudconstants.DQF_FLAGS.keys()),
            "flag_meanings": " ".join(v["meaning"] for v in multicloudconstants.DQF_FLAGS.values()),
            "valid_range": [min(multicloudconstants.DQF_FLAGS), max(multicloudconstants.DQF_FLAGS)],
            "comment": (
                "Flag 3 (no_value_qf) indicates target location is outside source data convex hull. "
                "Flag 5 (interpolated_qf) indicates value was computed via barycentric "
                "interpolation from neighboring source pixels with different quality flags. "
                "Flag 6 indicates target location has a NaN pixel within the convex hull"
            ),
        }

    def regridding_provenance(self) -> dict:
        """
        Return a provenance dict for storing in GOESZarrStore region attrs.

        This provenance dict stores information about the regridding method used,
        including the method name, source projection, triangulation used,
        direct hit threshold, and coverage/interpolated fractions.

        For IDW instances created via from_weights_at_resolution(), barycentric-
        specific fields (direct_hit_threshold, integer_epsilon, direct_hit_fraction,
        interpolated_fraction) are omitted since they are not meaningful for the
        IDW path.

        If the weights_dir attribute is set, it also stores the path to the
        cached weights directory.
        """
        use_idw = getattr(self, "_use_idw", False)

        provenance = {
            # Interpolation method: barycentric (native resolution) or idw (coarse resolution)
            "method": "idw" if use_idw else "barycentric",
            # Source projection is geostationary for all supported satellites
            "source_projection": "geostationary",
            # Triangulation method -- not applicable for IDW path
            "triangulation": "n/a" if use_idw else "delaunay",
            # Coverage fraction is valid for both paths
            "coverage_fraction": self.coverage_fraction,
            # Reference band used to compute the native weights
            "reference_band": self._reference_band,
        }

        if not use_idw:
            # Barycentric-specific fields -- not meaningful for IDW
            provenance["direct_hit_threshold"] = self.DIRECT_HIT_THRESHOLD
            provenance["integer_epsilon"] = self.INTEGER_EPSILON
            provenance["direct_hit_fraction"] = self.direct_hit_fraction
            provenance["interpolated_fraction"] = self.interpolated_fraction

        if self._weights_dir:
            # If the weights_dir attribute is set, store the path to the cached weights directory
            provenance["weights_path"] = str(self._weights_dir)

        return provenance

    ############################################################################################
    # DUNDER
    ############################################################################################

    def __repr__(self) -> str:
        """Return a concise string representation of the GeostationaryRegridder instance."""
        method = "IDW" if self._use_idw else "barycentric"
        return (
            f"GeostationaryRegridder(\n"
            # Source grid shape (lat, lon)
            f"    source={self.source_shape}, \n"
            # Target grid shape (lat, lon)
            f"    target={self.target_shape}, \n"
            # Coverage fraction (0.0 - 1.0)
            f"    coverage={self.coverage_fraction:.2%}, \n"
            # Whether cached weights are available
            f"    cached={self.has_cached_weights}\n"
            f")"
        )
