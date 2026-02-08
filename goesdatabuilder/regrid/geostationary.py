from pathlib import Path
import numpy as np
import json
from datetime import datetime
from scipy.spatial import Delaunay
from typing import Union, Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from ..data.goes.multicloud import GOESMultiCloudObservation

logger = logging.getLogger(__name__)


class GeostationaryRegridder:
    """
    Regrids geostationary imager data (x/y radians) to regular lat/lon grid.

    Uses Delaunay triangulation with barycentric interpolation.
    Weights computed once per source/destination grid pair and cached.

    Strategy:
    ---------
    Weights are computed from a reference band (default: band 7, shortwave window)
    to ensure consistent output grid size across all bands. Different bands can have
    slightly different source grids (10-20 pixels difference), so using a single
    reference prevents grid size mismatches.

    DQF Handling:
    ------------
    - Direct hit (single weight ≈ 1.0): preserve source DQF
    - Interpolated (distributed weights): DQF = 5 (interpolated)
    - Outside convex hull: DQF = 4 (no_input)

    Extended DQF Flag Values:
        0 = good_pixels_qf
        1 = conditionally_usable_pixels_qf
        2 = out_of_range_pixels_qf
        3 = no_value_pixels_qf
        4 = focal_plane_temperature_threshold_exceeded_qf
        5 = interpolated_qf         ← NEW

    Usage:
    ------
        # First time - computes and saves weights (takes ~40 mins)
        regridder = GeostationaryRegridder(
            source_x=obs.x.values,
            source_y=obs.y.values,
            projection=obs.projection,
            target_resolution=0.02,
            weights_dir='./regrid_weights/GOES-East/',
            reference_band=7
        )

        # Regrid bands
        cmi_regridded = regridder.regrid(obs.get_cmi(8).values)
        dqf_regridded = regridder.regrid_dqf(obs.get_dqf(8).values)

        # Later - loads cached weights instantly
        regridder = GeostationaryRegridder.from_weights('./regrid_weights/GOES-East/')

    Pipeline:
        GOESMultiCloudObservation → GeostationaryRegridder → GOESZarrStore
    """

    ############################################################################################
    # CLASS CONSTANTS
    ############################################################################################

    # DQF flag values
    # Original GOES DQF flag values (0-4, preserve as-is)
    DQF_GOOD = 0
    DQF_CONDITIONALLY_USABLE = 1
    DQF_OUT_OF_RANGE = 2
    DQF_NO_VALUE = 3
    DQF_FOCAL_PLANE_TEMP_EXCEEDED = 4

    # Extended DQF flags for regridding (5 new)
    DQF_INTERPOLATED = 5


    DQF_FLAG_MEANINGS = 'good_pixels_qf conditionally_usable_pixels_qf out_of_range_pixels_qf no_value_pixels_qf focal_plane_temperature_threshold_exceeded_qf interpolated_qf'
    DQF_FLAG_VALUES = [0, 1, 2, 3, 4, 5]
    # Weight threshold for "direct hit" (no interpolation)
    DIRECT_HIT_THRESHOLD = 0.999

    # File names for cached weights
    VERTICES_FILE = 'vertices.npy'
    WEIGHTS_FILE = 'weights.npy'
    MASK_FILE = 'mask.npy'
    METADATA_FILE = 'metadata.json'

    ############################################################################################
    # INITIALIZATION
    ############################################################################################

    def __init__(
            self,
            source_x: np.ndarray,
            source_y: np.ndarray,
            projection: dict,
            target_resolution: float = 0.02,
            target_lat: Optional[np.ndarray] = None,
            target_lon: Optional[np.ndarray] = None,
            weights_dir: Optional[Union[str, Path]] = None,
            load_cached: bool = True,
            reference_band: int = 7
    ):
        """
        Input:
            source_x          - 1D array of x coordinates (radians)
            source_y          - 1D array of y coordinates (radians)
            projection        - dict with geostationary projection params
            target_resolution - resolution in degrees (default 0.02)
            target_lat        - optional explicit lat array (overrides resolution)
            target_lon        - optional explicit lon array (overrides resolution)
            weights_dir       - directory to save/load cached weights
            load_cached       - if True, load existing weights if available
            reference_band    - band used to compute weights (default 7, shortwave)
        """
        self._source_x = source_x
        self._source_y = source_y
        self._projection = projection
        self._weights_dir = Path(weights_dir) if weights_dir else None
        self._reference_band = reference_band
        self._cached = False

        # Convert source x/y to lat/lon
        logger.info("Converting geostationary coordinates to lat/lon...")
        source_lat_2d, source_lon_2d = self._radians_to_latlon(source_x, source_y, projection)

        # Flatten and filter NaN values
        self._source_lat_flat = source_lat_2d.flatten()
        self._source_lon_flat = source_lon_2d.flatten()

        # Store source shape
        self._source_shape = source_lat_2d.shape

        # Build target grid
        if target_lat is not None and target_lon is not None:
            # Explicit target grid provided
            self._target_lat = target_lat
            self._target_lon = target_lon
        else:
            # Create target grid from resolution and source bounds
            valid_mask = ~np.isnan(self._source_lat_flat) & ~np.isnan(self._source_lon_flat)
            valid_lats = self._source_lat_flat[valid_mask]
            valid_lons = self._source_lon_flat[valid_mask]

            self._target_lat = np.round(
                np.arange(valid_lats.min(), valid_lats.max() + target_resolution, target_resolution),
                4
            )
            self._target_lon = np.round(
                np.arange(valid_lons.min(), valid_lons.max() + target_resolution, target_resolution),
                4
            )

        # Try to load cached weights
        if load_cached and self._weights_dir and self._validate_cached_weights(self._weights_dir):
            logger.info(f"Loading cached weights from {self._weights_dir}")
            self.load_weights(self._weights_dir)
        else:
            # Compute new weights
            logger.info("Computing interpolation weights (this may take ~40 minutes)...")
            self._vertices, self._weights, self._mask = self._compute_weights()

            # Save weights if directory specified
            if self._weights_dir:
                self.save_weights(self._weights_dir)

    @classmethod
    def from_weights(cls, weights_dir: Union[str, Path]) -> 'GeostationaryRegridder':
        """
        Load regridder from cached weights without source data.

        Input: directory containing cached weights
        Output: GeostationaryRegridder instance
        Raises: FileNotFoundError if weights not found
        """
        weights_dir = Path(weights_dir)

        if not weights_dir.exists():
            raise FileNotFoundError(f"Weights directory not found: {weights_dir}")

        # Load metadata to get grid info
        metadata_path = weights_dir / cls.METADATA_FILE
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

        with open(metadata_path, 'r') as f:
            metadata = json.load(f)

        # Create instance with minimal initialization
        instance = cls.__new__(cls)
        instance._weights_dir = weights_dir
        instance._cached = True
        instance._source_shape = tuple(metadata['source_shape'])

        # Reconstruct target grid from metadata
        instance._target_lat = np.linspace(
            metadata['target_lat_min'],
            metadata['target_lat_max'],
            metadata['target_shape'][0]
        )
        instance._target_lon = np.linspace(
            metadata['target_lon_min'],
            metadata['target_lon_max'],
            metadata['target_shape'][1]
        )

        # Load weights
        instance.load_weights(weights_dir)

        logger.info(f"Loaded regridder from {weights_dir}")

        return instance

    ############################################################################################
    # PROPERTIES
    ############################################################################################

    @property
    def target_lat(self) -> np.ndarray:
        """1D target latitude array"""
        return self._target_lat

    @property
    def target_lon(self) -> np.ndarray:
        """1D target longitude array"""
        return self._target_lon

    @property
    def target_shape(self) -> tuple[int, int]:
        """(lat_size, lon_size) of output grid"""
        return (len(self._target_lat), len(self._target_lon))

    @property
    def source_shape(self) -> tuple[int, int]:
        """(y_size, x_size) of source grid"""
        return self._source_shape

    @property
    def n_target_points(self) -> int:
        """Total points in target grid"""
        return self.target_shape[0] * self.target_shape[1]

    @property
    def n_valid_points(self) -> int:
        """Target points inside source convex hull"""
        return int((~self._mask).sum())

    @property
    def coverage_fraction(self) -> float:
        """Fraction of target grid covered by source data"""
        return self.n_valid_points / self.n_target_points

    @property
    def has_cached_weights(self) -> bool:
        """True if weights are loaded from cache"""
        return self._cached

    @property
    def weights_dir(self) -> Optional[Path]:
        """Directory where weights are cached"""
        return self._weights_dir

    @property
    def direct_hit_fraction(self) -> float:
        """Fraction of target points that are direct hits (no interpolation)"""
        max_weights = self._weights.max(axis=1)
        direct_hits = (max_weights > self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        return direct_hits.sum() / self.n_valid_points if self.n_valid_points > 0 else 0.0

    @property
    def interpolated_fraction(self) -> float:
        """Fraction of target points that require interpolation"""
        max_weights = self._weights.max(axis=1)
        interpolated = (max_weights <= self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        return interpolated.sum() / self.n_valid_points if self.n_valid_points > 0 else 0.0

    ############################################################################################
    # COORDINATE TRANSFORMS (PRIVATE)
    ############################################################################################

    def _radians_to_latlon(
            self,
            x: np.ndarray,
            y: np.ndarray,
            projection: dict
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Convert GOES-R ABI fixed grid projection coordinates (x, y) from radians
        to geographic latitude and longitude in degrees.

        Uses your calculate_degrees_from_radians function.
        """
        # Read projection parameters
        lon_origin = projection["longitude_of_projection_origin"]
        H = projection["perspective_point_height"] + projection["semi_major_axis"]
        r_eq = projection["semi_major_axis"]
        r_pol = projection["semi_minor_axis"]

        # Create 2D coordinate matrices from 1D coordinate vectors
        x_2d, y_2d = np.meshgrid(x, y)

        # Equations to calculate latitude and longitude
        lambda_0 = (lon_origin * np.pi) / 180.0
        a_var = np.power(np.sin(x_2d), 2.0) + (
                np.power(np.cos(x_2d), 2.0)
                * (
                        np.power(np.cos(y_2d), 2.0)
                        + (((r_eq * r_eq) / (r_pol * r_pol)) * np.power(np.sin(y_2d), 2.0))
                )
        )
        b_var = -2.0 * H * np.cos(x_2d) * np.cos(y_2d)
        c_var = (H ** 2.0) - (r_eq ** 2.0)
        r_s = (-1.0 * b_var - np.sqrt((b_var ** 2) - (4.0 * a_var * c_var))) / (2.0 * a_var)
        s_x = r_s * np.cos(x_2d) * np.cos(y_2d)
        s_y = -r_s * np.sin(x_2d)
        s_z = r_s * np.cos(x_2d) * np.sin(y_2d)

        # Ignore numpy errors for sqrt of negative number
        np.seterr(all="ignore")

        abi_lat = (180.0 / np.pi) * (
            np.arctan(((r_eq * r_eq) / (r_pol * r_pol)) * (s_z / np.sqrt(((H - s_x) * (H - s_x)) + (s_y * s_y))))
        )
        abi_lon = (lambda_0 - np.arctan(s_y / (H - s_x))) * (180.0 / np.pi)

        return abi_lat, abi_lon

    def _build_source_coords(self) -> np.ndarray:
        """
        Build (N, 2) array of [lat, lon] pairs from flattened source grid.
        Filter out NaN values (off-earth pixels).
        """
        # Stack lat/lon and filter valid points
        valid_mask = ~np.isnan(self._source_lat_flat) & ~np.isnan(self._source_lon_flat)

        source_lats = self._source_lat_flat[valid_mask]
        source_lons = self._source_lon_flat[valid_mask]

        # Stack as (N, 2) with [lat, lon] order
        return np.vstack((source_lats, source_lons)).T

    def _build_target_coords(self) -> np.ndarray:
        """
        Build (M, 2) array of [lat, lon] pairs from target grid.
        """
        # Create meshgrid
        lon_grid, lat_grid = np.meshgrid(self._target_lon, self._target_lat)

        # Flatten and stack
        return np.vstack((lat_grid.flatten(), lon_grid.flatten())).T

    ############################################################################################
    # WEIGHT COMPUTATION (PRIVATE)
    ############################################################################################

    def _compute_weights(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Compute Delaunay triangulation and barycentric interpolation weights.

        This is your interp_weights function.

        Returns:
            vertices - (M, 3) int32, indices into source coords
            weights  - (M, 3) float32, barycentric weights
            mask     - (M,) bool, True if outside convex hull
        """
        source_coords = self._build_source_coords()
        target_coords = self._build_target_coords()

        logger.info(f"Building Delaunay triangulation with {len(source_coords)} source points...")

        # Create Delaunay triangulation
        tri = Delaunay(source_coords)

        logger.info(f"Finding simplices for {len(target_coords)} target points...")

        # Find which triangle each target point falls in
        simplex = tri.find_simplex(target_coords)

        # Get vertices of triangles
        vertices = np.take(tri.simplices, simplex, axis=0)

        # Compute barycentric coordinates
        d = source_coords.shape[1]  # dimension (2 for lat/lon)
        temp = np.take(tri.transform, simplex, axis=0)
        delta = target_coords - temp[:, d]
        bary = np.einsum('njk,nk->nj', temp[:, :d, :], delta)

        # Combine barycentric coords (sum to 1)
        weights = np.hstack((bary, 1 - bary.sum(axis=1, keepdims=True)))

        # Mask for points outside convex hull
        mask = simplex == -1

        logger.info(f"Coverage: {(~mask).sum()}/{len(mask)} points ({self.coverage_fraction:.2%})")

        return vertices, weights.astype(np.float32), mask

    ############################################################################################
    # WEIGHT I/O
    ############################################################################################

    def save_weights(self, weights_dir: Optional[Union[str, Path]] = None):
        """Save vertices, weights, mask, and metadata to directory"""
        if weights_dir is None:
            weights_dir = self._weights_dir

        if weights_dir is None:
            raise ValueError("weights_dir must be specified")

        weights_dir = Path(weights_dir)
        weights_dir.mkdir(parents=True, exist_ok=True)

        # Save arrays
        np.save(weights_dir / self.VERTICES_FILE, self._vertices)
        np.save(weights_dir / self.WEIGHTS_FILE, self._weights)
        np.save(weights_dir / self.MASK_FILE, self._mask)

        # Save metadata
        self._save_metadata(weights_dir)

        logger.info(f"Saved weights to {weights_dir}")

    def load_weights(self, weights_dir: Union[str, Path]):
        """Load vertices, weights, and mask from directory"""
        weights_dir = Path(weights_dir)

        # Load arrays
        self._vertices = np.load(weights_dir / self.VERTICES_FILE)
        self._weights = np.load(weights_dir / self.WEIGHTS_FILE)
        self._mask = np.load(weights_dir / self.MASK_FILE)

        self._cached = True
        self._weights_dir = weights_dir

        logger.info(f"Loaded weights from {weights_dir}")

    def _save_metadata(self, weights_dir: Path):
        """Save metadata JSON with grid info and statistics"""
        metadata = {
            'source_shape': list(self._source_shape),
            'target_shape': list(self.target_shape),
            'target_lat_min': float(self._target_lat.min()),
            'target_lat_max': float(self._target_lat.max()),
            'target_lon_min': float(self._target_lon.min()),
            'target_lon_max': float(self._target_lon.max()),
            'target_lat_resolution': float(np.abs(np.diff(self._target_lat).mean())),
            'target_lon_resolution': float(np.abs(np.diff(self._target_lon).mean())),
            'n_target_points': self.n_target_points,
            'n_valid_points': self.n_valid_points,
            'coverage_fraction': self.coverage_fraction,
            'direct_hit_fraction': self.direct_hit_fraction,
            'interpolated_fraction': self.interpolated_fraction,
            'reference_band': self._reference_band,
            'created_at': datetime.utcnow().isoformat() + 'Z',
        }

        with open(weights_dir / self.METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)

    def _validate_cached_weights(self, weights_dir: Path) -> bool:
        """Check if cached weights exist and are compatible with current grid"""
        required_files = [
            self.VERTICES_FILE,
            self.WEIGHTS_FILE,
            self.MASK_FILE,
            self.METADATA_FILE
        ]

        # Check all files exist
        if not all((weights_dir / f).exists() for f in required_files):
            return False

        # Load metadata and check compatibility
        try:
            with open(weights_dir / self.METADATA_FILE, 'r') as f:
                metadata = json.load(f)

            # Check target grid matches
            expected_shape = (len(self._target_lat), len(self._target_lon))
            if tuple(metadata['target_shape']) != expected_shape:
                logger.warning(f"Cached grid shape {metadata['target_shape']} doesn't match expected {expected_shape}")
                return False

            return True

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Invalid metadata file: {e}")
            return False

    ############################################################################################
    # REGRIDDING - CMI (CONTINUOUS)
    ############################################################################################

    def regrid(self, data: np.ndarray) -> np.ndarray:
        """
        Regrid continuous data (CMI) using barycentric interpolation.

        This is your interpolate function.

        Input: (y, x) or (time, y, x) source array
        Output: (lat, lon) or (time, lat, lon) regridded array
        """
        # Handle 3D input (time, y, x)
        if data.ndim == 3:
            n_time = data.shape[0]
            regridded = np.empty((n_time, *self.target_shape), dtype=data.dtype)

            for t in range(n_time):
                regridded[t] = self._interpolate_2d(data[t])

            return regridded

        # Handle 2D input (y, x)
        elif data.ndim == 2:
            return self._interpolate_2d(data)

        else:
            raise ValueError(f"Input must be 2D or 3D, got shape {data.shape}")

    def _interpolate_2d(self, data: np.ndarray) -> np.ndarray:
        """
        Interpolate single 2D array.

        Uses your einsum-based interpolation.
        """
        # Flatten source data
        data_flat = data.flatten()

        # Filter to valid source points (same mask used during weight computation)
        valid_mask = ~np.isnan(self._source_lat_flat) & ~np.isnan(self._source_lon_flat)
        data_valid = data_flat[valid_mask]

        # Interpolate: weighted sum of values at triangle vertices
        interpolated = np.einsum('nj,nj->n', np.take(data_valid, self._vertices), self._weights)

        # Set NaN for masked (out of hull) points
        interpolated[self._mask] = np.nan

        # Reshape to target grid
        return interpolated.reshape(self.target_shape)

    def regrid_batch(self, data: dict[int, np.ndarray]) -> dict[int, np.ndarray]:
        """Regrid multiple bands efficiently"""
        return {band: self.regrid(arr) for band, arr in data.items()}

    ############################################################################################
    # REGRIDDING - DQF (CATEGORICAL)
    ############################################################################################

    def regrid_dqf(self, dqf: np.ndarray) -> np.ndarray:
        """
        Regrid categorical DQF data.

        Logic:
            - Direct hit (max weight > threshold): use source DQF
            - Interpolated: DQF = 5
            - Outside hull: DQF = 4

        Input: (y, x) or (time, y, x) source DQF array (uint8)
        Output: (lat, lon) or (time, lat, lon) regridded DQF (uint8)
        """
        # Handle 3D input
        if dqf.ndim == 3:
            n_time = dqf.shape[0]
            regridded = np.empty((n_time, *self.target_shape), dtype=np.uint8)

            for t in range(n_time):
                regridded[t] = self._classify_dqf_2d(dqf[t])

            return regridded

        # Handle 2D input
        elif dqf.ndim == 2:
            return self._classify_dqf_2d(dqf)

        else:
            raise ValueError(f"Input must be 2D or 3D, got shape {dqf.shape}")

    def _classify_dqf_2d(self, dqf: np.ndarray) -> np.ndarray:
        """Classify DQF for each target point"""
        # Flatten source DQF
        dqf_flat = dqf.flatten()

        # Filter to valid source points
        valid_mask = ~np.isnan(self._source_lat_flat) & ~np.isnan(self._source_lon_flat)
        dqf_valid = dqf_flat[valid_mask]

        # Initialize output
        dqf_out = np.full(len(self._mask), self.DQF_NO_INPUT, dtype=np.uint8)

        # Find direct hits
        max_weights = self._weights.max(axis=1)
        direct_hit_mask = (max_weights > self.DIRECT_HIT_THRESHOLD) & (~self._mask)

        # For direct hits, use source DQF from dominant vertex
        dominant_vertex_idx = self._weights.argmax(axis=1)
        dominant_vertices = self._vertices[np.arange(len(self._vertices)), dominant_vertex_idx]
        dqf_out[direct_hit_mask] = dqf_valid[dominant_vertices[direct_hit_mask]]

        # For interpolated points, set flag 5
        interpolated_mask = (max_weights <= self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        dqf_out[interpolated_mask] = self.DQF_INTERPOLATED

        # Points outside hull already set to DQF_NO_INPUT

        return dqf_out.reshape(self.target_shape)

    def regrid_dqf_batch(self, dqf: dict[int, np.ndarray]) -> dict[int, np.ndarray]:
        """Regrid multiple DQF bands"""
        return {band: self.regrid_dqf(arr) for band, arr in dqf.items()}

    ############################################################################################
    # FULL OBSERVATION REGRIDDING
    ############################################################################################

    def regrid_observation(
            self,
            cmi_data: dict[int, np.ndarray],
            dqf_data: dict[int, np.ndarray]
    ) -> tuple[dict[int, np.ndarray], dict[int, np.ndarray]]:
        """
        Regrid CMI and DQF dicts from GOESMultiCloudObservation.

        Returns: (regridded_cmi, regridded_dqf) ready for GOESZarrStore
        """
        regridded_cmi = self.regrid_batch(cmi_data)
        regridded_dqf = self.regrid_dqf_batch(dqf_data)

        return regridded_cmi, regridded_dqf

    def regrid_to_observation_dict(
            self,
            obs: 'GOESMultiCloudObservation',
            time_idx: int = 0,
            bands: Optional[list[int]] = None
    ) -> dict:
        """
        Extract, regrid, and package single observation for GOESZarrStore.

        Returns dict compatible with GOESZarrStore.append_observation():
            {
                'timestamp': datetime64,
                'platform_id': str,
                'scan_mode': str,
                'cmi_data': {band: (lat, lon) array},
                'dqf_data': {band: (lat, lon) array},
            }
        """
        if bands is None:
            bands = list(range(1, 17))  # All bands

        # Extract single timestep
        obs_single = obs.isel_time(time_idx)

        # Get CMI and DQF for requested bands
        cmi_data = {band: obs_single.get_cmi(band).values for band in bands}
        dqf_data = {band: obs_single.get_dqf(band).values for band in bands}

        # Regrid
        cmi_regridded, dqf_regridded = self.regrid_observation(cmi_data, dqf_data)

        # Package as dict
        return {
            'timestamp': obs_single.time.values[0],
            'platform_id': obs_single.platform_id.values[0],
            'scan_mode': obs_single.scan_mode.values[0],
            'cmi_data': cmi_regridded,
            'dqf_data': dqf_regridded,
        }

    ############################################################################################
    # DIAGNOSTICS
    ############################################################################################

    def weight_statistics(self) -> dict:
        """Statistics on weight distribution"""
        max_weights = self._weights.max(axis=1)
        min_weights = self._weights.min(axis=1)

        # Filter to valid points
        valid = ~self._mask

        return {
            'max_weight_mean': float(max_weights[valid].mean()),
            'max_weight_std': float(max_weights[valid].std()),
            'min_weight_mean': float(min_weights[valid].mean()),
            'min_weight_std': float(min_weights[valid].std()),
            'direct_hit_fraction': self.direct_hit_fraction,
            'interpolated_fraction': self.interpolated_fraction,
            'coverage_fraction': self.coverage_fraction,
        }

    def coverage_map(self) -> np.ndarray:
        """(lat, lon) bool array - True where target has valid source data"""
        return (~self._mask).reshape(self.target_shape)

    def interpolation_map(self) -> np.ndarray:
        """
        (lat, lon) uint8 array
            0 = direct hit
            1 = interpolated
            2 = no coverage
        """
        max_weights = self._weights.max(axis=1)

        interp_map = np.full(len(self._mask), 2, dtype=np.uint8)

        # Direct hits
        direct = (max_weights > self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        interp_map[direct] = 0

        # Interpolated
        interpolated = (max_weights <= self.DIRECT_HIT_THRESHOLD) & (~self._mask)
        interp_map[interpolated] = 1

        return interp_map.reshape(self.target_shape)

    ############################################################################################
    # CF METADATA HELPERS
    ############################################################################################

    @staticmethod
    def dqf_attrs() -> dict:
        """CF-compliant DQF attributes with extended flag values"""
        return {
            'standard_name': 'status_flag',
            'flag_values': GeostationaryRegridder.DQF_FLAG_VALUES,
            'flag_meanings': GeostationaryRegridder.DQF_FLAG_MEANINGS,
            'valid_range': [0, 5],
            'comment': 'Flag 5 (interpolated) indicates value was computed via barycentric interpolation from neighboring source pixels. Flag 4 (no_input) indicates target location is outside source data convex hull.'
        }

    def regridding_provenance(self) -> dict:
        """
        Provenance dict for storing in GOESZarrStore region attrs.

        Returns dict with regridding metadata that GOESZarrStore expects.
        """
        provenance = {
            'method': 'barycentric',
            'source_projection': 'geostationary',
            'triangulation': 'delaunay',
            'direct_hit_threshold': self.DIRECT_HIT_THRESHOLD,
            'coverage_fraction': self.coverage_fraction,
            'direct_hit_fraction': self.direct_hit_fraction,
            'interpolated_fraction': self.interpolated_fraction,
            'reference_band': self._reference_band,
        }

        if self._weights_dir:
            provenance['weights_path'] = str(self._weights_dir)

        return provenance

    ############################################################################################
    # DUNDER
    ############################################################################################

    def __repr__(self) -> str:
        return (
            f"GeostationaryRegridder(\n"
            f"    source={self.source_shape}, \n"
            f"    target={self.target_shape}, \n"
            f"    coverage={self.coverage_fraction:.2%}, \n"
            f"    cached={self.has_cached_weights}\n"
            f")"
        )