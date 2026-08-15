import logging
import warnings
from datetime import UTC, datetime
from typing import Any

import numpy as np
from zarr import Array

import goesdatabuilder
from goesdatabuilder.data.goes import multicloudconstants
from goesdatabuilder.data.goes.multicloud import GOESMultiCloudObservation
from goesdatabuilder.store.zarrstore import ArrayPresetLike, ZarrStoreBuilder
from goesdatabuilder.utils.config import ConfigDefault
from goesdatabuilder.utils.grid_utils import validate_longitude_monotonic

logger = logging.getLogger(__name__)


class GOESZarrStore(ZarrStoreBuilder):
    """
    CF-compliant Zarr store builder for GOES ABI imagery.

    Stores regridded lat/lon data with full CF metadata.
    Fully configurable via YAML - supports ACDD-1.3, provenance tracking, and extended DQF flags.
    """

    ############################################################################################
    # CLASS CONSTANTS (FALLBACK IF NOT IN CONFIG)
    ############################################################################################

    CELL_METHODS = "time: point latitude,longitude: mean"  # indicates what each pixel means in metadata

    ############################################################################################
    # STORE INITIALIZATION
    ############################################################################################

    def initialize_store(
        self,
        global_metadata: dict[str, Any] = ConfigDefault("store", "global_metadata"),
    ) -> None:
        """Create store, root group with CF global attributes."""
        self.open_store(mode="a")
        if not self.get_attrs():
            # only set attrs if they are empty (the store was newly created)
            global_attrs = self._cf_global_attrs()
            self.set_attrs("/", global_attrs, merge=False)
            self.set_attrs("/", global_metadata, merge=True)

        logger.info("Initialized GOES Zarr store.")

    def initialize_region(
        self,
        lat: np.ndarray,
        lon: np.ndarray,
        observation: "GOESMultiCloudObservation",
        presets: dict[str, ArrayPresetLike] = ConfigDefault("store", "presets"),
        bands: list[int] = multicloudconstants.ALL_BANDS,
        include_dqf: bool = True,
        provenance: dict[str, Any] | None = None,
    ) -> None:
        """
        Create region group.

        Input: region name ('GOES-East' or 'GOES-West' or 'GOES-Test')
               lat - 1D array of latitudes (degrees_north), must be monotonic
               lon - 1D array of longitudes (degrees_east), must be monotonic
               observation - GOESMultiCloudObservation instance
               bands - which bands to create (default: from config or all 16)
               include_dqf - whether to create DQF arrays
               regridder - GeostationaryRegridder instance for full provenance
               exist_ok - does not raise an error if the region group already exists
        Job: Create region group with full provenance attrs,
             create dimension coords (lat, lon, time),
             create auxiliary coords (platform_id, scan_mode),
             create all CMI and DQF arrays with CF attrs.
        """
        # ensures that only a single file is loaded (assumes encoding is consistent across all files in the observation)
        region = observation.first.ds.attrs["orbital_slot"]

        if region not in multicloudconstants.VALID_ORBITAL_SLOTS:
            raise ValueError(f"Invalid region '{region}'. Must be one of {multicloudconstants.VALID_ORBITAL_SLOTS}")

        if self.group_exists(region):
            return

        # Validate lat/lon are monotonic
        if not (np.all(np.diff(lat) > 0) or np.all(np.diff(lat) < 0)):
            raise ValueError("Latitude array must be monotonic")
        if not validate_longitude_monotonic(lon):
            raise ValueError(
                "Longitude array must be monotonic (checked in 0-360 space for antimeridian-crossing grids)"
            )

        # Create region group with full metadata
        region_attrs = self._cf_region_attrs(lat, lon, provenance)
        self.create_group(region, attrs=region_attrs)

        logger.info(f"Creating region '{region}' with lat={len(lat)}, lon={len(lon)}, bands={bands}")

        # Create dimension coordinates
        self._create_lat_coord(region, lat, presets.get("lat", "coordinate"))
        self._create_lon_coord(region, lon, presets.get("lon", "coordinate"))
        self._create_time_coord(region, presets.get("time", "coordinate"))

        # Create auxiliary coordinates
        self._create_auxiliary_coords(region, presets)
        # Create CMI and DQF arrays for each band

        for band in bands:
            self._create_cmi_array(region, band, presets.get(f"CMI_C{band:02d}", "field"), observation.first)
            if include_dqf:
                self._create_dqf_array(region, band, presets.get(f"DQF_C{band:02d}", "field"))

        logger.info(f"Initialized region '{region}' with {len(bands)} bands")

    ############################################################################################
    # COORDINATE CREATION (PRIVATE)
    ############################################################################################

    def _create_lat_coord(self, region: str, lat: np.ndarray, preset: ArrayPresetLike) -> None:
        """
        Create latitude coordinate array for a region.

        Creates a CF-compliant latitude coordinate array with proper metadata.
        The coordinate is created with the full latitude extent as a single chunk
        since it's typically accessed in its entirety for spatial operations.

        :param region: Region identifier (e.g., 'GOES-East', 'GOES-West')
        :type region: str
        :param lat: Array of latitude values in degrees north
        :type lat: np.ndarray
        :param preset: Array pipeline preset for compression configuration
        :type preset: str
        """
        path = f"{region}/lat"

        attrs = {
            "standard_name": "latitude",
            "long_name": "latitude",
            "units": "degrees_north",
            "axis": "Y",
        }

        self.create_array(
            path=path,
            shape=(len(lat),),
            dtype=lat.dtype,
            attrs=attrs,
            preset=preset,
            dimension_names=["lat"],
        )

        self.write_array(path, lat)

    def _create_lon_coord(self, region: str, lon: np.ndarray, preset: ArrayPresetLike) -> None:
        """
        Create longitude coordinate array for a region.

        Creates a CF-compliant longitude coordinate array with proper metadata.
        The coordinate is created with the full longitude extent as a single chunk
        since it's typically accessed in its entirety for spatial operations.

        :param region: Region identifier (e.g., 'GOES-East', 'GOES-West')
        :type region: str
        :param lon: Array of longitude values in degrees east
        :type lon: np.ndarray
        :param preset: Array pipeline preset for compression configuration
        :type preset: str
        """
        path = f"{region}/lon"

        attrs = {
            "standard_name": "longitude",
            "long_name": "longitude",
            "units": "degrees_east",
            "axis": "X",
        }

        self.create_array(
            path=path,
            shape=(len(lon),),
            dtype=lon.dtype,
            attrs=attrs,
            preset=preset,
            dimension_names=["lon"],
        )

        self.write_array(path, lon)

    def _create_time_coord(self, region: str, preset: ArrayPresetLike) -> None:
        """
        Create extensible time coordinate array for a region.

        Creates an empty, extensible time dimension coordinate that can be appended
        to as new observations are added. Uses datetime64[ns] for CF-compliant
        time representation and is configured for efficient time-series operations.

        :param region: Region identifier (e.g., 'GOES-East', 'GOES-West')
        :type region: str
        :param chunks: Chunk size for time dimension (default: 512 for efficient append)
        :type chunks: tuple
        :param preset: Array pipeline preset for compression configuration
        :type preset: str
        """
        path = f"{region}/time"

        attrs = {
            "standard_name": "time",
            "long_name": "observation time",
            "axis": "T",
        }

        self.create_array(
            path=path,
            shape=(0,),
            dtype="datetime64[ns]",
            attrs=attrs,
            preset=preset,
            dimension_names=["time"],
        )

    def _create_auxiliary_coords(self, region: str, presets: dict[str, ArrayPresetLike]) -> None:
        """
        Create auxiliary coordinate arrays for a region.

        Creates empty, extensible auxiliary coordinate arrays that store metadata
        for each observation including platform identifier and scan mode. These
        coordinates are aligned with the time dimension and are populated as
        observations are appended to the store.

        Creates two auxiliary arrays:
        - platform_id: Satellite platform identifier (e.g., 'G16', 'G18')
        - scan_mode: ABI scan mode (e.g., '3', '4', '6')

        :param region: Region identifier (e.g., 'GOES-East', 'GOES-West')
        :type region: str
        :param preset: Array pipeline preset for compression configuration
        :type preset: str
        """
        with warnings.catch_warnings():
            # ignore warning that U3 and U10 are not valid zarr dtypes
            warnings.simplefilter("ignore")
            platform_attrs = {
                "long_name": "satellite platform identifier",
                "cf_role": "auxiliary_coordinate",
            }
            self.create_array(
                path=f"{region}/platform_id",
                shape=(0,),
                dtype="U3",
                attrs=platform_attrs,
                preset=presets.get("platform_id", "coordinate"),
                dimension_names=["time"],
            )
            scan_attrs = {
                "long_name": "ABI scan mode",
                "cf_role": "auxiliary_coordinate",
            }
            self.create_array(
                path=f"{region}/scan_mode",
                shape=(0,),
                dtype="U10",
                attrs=scan_attrs,
                preset=presets.get("scan_mode", "coordinate"),
                dimension_names=["time"],
            )

    ############################################################################################
    # ARRAY CREATION (PRIVATE)
    ############################################################################################

    def _create_cmi_array(
        self, region: str, band: int, preset: ArrayPresetLike, observation: "GOESMultiCloudObservation"
    ) -> Array:
        """Create CMI_C##(time, lat, lon) float32, empty/extensible on time."""
        if band not in range(1, 17):
            raise ValueError(f"Invalid band {band}. Must be 1-16")

        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")

        path = f"{region}/CMI_C{band:02d}"

        preset = self._get_array_configuration(preset)

        encoding = observation.ds[f"CMI_C{band:02d}"].encoding
        overrides = {}

        if "scale_factor" in encoding and "add_offset" in encoding:
            filters = preset.get("filters")
            overrides["fill_value"] = fill_value = preset.get("fill_value", 0)
            zarr_scale = float(1 / encoding["scale_factor"])
            # the additional scale_factors add 2 positions at 0 and 1 for the NaN and fill_values
            zarr_offset = float(-(encoding["add_offset"] + encoding["scale_factor"] * 2))
            scale_offset_fill_value = (fill_value - zarr_offset) * zarr_scale
            # The ScaleOffset codec converts the data to a positive integer value using the scale_factor and
            # offset values provided by the input netcdf GOES data. This value is increased by 2 so that 0 can
            # be reserved for storing NaN values and 1 for the fill_value.
            # These values are then converted to uint16 using the CastValue codec to store the data on disk
            # The fill_value is stored specially so that it can be matched exactly when determining if a selection
            # of a given array is empty or not.
            codec = [
                {
                    "codec": "zarr.codecs:ScaleOffset",
                    "kwargs": {
                        "scale": zarr_scale,
                        "offset": zarr_offset,
                    },
                },
                {
                    "codec": "zarr.codecs:CastValue",
                    "kwargs": {
                        "data_type": "uint16",
                        "rounding": "nearest-even",
                        "scalar_map": {
                            "encode": [["NaN", 0], [scale_offset_fill_value, 1]],
                            "decode": [[0, "NaN"], [1, scale_offset_fill_value]],
                        },
                    },
                },
            ]
            if isinstance(filters, list):
                if filters:
                    logger.warning(
                        f"ScaleOffset filter is being appended to the filters list for the zarr array at {path}. "
                        "If you have already set a scale offset filter for this array, please remove it so that the filter doesn't "
                        "get applied twice."
                    )
                overrides["filters"] = filters + codec
            else:
                overrides["filters"] = codec

        return self.create_array(
            path=path,
            shape=(0, lat_arr.shape[0], lon_arr.shape[0]),
            dtype=np.float32,
            attrs=self._cf_cmi_attrs(band),
            preset=preset,
            dimension_names=["time", "lat", "lon"],
            **overrides,
        )

    def _create_dqf_array(self, region: str, band: int, preset: ArrayPresetLike) -> Array:
        """Create DQF_C##(time, lat, lon) uint8, empty/extensible on time."""
        if band not in range(1, 17):
            raise ValueError(f"Invalid band {band}. Must be 1-16")

        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")

        path = f"{region}/DQF_C{band:02d}"

        return self.create_array(
            path=path,
            shape=(0, lat_arr.shape[0], lon_arr.shape[0]),
            dtype=np.uint8,
            attrs=self._cf_dqf_attrs(band),
            preset=preset,
            dimension_names=["time", "lat", "lon"],
        )

    ############################################################################################
    # VALIDATION
    ############################################################################################

    def _validate_region(self, region: str) -> None:
        """Check region is valid and exists in store."""
        if region not in multicloudconstants.VALID_ORBITAL_SLOTS:
            raise ValueError(f"Invalid region '{region}'. Must be one of {multicloudconstants.VALID_ORBITAL_SLOTS}")

        if not self.group_exists(region):
            raise KeyError(f"Region '{region}' not initialized in store")

    ############################################################################################
    # QUERY
    ############################################################################################

    def get_time_range(self, region: str) -> tuple[np.datetime64, np.datetime64] | None:
        """Get (start, end) as datetime64, or None if no observations."""
        self._validate_region(region)

        time_arr = self.get_array(f"{region}/time")

        if time_arr.shape[0] == 0:
            return None

        return time_arr[0], time_arr[-1]

    def get_observation_count(self, region: str) -> int:
        """Get number of time steps."""
        self._validate_region(region)

        time_arr = self.get_array(f"{region}/time")
        return time_arr.shape[0]

    def get_spatial_extent(self, region: str) -> dict:
        """Get lat/lon bounds."""
        self._validate_region(region)

        lat_arr = self.get_array(f"{region}/lat")
        lon_arr = self.get_array(f"{region}/lon")

        lat_vals = lat_arr[:]
        lon_vals = lon_arr[:]

        return {
            "lat_min": float(lat_vals.min()),
            "lat_max": float(lat_vals.max()),
            "lon_min": float(lon_vals.min()),
            "lon_max": float(lon_vals.max()),
        }

    def get_bands(self, region: str) -> list:
        """Get sorted list of band numbers present."""
        self._validate_region(region)

        arrays = self.array_list(region)

        bands = []
        for name in arrays:
            if name.startswith("CMI_C"):
                try:
                    band_num = int(name[5:7])
                    bands.append(band_num)
                except ValueError:
                    continue

        return sorted(bands)

    def get_platforms(self, region: str) -> list:
        """Get unique platform_id values."""
        self._validate_region(region)

        platform_arr = self.get_array(f"{region}/platform_id")

        if platform_arr.shape[0] == 0:
            return []

        platforms = platform_arr[:]
        return sorted(list(set(platforms)))

    def get_time_index(self, region: str, timestamp: np.datetime64) -> int | None:
        """
        Return the index of the timestamp on the region/time array or None if it doesn't exist.

        Ensures that timestamps are increasing and unique. This is used to determine if regridded
        observation data should replace a value in the store or be appended to the store.
        """
        store_times = self.get_array(f"{region}/time")[:]  # TODO: cache this
        stored_observation_idx = (store_times == timestamp).nonzero()[0]

        if stored_observation_idx.size == 0:
            if store_times.size > 0 and timestamp < store_times[-1]:
                raise Exception(
                    f"Observation at time {timestamp} cannot be added to the zarr store in a way that maintains monotonicity. "
                    "Values must be increasing on the time dimension."
                )
            return None
        elif stored_observation_idx.size == 1:
            return int(stored_observation_idx[0])
        else:
            raise Exception(
                f"Region {region} already has multiple values for time {timestamp}. "
                "This should not happen: please manually remove duplicate entries from the zarr store."
            )

    ############################################################################################
    # PROVENANCE & METADATA UPDATES
    ############################################################################################

    def update_temporal_coverage(self, region: str) -> None:
        """
        Update time_coverage_* attributes based on current data.

        Should be called after appending observations.

        Note: this also updates the date_modified value for the global attributes.
        """
        time_range = self.get_time_range(region)

        if time_range is None:
            return

        region_start, region_end = time_range

        # TODO: update time_coverage_resolution as well
        # Update global attrs
        for path in ["/", f"/{region}"]:
            current_attrs = self.get_attrs(path)

            start = min(region_start, np.datetime64(current_attrs.get("time_coverage_start", region_start)))
            end = max(region_end, np.datetime64(current_attrs.get("time_coverage_end", region_end)))

            current_attrs["time_coverage_start"] = str(start)
            current_attrs["time_coverage_end"] = str(end)

            # Calculate duration
            duration = end - start
            current_attrs["time_coverage_duration"] = str(duration)

            # Update modified timestamp
            if path == "/":
                current_attrs["date_modified"] = datetime.now(UTC).isoformat() + "Z"

            self.set_attrs(path, current_attrs, merge=True)

        logger.debug(f"Updated temporal coverage for {region}: {start} to {end}")

    def add_processing_history(self, message: str) -> None:
        """Append to processing history attribute."""
        current_attrs = self.get_attrs("/")
        current_history = current_attrs.get("history", "")

        timestamp = datetime.now(UTC).isoformat() + "Z"
        new_entry = f"{timestamp}: {message}"

        if current_history:
            current_attrs["history"] = f"{current_history}\n{new_entry}"
        else:
            current_attrs["history"] = new_entry

        self.set_attrs("/", current_attrs, merge=True)

    def add_source_files(self, region: str, file_paths: list[str]) -> None:
        """Track source files used to create this dataset."""
        region_attrs = self.get_attrs(region)

        existing_raw = region_attrs.get("source_files", "")
        existing_sources = existing_raw.split("\n") if existing_raw else []

        all_sources = sorted(set(existing_sources + file_paths))

        self.set_attrs(
            region,
            {
                "source_files": "\n".join(all_sources),
                "source_file_count": len(all_sources),
            },
            merge=True,
        )

    def finalize_dataset(self) -> None:
        """
        Finalize metadata updates.

        - Update all temporal coverage
        - Add final history entry
        - Optionally validate CF compliance.
        """
        for region in multicloudconstants.VALID_ORBITAL_SLOTS:
            if self.group_exists(region):
                self.update_temporal_coverage(region)
            else:
                logger.warning(f"Configured region '{region}' not found in store, skipping")

        # Add final history entry
        self.add_processing_history("Dataset finalized and ready for distribution")

        logger.info("Dataset finalized")

    ############################################################################################
    # CF METADATA (PRIVATE)
    ############################################################################################

    def _cf_global_attrs(self) -> dict:
        """Return CF global attributes from config with ACDD compliance."""
        # Default values
        attrs = {
            "processing_software": goesdatabuilder.__name__,
            "processing_software_version": goesdatabuilder.__version__,
            "processing_software_url": goesdatabuilder.__url__,
        }

        # Add timestamps (always current)
        now = datetime.now(UTC).isoformat() + "Z"
        attrs["date_created"] = now
        attrs["date_modified"] = now
        attrs["history"] = f"Created {now}"

        return attrs

    def _cf_region_attrs(self, lat: np.ndarray, lon: np.ndarray, regridding_provenance: dict | None = None) -> dict:
        """Return region attributes with full regridding provenance."""
        attrs = {
            "geospatial_lat_min": float(lat.min()),
            "geospatial_lat_max": float(lat.max()),
            "geospatial_lon_min": float(lon.min()),
            "geospatial_lon_max": float(lon.max()),
            "geospatial_lat_units": "degrees_north",
            "geospatial_lon_units": "degrees_east",
            "geospatial_lat_resolution": float(np.abs(np.diff(lat).mean())),
            "geospatial_lon_resolution": float(np.abs(np.diff(lon).mean())),
        }

        # Add regridding provenance if available
        if regridding_provenance:
            # Store as individual attrs for CF compliance
            attrs["regridding_method"] = regridding_provenance.get("method", "barycentric")
            attrs["regridding_triangulation"] = regridding_provenance.get("triangulation", "delaunay")
            attrs["regridding_direct_hit_threshold"] = regridding_provenance.get("direct_hit_threshold", 0.999)
            attrs["source_projection"] = regridding_provenance.get("source_projection", "geostationary")

        return attrs

    def _cf_cmi_attrs(self, band: int) -> dict:
        """Return CF attributes for CMI array from config."""
        band_meta = multicloudconstants.BAND_METADATA.get(band, {})

        attrs = {
            "long_name": band_meta.get("long_name", f"Band {band}"),
            "standard_name": band_meta.get("standard_name", ""),
            "units": band_meta.get("units", "1"),
            "radiation_wavelength": band_meta.get("wavelength", 0.0),
            "radiation_wavelength_units": "um",
            "cell_methods": self.CELL_METHODS,
            "coordinates": "time lat lon",
            "ancillary_variables": f"DQF_C{band:02d}",
        }

        # Add description if present
        if "description" in band_meta:
            attrs["description"] = band_meta["description"]

        # Add products/applications if present
        if "products" in band_meta:
            # Store as comma-separated string for CF compliance
            attrs["products"] = ", ".join(band_meta["products"])

        # Add valid range from config
        if "valid_range" in band_meta:
            attrs["valid_range"] = band_meta["valid_range"]

        return attrs

    def _cf_dqf_attrs(self, band: int) -> dict:
        """Return CF attributes for DQF array with extended flags (0-6)."""
        attrs = {
            "long_name": f"ABI L2+ CMI data quality flags for band {band}",
            "standard_name": "status_flag",
            "units": "1",
            "flag_values": list(multicloudconstants.DQF_FLAGS.keys()),
            "flag_meanings": " ".join(v["meaning"] for v in multicloudconstants.DQF_FLAGS.values()),
            "valid_range": [min(multicloudconstants.DQF_FLAGS), max(multicloudconstants.DQF_FLAGS)],
            "coordinates": "time lat lon",
            "comment": (
                "Flags 0-4 from original GOES-R ABI L2 CMI product. "
                f"Flag {multicloudconstants.DQF_FLAGS[5]['name']} ({multicloudconstants.DQF_FLAGS[5]['meaning']}) "
                "indicates value was computed via barycentric interpolation. "
                f"Flag {multicloudconstants.DQF_FLAGS[6]['name']} ({multicloudconstants.DQF_FLAGS[6]['meaning']}) "
                "indicates some source pixels in the interpolation hull were NaN."
            ),
        }

        return attrs

    ############################################################################################
    # BAND METADATA HELPERS
    ############################################################################################

    def get_bands_for_product(self, product_name: str) -> list:
        """Return a list of list of band numbers that support the given product."""
        matching_bands = []

        for band in range(1, 17):
            band_meta = multicloudconstants.BAND_METADATA.get(band, {})
            products = band_meta.get("products", [])

            if product_name in products:
                matching_bands.append(band)

        return sorted(matching_bands)

    def get_products_for_band(self, band: int) -> list:
        """Return a list of products that use the band with the given number."""
        band_meta = multicloudconstants.BAND_METADATA.get(band, {})
        return band_meta.get("products", [])

    def list_all_products(self) -> list:
        """Return a sorted list of all unique products across all bands."""
        all_products = set()

        for band in range(1, 17):
            band_meta = multicloudconstants.BAND_METADATA.get(band, {})
            products = band_meta.get("products", [])
            all_products.update(products)

        return sorted(list(all_products))
