# conftest.py

import uuid
from pathlib import Path

import numpy as np
import pytest
import xarray as xr


# Default valid GOES filename components
_DEFAULT_SCENE = "F"
_DEFAULT_MODE = "6"
_DEFAULT_SATELLITE = "18"
_DEFAULT_START = "20242842040203"
_DEFAULT_END = "20242842049523"
_DEFAULT_CREATED = "20242842050000"

# Default valid global attributes (values that pass orbital consistency validation)
_DEFAULT_ATTRS = {
    "id": "5c71d7cb-ab1d-451f-b0a4-1b47f0ba14ff",
    "dataset_name": f"OR_ABI-L2-MCMIPF-M6_G18_s{_DEFAULT_START}_e{_DEFAULT_END}_c{_DEFAULT_CREATED}.nc",
    "naming_authority": "gov.nesdis.noaa",
    "platform_ID": "G18",
    "orbital_slot": "GOES-East",
    "instrument_type": "GOES-R Series Advanced Baseline Imager (ABI)",
    "instrument_ID": "FM3",
    "scene_id": "Full Disk",
    "timeline_id": "ABI Mode 6",
    "spatial_resolution": "2km at nadir",
    "time_coverage_start": "2024-10-10T20:40:20.3Z",
    "time_coverage_end": "2024-10-10T20:49:52.3Z",
    "date_created": "2024-10-10T20:50:00.0Z",
    "production_site": "NSOF",
    "production_environment": "OE",
    "production_data_source": "Realtime",
    "processing_level": "National Aeronautics and Space Administration (NASA)",
    "Conventions": "CF-1.7",
    "Metadata_Conventions": "Unidata Dataset Discovery v1.0",
    "standard_name_vocabulary": "CF Standard Name Table (v35, 20 July 2016)",
    "title": "ABI L2 Cloud and Moisture Imagery",
    "summary": "Multiple reflectance and emissive channel Cloud and Moisture Imagery",
    "institution": "DOC/NOAA/NESDIS",
    "project": "GOES",
    "license": "Unclassified data.",
    "keywords": "ATMOSPHERE > ATMOSPHERIC RADIATION > REFLECTANCE",
    "keywords_vocabulary": "NASA Global Change Master Directory (GCMD)",
    "cdm_data_type": "Image",
    "iso_series_metadata_id": "8c9e8150-3692-11e3-aa6e-0800200c9a66",
}

# Tiny spatial dims (the catalog never reads array data, so size is irrelevant)
_NY = 4
_NX = 4


def _build_goes_filename(
    scene=_DEFAULT_SCENE,
    mode=_DEFAULT_MODE,
    satellite=_DEFAULT_SATELLITE,
    start=_DEFAULT_START,
    end=_DEFAULT_END,
    created=_DEFAULT_CREATED,
):
    return f"OR_ABI-L2-MCMIP{scene}-M{mode}_G{satellite}_s{start}_e{end}_c{created}.nc"


def _build_valid_dataset(
    override_attrs=None,
    drop_attrs=None,
    drop_vars=None,
    corrupt_time=False,
    include_cmi=True,
    ny=_NY,
    nx=_NX,
):
    """
    Build a complete, valid GOES MCMIP xarray Dataset at tiny spatial resolution.

    Parameters
    ----------
    override_attrs : dict, optional
        Keys to override in global attrs. Overrides are applied after defaults.
    drop_attrs : list[str], optional
        Attribute keys to remove entirely from the dataset.
    drop_vars : list[str], optional
        Variable names to remove from the dataset after construction.
    corrupt_time : bool
        If True, set the t coordinate to a non-datetime value.
    include_cmi : bool
        If True, include CMI_CXX and DQF_CXX arrays. Set False for a lighter dataset
        when you only care about scalar metadata.
    ny, nx : int
        Spatial dimension sizes.
    """
    rng = np.random.default_rng(787)

    # ---- coordinates ----
    y = np.linspace(0.1518, -0.1518, ny, dtype=np.float32)
    x = np.linspace(-0.1518, 0.1518, nx, dtype=np.float32)

    coords = {
        "y": ("y", y),
        "x": ("x", x),
        "y_image": np.float32(0.0),
        "x_image": np.float32(0.0),
    }

    # t coordinate
    if corrupt_time:
        # Store a plain string so that pd.Timestamp / np.datetime64 parsing breaks
        coords["t"] = "not-a-timestamp"
    else:
        coords["t"] = np.datetime64("2024-10-10T20:40:20", "ns")

    # Band wavelength and band_id coordinates for all 16 bands
    # Approximate central wavelengths in micrometers
    wavelengths = [
        0.47, 0.64, 0.86, 1.37, 1.61, 2.24,
        3.90, 6.19, 6.95, 7.34, 8.50, 9.61,
        10.35, 11.20, 12.30, 13.30,
    ]
    for i, wl in enumerate(wavelengths, start=1):
        band_str = f"C{i:02d}"
        coords[f"band_wavelength_{band_str}"] = ("band", np.array([wl], dtype=np.float32))
        coords[f"band_id_{band_str}"] = ("band", np.array([i], dtype=np.int8))

    # ---- data variables ----
    data_vars = {}

    # Scalar metadata variables
    data_vars["goes_imager_projection"] = xr.DataArray(
        np.int32(0),
        attrs={
            "perspective_point_height": 35786023.0,
            "longitude_of_projection_origin": -75.0,
        },
    )
    data_vars["time_bounds"] = ("number_of_time_bounds", np.array([
        np.datetime64("2024-10-10T20:40:20", "ns"),
        np.datetime64("2024-10-10T20:49:52", "ns"),
    ]))
    data_vars["y_image_bounds"] = ("number_of_image_bounds", np.array([0.1518, -0.1518], dtype=np.float32))
    data_vars["x_image_bounds"] = ("number_of_image_bounds", np.array([-0.1518, 0.1518], dtype=np.float32))
    data_vars["nominal_satellite_subpoint_lat"] = np.float32(0.0)
    data_vars["nominal_satellite_subpoint_lon"] = np.float32(-75.0)
    data_vars["nominal_satellite_height"] = np.float32(35786.023)
    data_vars["geospatial_lat_lon_extent"] = np.float32(0.0)
    data_vars["dynamic_algorithm_input_data_container"] = np.int32(0)
    data_vars["algorithm_product_version_container"] = np.int32(0)

    # Per-band scalar statistics
    for band in range(1, 17):
        band_str = f"C{band:02d}"

        if band <= 6:
            # Reflective bands: reflectance factor stats
            base = rng.uniform(0.0, 0.5)
            data_vars[f"min_reflectance_factor_{band_str}"] = np.float32(base)
            data_vars[f"max_reflectance_factor_{band_str}"] = np.float32(base + 0.3)
            data_vars[f"mean_reflectance_factor_{band_str}"] = np.float32(base + 0.15)
            data_vars[f"std_dev_reflectance_factor_{band_str}"] = np.float32(0.05)
        else:
            # Emissive bands: brightness temperature stats
            base = rng.uniform(200.0, 300.0)
            data_vars[f"min_brightness_temperature_{band_str}"] = np.float32(base)
            data_vars[f"max_brightness_temperature_{band_str}"] = np.float32(base + 30.0)
            data_vars[f"mean_brightness_temperature_{band_str}"] = np.float32(base + 15.0)
            data_vars[f"std_dev_brightness_temperature_{band_str}"] = np.float32(3.0)

        data_vars[f"outlier_pixel_count_{band_str}"] = np.float64(rng.integers(0, 100))

        # CMI and DQF arrays at tiny resolution
        if include_cmi:
            data_vars[f"CMI_{band_str}"] = (("y", "x"), rng.random((ny, nx)).astype(np.float32))
            data_vars[f"DQF_{band_str}"] = (("y", "x"), rng.integers(0, 4, size=(ny, nx)).astype(np.float32))

    # Data quality scalars
    data_vars["percent_uncorrectable_GRB_errors"] = np.float32(0.001)
    data_vars["percent_uncorrectable_L0_errors"] = np.float32(0.002)

    # ---- assemble dataset ----
    ds = xr.Dataset(data_vars=data_vars, coords=coords)

    # Global attributes
    attrs = dict(_DEFAULT_ATTRS)
    if override_attrs:
        attrs.update(override_attrs)
    if drop_attrs:
        for key in drop_attrs:
            attrs.pop(key, None)
    ds.attrs = attrs

    # Drop variables if requested
    if drop_vars:
        ds = ds.drop_vars([v for v in drop_vars if v in ds])

    return ds


@pytest.fixture
def goes_factory(tmp_path):
    """
    Factory fixture that writes a GOES MCMIP NetCDF to tmp_path and returns the file Path.

    Parameters match _build_valid_dataset plus filename controls.

    Usage in tests:
        def test_valid_scan(goes_factory):
            nc_path = goes_factory()
            result = catalog.scan_file(nc_path)
            assert result is not None

        def test_bad_platform(goes_factory):
            nc_path = goes_factory(override_attrs={"platform_ID": "FAKE"})
            result = catalog.scan_file(nc_path)
            assert result is None
    """
    created_files = []

    def _create(
        *,
        output_dir=None,
        filename=None,
        filename_override=None,
        scene=_DEFAULT_SCENE,
        mode=_DEFAULT_MODE,
        satellite=_DEFAULT_SATELLITE,
        override_attrs=None,
        drop_attrs=None,
        drop_vars=None,
        corrupt_time=False,
        include_cmi=True,
    ):
        """
        Parameters
        ----------
        filename : str, optional
            Explicit full filename. If not provided, built from scene/mode/satellite.
        filename_override : str, optional
            Non-compliant filename for testing filename validation failures.
            Takes precedence over filename and the generated name.
        scene, mode, satellite : str
            Components for building a valid GOES filename.
        override_attrs, drop_attrs, drop_vars, corrupt_time, include_cmi
            Passed through to _build_valid_dataset.
        """
        # Build the dataset
        ds = _build_valid_dataset(
            override_attrs=override_attrs,
            drop_attrs=drop_attrs,
            drop_vars=drop_vars,
            corrupt_time=corrupt_time,
            include_cmi=include_cmi,
        )

        # Determine filename
        if filename_override:
            name = filename_override
        elif filename:
            name = filename
        else:
            name = _build_goes_filename(scene=scene, mode=mode, satellite=satellite)

        target_dir = output_dir if output_dir is not None else tmp_path  # <-- only change
        nc_path = target_dir / name
        ds.to_netcdf(nc_path)
        created_files.append(nc_path)
        return nc_path

    return _create




# ---------------------------------------------------------------------------
# Multi-file fixtures for GOESMultiCloudObservation tests
# ---------------------------------------------------------------------------
# The fixtures above (`goes_factory`, `_build_valid_dataset`, etc.) build
# individual files and are used by both the catalog tests and the
# GOESMultiCloudObservation tests. The fixtures below build collections of
# files (flat directories, nested archives) specifically for exercising
# GOESMultiCloudObservation's file-discovery, sorting, validation-sampling,
# and multi-file concatenation behavior.

from datetime import datetime, timedelta


def _timestamp_str(dt: datetime) -> str:
    """Build the 14-char GOES timestamp: YYYY + DOY(3) + HHMMSS + tenth-of-sec.

    Matches what get_nc_files parses: it strips the last digit as
    tenth-of-second and parses the first 13 chars as %Y%j%H%M%S.
    """
    return dt.strftime("%Y%j%H%M%S") + str(dt.microsecond // 100_000)


def _iso_z(dt: datetime) -> str:
    """ISO timestamp with tenth-of-second precision and Z suffix."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + str(dt.microsecond // 100_000) + "Z"


def _make_file_for_timestamp(
    output_dir: Path,
    start: datetime,
    *,
    scan_duration_sec: int = 572,
    create_delay_sec: int = 10,
    satellite: str = _DEFAULT_SATELLITE,
    scene: str = _DEFAULT_SCENE,
    mode: str = _DEFAULT_MODE,
    override_attrs: dict | None = None,
) -> Path:
    """Build a single valid GOES file whose filename AND global attrs are
    consistent with `start`. Used to assemble multi-file fixtures where each
    file needs a distinct, self-consistent timestamp.
    """
    end = start + timedelta(seconds=scan_duration_sec)
    created = end + timedelta(seconds=create_delay_sec)

    s_str = _timestamp_str(start)
    e_str = _timestamp_str(end)
    c_str = _timestamp_str(created)

    filename = _build_goes_filename(
        scene=scene, mode=mode, satellite=satellite,
        start=s_str, end=e_str, created=c_str,
    )

    # Keep filename and attrs in sync so time_range, time_coverage_start,
    # validate_consistency etc. all see matching values.
    per_file_attrs = {
        "dataset_name": filename,
        "time_coverage_start": _iso_z(start),
        "time_coverage_end": _iso_z(end),
        "date_created": _iso_z(created),
        "id": str(uuid.uuid4()),
    }
    if override_attrs:
        per_file_attrs.update(override_attrs)

    ds = _build_valid_dataset(override_attrs=per_file_attrs)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename
    ds.to_netcdf(path)
    return path


def _archive_timestamps() -> list[datetime]:
    """20 timestamps spanning 2 years, multiple DOYs, multiple hours.

    Crosses the 2023->2024 year boundary so sorting must handle it correctly.
    """
    return [
        # Late Dec 2023, various DOYs and hours
        datetime(2023, 12, 28,  3, 10, 20, 300_000),
        datetime(2023, 12, 28, 11, 40, 20, 300_000),
        datetime(2023, 12, 28, 18, 10, 20, 300_000),
        datetime(2023, 12, 29,  6, 20, 20, 300_000),
        datetime(2023, 12, 29, 14, 50, 20, 300_000),
        datetime(2023, 12, 30,  1, 10, 20, 300_000),
        datetime(2023, 12, 30, 12, 30, 20, 300_000),
        datetime(2023, 12, 30, 22, 40, 20, 300_000),
        datetime(2023, 12, 31,  9, 20, 20, 300_000),
        datetime(2023, 12, 31, 20, 50, 20, 300_000),
        # Early Jan 2024
        datetime(2024,  1,  1,  0, 30, 20, 300_000),
        datetime(2024,  1,  1, 10, 10, 20, 300_000),
        datetime(2024,  1,  1, 19, 40, 20, 300_000),
        datetime(2024,  1,  2,  4, 20, 20, 300_000),
        datetime(2024,  1,  2, 13, 50, 20, 300_000),
        datetime(2024,  1,  2, 23,  0, 20, 300_000),
        datetime(2024,  1,  3,  7, 30, 20, 300_000),
        datetime(2024,  1,  3, 15, 10, 20, 300_000),
        datetime(2024,  1,  3, 21, 40, 20, 300_000),
        datetime(2024,  1,  4,  5, 20, 20, 300_000),
    ]


@pytest.fixture
def goes_archive_dir(tmp_path):
    """A nested archive of 20 valid GOES files spanning 2 years.

    Layout: tmp_path/YYYY/DOY/HH/OR_ABI-L2-MCMIPF-M6_G18_s..._e..._c....nc

    Useful for exercising:
      - get_nc_files(recursive=True) against a realistic directory tree
      - sort=True ordering across a year boundary, multiple DOYs, and hours
      - _validate_nc_files sampling (default sample_size=5 over 20 files)

    Returns
    -------
    Path
        The root directory containing the nested archive.
    """
    for start in _archive_timestamps():
        subdir = tmp_path / f"{start.year}" / f"{start.timetuple().tm_yday:03d}" / f"{start.hour:02d}"
        _make_file_for_timestamp(subdir, start)
    return tmp_path


@pytest.fixture
def goes_archive_files(goes_archive_dir):
    """The same 20-file archive as goes_archive_dir, returned as a sorted list.

    Sorted by the start timestamp string embedded in the filename, ascending.
    Since timestamps are zero-padded YYYY-DOY-HHMMSS-tenths, lexicographic
    sort agrees with chronological sort. This serves as an independent
    oracle for testing the class's own sort logic.
    """
    files = list(goes_archive_dir.rglob("*.nc"))
    files.sort(key=lambda p: p.name.split("_s", 1)[1])
    return files


@pytest.fixture
def goes_flat_dir(tmp_path):
    """Three valid GOES files at 10-minute intervals, flat in tmp_path.

    Cheaper than goes_archive_dir. Use this for tests that need multiple
    files but don't care about directory structure (concatenation behavior,
    __iter__, __getitem__, time_range across files, etc.).
    """
    base = datetime(2024, 10, 10, 20, 40, 20, 300_000)
    paths = [
        _make_file_for_timestamp(tmp_path, base + timedelta(minutes=10 * i))
        for i in range(3)
    ]
    return paths


@pytest.fixture
def goes_mixed_depth_dir(tmp_path):
    """Files at varying depths under tmp_path.

    For testing that recursive=True finds them all and recursive=False finds
    only the top-level ones.

    Layout:
        tmp_path/top1.nc
        tmp_path/top2.nc
        tmp_path/sub_a/mid.nc
        tmp_path/sub_a/sub_b/deep.nc

    Returns
    -------
    dict
        {'top': [Path, Path], 'nested': [Path, Path],
         'all': [Path, ...], 'root': Path}
    """
    base = datetime(2024, 6, 15, 12, 0, 20, 300_000)

    top = [
        _make_file_for_timestamp(tmp_path, base),
        _make_file_for_timestamp(tmp_path, base + timedelta(minutes=10)),
    ]
    nested = [
        _make_file_for_timestamp(tmp_path / "sub_a", base + timedelta(minutes=20)),
        _make_file_for_timestamp(tmp_path / "sub_a" / "sub_b", base + timedelta(minutes=30)),
    ]
    return {"top": top, "nested": nested, "all": top + nested, "root": tmp_path}