from __future__ import annotations

import numpy as np
import pytest

from goesdatabuilder.store.datasets import GOESZarrStore


# ---------------------------------------------------------------------------
# Shared config & fixtures
# ---------------------------------------------------------------------------

MINIMAL_CODEC = {
    "compressor": {"codec": None},
    "serializer": {"codec": None},
    "filter": {"codec": None},
    "chunks": "auto",
    "shards": None,
    "fill_value": None,
}

MEMORY_STORE_CONFIG = {
    "store": {"type": "memory", "path": None, "storage_options": {}},
    "zarr": {
        "field": MINIMAL_CODEC,
        "coordinate": {**MINIMAL_CODEC, "chunks": [512]},
    },
}

_LAT = np.linspace(20.0, 21.0, 5)
_LON = np.linspace(-80.0, -79.0, 5)
_BANDS = [1, 7]
_REGION = "GOES-East"


def _make_store() -> GOESZarrStore:
    """Return a GOESZarrStore with store created but not yet initialized."""
    store = GOESZarrStore(
        store=MEMORY_STORE_CONFIG["store"],
        zarr=MEMORY_STORE_CONFIG["zarr"],
    )
    store.create_store()
    return store


def _make_initialized_store() -> GOESZarrStore:
    """Return a GOESZarrStore with initialize_store called."""
    store = _make_store()
    store.initialize_store()
    return store


def _make_region_store(bands: list[int] = None) -> GOESZarrStore:
    """Return a store with GOES-East initialized at a 5x5 grid."""
    store = _make_initialized_store()
    store.initialize_region(_REGION, _LAT, _LON, bands=bands or _BANDS)
    return store


def _cmi(bands: list[int] = None, shape: tuple = (5, 5)) -> dict:
    """Minimal CMI data dict keyed by band number."""
    rng = np.random.default_rng(0)
    bands = bands or _BANDS
    return {b: rng.random(shape).astype(np.float32) for b in bands}


def _dqf(bands: list[int] = None, shape: tuple = (5, 5)) -> dict:
    """Minimal DQF data dict keyed by band number."""
    rng = np.random.default_rng(1)
    bands = bands or _BANDS
    return {b: rng.integers(0, 4, size=shape).astype(np.uint8) for b in bands}


_T0 = np.datetime64("2024-10-10T20:40:20", "ns")
_T1 = np.datetime64("2024-10-10T20:50:20", "ns")
_T2 = np.datetime64("2024-10-10T21:00:20", "ns")


# ---------------------------------------------------------------------------
# initialize_store
# ---------------------------------------------------------------------------

class TestInitializeStore:
    def test_store_is_open_after_initialize(self):
        """initialize_store leaves the store open."""
        store = _make_store()
        store.initialize_store()
        assert store.is_open is True

    def test_root_attrs_contain_conventions(self):
        """initialize_store writes CF Conventions to root attrs."""
        store = _make_initialized_store()
        attrs = store.get_attrs("/")
        assert "Conventions" in attrs

    def test_root_attrs_contain_title(self):
        """initialize_store writes a title to root attrs."""
        store = _make_initialized_store()
        assert "title" in store.get_attrs("/")

    def test_root_attrs_contain_date_created(self):
        """initialize_store writes a date_created timestamp."""
        store = _make_initialized_store()
        assert "date_created" in store.get_attrs("/")

    def test_root_attrs_contain_history(self):
        """initialize_store seeds a history entry."""
        store = _make_initialized_store()
        assert "history" in store.get_attrs("/")


# ---------------------------------------------------------------------------
# initialize_region
# ---------------------------------------------------------------------------

class TestInitializeRegion:
    def test_region_group_created(self):
        """initialize_region creates the region group."""
        store = _make_region_store()
        assert store.group_exists(_REGION)

    def test_lat_array_created(self):
        """initialize_region creates a lat coordinate array."""
        store = _make_region_store()
        assert store.array_exists(f"{_REGION}/lat")

    def test_lon_array_created(self):
        """initialize_region creates a lon coordinate array."""
        store = _make_region_store()
        assert store.array_exists(f"{_REGION}/lon")

    def test_time_array_created(self):
        """initialize_region creates an empty time coordinate array."""
        store = _make_region_store()
        assert store.array_exists(f"{_REGION}/time")

    def test_time_array_starts_empty(self):
        """Time array has shape (0,) before any observations are appended."""
        store = _make_region_store()
        assert store.get_array(f"{_REGION}/time").shape == (0,)

    def test_platform_id_array_created(self):
        """initialize_region creates a platform_id auxiliary array."""
        store = _make_region_store()
        assert store.array_exists(f"{_REGION}/platform_id")

    def test_scan_mode_array_created(self):
        """initialize_region creates a scan_mode auxiliary array."""
        store = _make_region_store()
        assert store.array_exists(f"{_REGION}/scan_mode")

    def test_cmi_arrays_created_for_requested_bands(self):
        """CMI arrays are created for each band in the bands list."""
        store = _make_region_store(bands=[1, 7])
        assert store.array_exists(f"{_REGION}/CMI_C01")
        assert store.array_exists(f"{_REGION}/CMI_C07")

    def test_dqf_arrays_created_for_requested_bands(self):
        """DQF arrays are created for each band when include_dqf=True."""
        store = _make_region_store(bands=[1, 7])
        assert store.array_exists(f"{_REGION}/DQF_C01")
        assert store.array_exists(f"{_REGION}/DQF_C07")

    def test_cmi_array_initial_shape(self):
        """CMI array has shape (0, lat, lon) before observations."""
        store = _make_region_store()
        arr = store.get_array(f"{_REGION}/CMI_C01")
        assert arr.shape == (0, len(_LAT), len(_LON))

    def test_lat_values_written(self):
        """Lat array contains the values passed to initialize_region."""
        store = _make_region_store()
        np.testing.assert_array_almost_equal(
            store.get_array(f"{_REGION}/lat")[:], _LAT
        )

    def test_lon_values_written(self):
        """Lon array contains the values passed to initialize_region."""
        store = _make_region_store()
        np.testing.assert_array_almost_equal(
            store.get_array(f"{_REGION}/lon")[:], _LON
        )

    def test_region_attrs_contain_geospatial_bounds(self):
        """Region group attrs contain geospatial lat/lon bounds."""
        store = _make_region_store()
        attrs = store.get_attrs(_REGION)
        assert "geospatial_lat_min" in attrs
        assert "geospatial_lat_max" in attrs
        assert "geospatial_lon_min" in attrs
        assert "geospatial_lon_max" in attrs

    def test_region_cache_populated(self):
        """_region_shapes and _region_bands are populated after initialize_region."""
        store = _make_region_store()
        assert _REGION in store._region_shapes
        assert store._region_shapes[_REGION] == (len(_LAT), len(_LON))
        assert store._region_bands[_REGION] == set(_BANDS)

    def test_raises_for_invalid_region(self):
        """initialize_region raises ValueError for an unrecognised region name."""
        store = _make_initialized_store()
        with pytest.raises(ValueError):
            store.initialize_region("FAKE-SLOT", _LAT, _LON)

    def test_raises_for_non_monotonic_lat(self):
        """initialize_region raises ValueError when lat is not monotonic."""
        store = _make_initialized_store()
        bad_lat = np.array([20.0, 21.0, 19.0, 22.0, 23.0])
        with pytest.raises(ValueError, match="[Ll]atitude"):
            store.initialize_region(_REGION, bad_lat, _LON)

    def test_no_dqf_when_include_dqf_false(self):
        """DQF arrays are not created when include_dqf=False."""
        store = _make_initialized_store()
        store.initialize_region(_REGION, _LAT, _LON, bands=[1], include_dqf=False)
        assert not store.array_exists(f"{_REGION}/DQF_C01")
        assert store.array_exists(f"{_REGION}/CMI_C01")


# ---------------------------------------------------------------------------
# append_observation
# ---------------------------------------------------------------------------

class TestAppendObservation:
    def test_returns_int_time_index(self):
        """append_observation returns an integer time index."""
        store = _make_region_store()
        idx = store.append_observation(_REGION, _T0, "G18", _cmi())
        assert isinstance(idx, (int, np.integer))

    def test_first_observation_returns_index_zero(self):
        """First append returns time index 0."""
        store = _make_region_store()
        idx = store.append_observation(_REGION, _T0, "G18", _cmi())
        assert int(idx) == 0

    def test_second_observation_returns_index_one(self):
        """Second append returns time index 1."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        idx = store.append_observation(_REGION, _T1, "G18", _cmi())
        assert int(idx) == 1

    def test_time_array_grows(self):
        """Time array length increases by one after each append."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        assert store.get_array(f"{_REGION}/time").shape == (1,)
        store.append_observation(_REGION, _T1, "G18", _cmi())
        assert store.get_array(f"{_REGION}/time").shape == (2,)

    def test_cmi_time_axis_grows(self):
        """CMI array time axis grows by one after each append."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        assert store.get_array(f"{_REGION}/CMI_C01").shape[0] == 1

    def test_cmi_spatial_shape_preserved(self):
        """CMI spatial dimensions are unchanged after append."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        arr = store.get_array(f"{_REGION}/CMI_C01")
        assert arr.shape[1] == len(_LAT)
        assert arr.shape[2] == len(_LON)

    def test_platform_id_written(self):
        """platform_id is stored correctly after append."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        assert store.get_array(f"{_REGION}/platform_id")[0] == "G18"

    def test_scan_mode_written(self):
        """scan_mode is stored correctly after append."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi(), scan_mode="6")
        assert store.get_array(f"{_REGION}/scan_mode")[0] == "6"

    def test_scan_mode_defaults_to_unknown(self):
        """scan_mode defaults to 'unknown' when not provided."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        assert store.get_array(f"{_REGION}/scan_mode")[0] == "unknown"

    def test_dqf_written_when_provided(self):
        """DQF data is written when dqf_data is provided."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi(), dqf_data=_dqf())
        assert store.get_array(f"{_REGION}/DQF_C01").shape[0] == 1

    def test_raises_for_wrong_cmi_shape(self):
        """append_observation raises ValueError when CMI shape does not match region."""
        store = _make_region_store()
        bad_cmi = {1: np.ones((3, 3), dtype=np.float32),
                   7: np.ones((3, 3), dtype=np.float32)}
        with pytest.raises(ValueError):
            store.append_observation(_REGION, _T0, "G18", bad_cmi)

    def test_raises_for_uninitialised_region(self):
        """append_observation raises KeyError when region has not been initialized."""
        store = _make_initialized_store()
        with pytest.raises(KeyError):
            store.append_observation(_REGION, _T0, "G18", _cmi())

    def test_raises_for_unknown_band(self):
        """append_observation raises KeyError when a band is not in the region."""
        store = _make_region_store(bands=[1])
        bad_cmi = {1: np.ones((5, 5), dtype=np.float32),
                   99: np.ones((5, 5), dtype=np.float32)}
        with pytest.raises(KeyError):
            store.append_observation(_REGION, _T0, "G18", bad_cmi)

    def test_cmi_values_written_correctly(self):
        """CMI values written by append_observation match the input array."""
        store = _make_region_store()
        cmi = _cmi()
        store.append_observation(_REGION, _T0, "G18", cmi)
        stored = store.get_array(f"{_REGION}/CMI_C01")[0]
        np.testing.assert_array_almost_equal(stored, cmi[1])


# ---------------------------------------------------------------------------
# append_batch
# ---------------------------------------------------------------------------

class TestAppendBatch:
    def _make_obs(self, timestamp, platform="G18", scan_mode="6"):
        return {
            "timestamp": timestamp,
            "platform_id": platform,
            "scan_mode": scan_mode,
            "cmi_data": _cmi(),
            "dqf_data": _dqf(),
        }

    def test_empty_batch_returns_zero_zero(self):
        """append_batch with an empty list returns (0, 0)."""
        store = _make_region_store()
        result = store.append_batch(_REGION, [])
        assert result == (0, 0)

    def test_returns_start_end_indices(self):
        """append_batch returns (start_idx, end_idx) tuple."""
        store = _make_region_store()
        obs = [self._make_obs(_T0), self._make_obs(_T1)]
        start, end = store.append_batch(_REGION, obs)
        assert start == 0
        assert end == 2

    def test_second_batch_indices_continue(self):
        """Second append_batch starts where the first ended."""
        store = _make_region_store()
        store.append_batch(_REGION, [self._make_obs(_T0)])
        start, end = store.append_batch(_REGION, [self._make_obs(_T1), self._make_obs(_T2)])
        assert start == 1
        assert end == 3

    def test_time_array_length_correct(self):
        """Time array length equals the total number of appended observations."""
        store = _make_region_store()
        store.append_batch(_REGION, [self._make_obs(_T0), self._make_obs(_T1)])
        assert store.get_array(f"{_REGION}/time").shape == (2,)

    def test_cmi_time_axis_correct(self):
        """CMI time axis equals the number of appended observations."""
        store = _make_region_store()
        store.append_batch(_REGION, [self._make_obs(_T0), self._make_obs(_T1)])
        assert store.get_array(f"{_REGION}/CMI_C01").shape[0] == 2

    def test_raises_for_uninitialised_region(self):
        """append_batch raises KeyError when the region has not been initialized."""
        store = _make_initialized_store()
        with pytest.raises(KeyError):
            store.append_batch(_REGION, [self._make_obs(_T0)])

    def test_raises_for_wrong_shape(self):
        """append_batch raises ValueError when CMI shape does not match region."""
        store = _make_region_store()
        bad_obs = [{
            "timestamp": _T0,
            "platform_id": "G18",
            "cmi_data": {1: np.ones((3, 3), dtype=np.float32),
                         7: np.ones((3, 3), dtype=np.float32)},
        }]
        with pytest.raises(ValueError):
            store.append_batch(_REGION, bad_obs)

    def test_raises_for_inconsistent_bands_across_observations(self):
        """append_batch raises ValueError when band sets differ across observations."""
        store = _make_region_store()
        obs1 = self._make_obs(_T0)
        obs2 = {
            "timestamp": _T1,
            "platform_id": "G18",
            "cmi_data": {1: np.ones((5, 5), dtype=np.float32)},
        }
        with pytest.raises((ValueError, KeyError)):
            store.append_batch(_REGION, [obs1, obs2])

    def test_platform_ids_written(self):
        """Platform IDs are written correctly for each observation in the batch."""
        store = _make_region_store()
        obs = [self._make_obs(_T0, platform="G16"),
               self._make_obs(_T1, platform="G18")]
        store.append_batch(_REGION, obs)
        platforms = list(store.get_array(f"{_REGION}/platform_id")[:])
        assert "G16" in platforms
        assert "G18" in platforms


# ---------------------------------------------------------------------------
# query methods
# ---------------------------------------------------------------------------

class TestGetTimeRange:
    def test_returns_none_when_empty(self):
        """get_time_range returns None when no observations have been appended."""
        store = _make_region_store()
        assert store.get_time_range(_REGION) is None

    def test_returns_tuple_after_one_observation(self):
        """get_time_range returns a two-element tuple after one observation."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        result = store.get_time_range(_REGION)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_start_equals_end_for_single_observation(self):
        """For a single observation, start and end timestamps are equal."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        start, end = store.get_time_range(_REGION)
        assert start == end

    def test_range_spans_all_observations(self):
        """get_time_range spans from first to last appended timestamp."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        store.append_observation(_REGION, _T2, "G18", _cmi())
        start, end = store.get_time_range(_REGION)
        assert np.datetime64(start, "ns") == _T0
        assert np.datetime64(end, "ns") == _T2

    def test_raises_for_invalid_region(self):
        """get_time_range raises ValueError for an unrecognised region."""
        store = _make_region_store()
        with pytest.raises(ValueError):
            store.get_time_range("FAKE-SLOT")


class TestGetObservationCount:
    def test_zero_before_any_observations(self):
        """get_observation_count returns 0 before any observations."""
        store = _make_region_store()
        assert store.get_observation_count(_REGION) == 0

    def test_increments_after_each_append(self):
        """get_observation_count increments by one after each append."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        assert store.get_observation_count(_REGION) == 1
        store.append_observation(_REGION, _T1, "G18", _cmi())
        assert store.get_observation_count(_REGION) == 2

    def test_reflects_batch_append(self):
        """get_observation_count reflects the total after append_batch."""
        store = _make_region_store()
        obs = [
            {"timestamp": _T0, "platform_id": "G18",
             "cmi_data": _cmi(), "dqf_data": _dqf()},
            {"timestamp": _T1, "platform_id": "G18",
             "cmi_data": _cmi(), "dqf_data": _dqf()},
        ]
        store.append_batch(_REGION, obs)
        assert store.get_observation_count(_REGION) == 2

    def test_raises_for_invalid_region(self):
        """get_observation_count raises ValueError for an unrecognised region."""
        store = _make_region_store()
        with pytest.raises(ValueError):
            store.get_observation_count("FAKE-SLOT")


class TestGetSpatialExtent:
    def test_returns_dict_with_required_keys(self):
        """get_spatial_extent returns a dict with all four bound keys."""
        store = _make_region_store()
        result = store.get_spatial_extent(_REGION)
        for key in ("lat_min", "lat_max", "lon_min", "lon_max"):
            assert key in result

    def test_lat_bounds_match_input(self):
        """Lat bounds match the lat array passed to initialize_region."""
        store = _make_region_store()
        result = store.get_spatial_extent(_REGION)
        assert result["lat_min"] == pytest.approx(float(_LAT.min()))
        assert result["lat_max"] == pytest.approx(float(_LAT.max()))

    def test_lon_bounds_match_input(self):
        """Lon bounds match the lon array passed to initialize_region."""
        store = _make_region_store()
        result = store.get_spatial_extent(_REGION)
        assert result["lon_min"] == pytest.approx(float(_LON.min()))
        assert result["lon_max"] == pytest.approx(float(_LON.max()))

    def test_values_are_floats(self):
        """All values in the extent dict are Python floats."""
        store = _make_region_store()
        for val in store.get_spatial_extent(_REGION).values():
            assert isinstance(val, float)

    def test_raises_for_invalid_region(self):
        """get_spatial_extent raises ValueError for an unrecognised region."""
        store = _make_region_store()
        with pytest.raises(ValueError):
            store.get_spatial_extent("FAKE-SLOT")


class TestGetBands:
    def test_returns_list(self):
        """get_bands returns a list."""
        store = _make_region_store()
        assert isinstance(store.get_bands(_REGION), list)

    def test_returns_correct_bands(self):
        """get_bands returns the bands passed to initialize_region."""
        store = _make_region_store(bands=[1, 7])
        assert store.get_bands(_REGION) == [1, 7]

    def test_bands_are_sorted(self):
        """get_bands returns band numbers in sorted order."""
        store = _make_initialized_store()
        store.initialize_region(_REGION, _LAT, _LON, bands=[7, 1, 13])
        assert store.get_bands(_REGION) == [1, 7, 13]

    def test_raises_for_invalid_region(self):
        """get_bands raises ValueError for an unrecognised region."""
        store = _make_region_store()
        with pytest.raises(ValueError):
            store.get_bands("FAKE-SLOT")


class TestGetPlatforms:
    def test_empty_before_observations(self):
        """get_platforms returns an empty list before any observations."""
        store = _make_region_store()
        assert store.get_platforms(_REGION) == []

    def test_returns_unique_platforms(self):
        """get_platforms returns deduplicated platform IDs."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        store.append_observation(_REGION, _T1, "G18", _cmi())
        assert store.get_platforms(_REGION) == ["G18"]

    def test_returns_multiple_platforms_sorted(self):
        """get_platforms returns all unique platforms in sorted order."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        store.append_observation(_REGION, _T1, "G16", _cmi())
        platforms = store.get_platforms(_REGION)
        assert set(platforms) == {"G16", "G18"}
        assert platforms == sorted(platforms)

    def test_raises_for_invalid_region(self):
        """get_platforms raises ValueError for an unrecognised region."""
        store = _make_region_store()
        with pytest.raises(ValueError):
            store.get_platforms("FAKE-SLOT")


class TestRebuildRegionCache:
    def test_cache_repopulated_after_clear(self):
        """rebuild_region_cache repopulates _region_shapes and _region_bands."""
        store = _make_region_store()
        store._region_shapes.clear()
        store._region_bands.clear()
        store.rebuild_region_cache(_REGION)
        assert _REGION in store._region_shapes
        assert _REGION in store._region_bands

    def test_shape_matches_lat_lon(self):
        """Rebuilt cache shape matches the lat/lon arrays."""
        store = _make_region_store()
        store._region_shapes.clear()
        store.rebuild_region_cache(_REGION)
        assert store._region_shapes[_REGION] == (len(_LAT), len(_LON))

    def test_bands_match_initialized_bands(self):
        """Rebuilt cache bands match those passed to initialize_region."""
        store = _make_region_store(bands=[1, 7])
        store._region_bands.clear()
        store.rebuild_region_cache(_REGION)
        assert store._region_bands[_REGION] == {1, 7}

    def test_raises_for_missing_region(self):
        """rebuild_region_cache raises KeyError for a region not in the store."""
        store = _make_initialized_store()
        with pytest.raises(KeyError):
            store.rebuild_region_cache(_REGION)

    def test_append_works_after_rebuild(self):
        """append_observation succeeds after cache is rebuilt from scratch."""
        store = _make_region_store()
        store._region_shapes.clear()
        store._region_bands.clear()
        store.rebuild_region_cache(_REGION)
        idx = store.append_observation(_REGION, _T0, "G18", _cmi())
        assert int(idx) == 0


# ---------------------------------------------------------------------------
# provenance & metadata
# ---------------------------------------------------------------------------

class TestUpdateTemporalCoverage:
    def test_no_op_when_no_observations(self):
        """update_temporal_coverage does not raise when time array is empty."""
        store = _make_region_store()
        store.update_temporal_coverage(_REGION)

    def test_updates_time_coverage_start(self):
        """time_coverage_start is updated after appending an observation."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        store.update_temporal_coverage(_REGION)
        attrs = store.get_attrs("/")
        assert "time_coverage_start" in attrs
        assert attrs["time_coverage_start"] is not None

    def test_updates_time_coverage_end(self):
        """time_coverage_end is updated after appending an observation."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        store.update_temporal_coverage(_REGION)
        attrs = store.get_attrs("/")
        assert "time_coverage_end" in attrs

    def test_updates_date_modified(self):
        """date_modified is updated in root attrs."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        store.update_temporal_coverage(_REGION)
        assert "date_modified" in store.get_attrs("/")


class TestAddProcessingHistory:
    def test_adds_history_entry(self):
        """add_processing_history appends a new entry to history."""
        store = _make_initialized_store()
        store.add_processing_history("Test step completed")
        history = store.get_attrs("/")["history"]
        assert "Test step completed" in history

    def test_preserves_existing_history(self):
        """Second call appends without overwriting the first entry."""
        store = _make_initialized_store()
        store.add_processing_history("Step 1")
        store.add_processing_history("Step 2")
        history = store.get_attrs("/")["history"]
        assert "Step 1" in history
        assert "Step 2" in history

    def test_history_includes_timestamp(self):
        """Each history entry includes a timestamp."""
        store = _make_initialized_store()
        store.add_processing_history("Timestamped step")
        history = store.get_attrs("/")["history"]
        assert "Z" in history or "T" in history


class TestAddSourceFiles:
    def test_adds_source_files(self):
        """add_source_files writes file paths to region attrs."""
        store = _make_region_store()
        store.add_source_files(_REGION, ["file_a.nc", "file_b.nc"])
        attrs = store.get_attrs(_REGION)
        assert "source_files" in attrs
        assert "file_a.nc" in attrs["source_files"]

    def test_source_file_count_correct(self):
        """source_file_count reflects the number of unique files."""
        store = _make_region_store()
        store.add_source_files(_REGION, ["a.nc", "b.nc", "c.nc"])
        assert store.get_attrs(_REGION)["source_file_count"] == 3

    def test_deduplicates_files(self):
        """Calling add_source_files twice with the same file does not duplicate."""
        store = _make_region_store()
        store.add_source_files(_REGION, ["a.nc"])
        store.add_source_files(_REGION, ["a.nc", "b.nc"])
        assert store.get_attrs(_REGION)["source_file_count"] == 2

    def test_accumulates_across_calls(self):
        """Multiple calls accumulate all unique files."""
        store = _make_region_store()
        store.add_source_files(_REGION, ["a.nc"])
        store.add_source_files(_REGION, ["b.nc"])
        attrs = store.get_attrs(_REGION)
        assert "a.nc" in attrs["source_files"]
        assert "b.nc" in attrs["source_files"]


class TestFinalizeDataset:
    def test_does_not_raise_on_empty_store(self):
        """finalize_dataset does not raise when no regions have observations."""
        store = _make_region_store()
        store.finalize_dataset()

    def test_history_updated_after_finalize(self):
        """finalize_dataset adds a finalization entry to history."""
        store = _make_region_store()
        store.finalize_dataset()
        history = store.get_attrs("/")["history"]
        assert "finalized" in history.lower()

    def test_temporal_coverage_updated_for_regions_with_data(self):
        """finalize_dataset updates temporal coverage for regions that have data."""
        store = _make_region_store()
        store.append_observation(_REGION, _T0, "G18", _cmi())
        store.finalize_dataset()
        attrs = store.get_attrs("/")
        assert attrs.get("time_coverage_start") is not None


# ---------------------------------------------------------------------------
# band metadata helpers
# ---------------------------------------------------------------------------

class TestGetBandsForProduct:
    def test_returns_list(self):
        """get_bands_for_product returns a list."""
        store = _make_store()
        assert isinstance(store.get_bands_for_product("Radiances"), list)

    def test_radiances_includes_all_bands(self):
        """Every band supports the 'Radiances' product."""
        store = _make_store()
        bands = store.get_bands_for_product("Radiances")
        assert set(range(1, 17)) == set(bands)

    def test_returns_sorted_list(self):
        """Returned band list is sorted."""
        store = _make_store()
        bands = store.get_bands_for_product("Radiances")
        assert bands == sorted(bands)

    def test_unknown_product_returns_empty(self):
        """An unrecognised product name returns an empty list."""
        store = _make_store()
        assert store.get_bands_for_product("NonExistentProduct") == []

    def test_fire_product_bands(self):
        """Fire/hotspot characterization is supported by known bands."""
        store = _make_store()
        bands = store.get_bands_for_product("Fire/hotspot characterization")
        assert len(bands) > 0
        assert all(1 <= b <= 16 for b in bands)


class TestGetProductsForBand:
    def test_returns_list(self):
        """get_products_for_band returns a list."""
        store = _make_store()
        assert isinstance(store.get_products_for_band(1), list)

    def test_band_1_includes_radiances(self):
        """Band 1 supports the Radiances product."""
        store = _make_store()
        assert "Radiances" in store.get_products_for_band(1)

    def test_band_7_includes_fire(self):
        """Band 7 supports fire/hotspot characterization."""
        store = _make_store()
        products = store.get_products_for_band(7)
        assert any("ire" in p for p in products)

    def test_out_of_range_band_returns_empty(self):
        """A band number with no metadata returns an empty list."""
        store = _make_store()
        assert store.get_products_for_band(99) == []

    def test_all_bands_have_at_least_one_product(self):
        """Every band 1-16 has at least one associated product."""
        store = _make_store()
        for band in range(1, 17):
            assert len(store.get_products_for_band(band)) > 0, \
                f"Band {band} has no products"


class TestListAllProducts:
    def test_returns_list(self):
        """list_all_products returns a list."""
        store = _make_store()
        assert isinstance(store.list_all_products(), list)

    def test_returns_sorted_list(self):
        """list_all_products returns products in sorted order."""
        store = _make_store()
        products = store.list_all_products()
        assert products == sorted(products)

    def test_radiances_in_all_products(self):
        """Radiances appears in the list of all products."""
        store = _make_store()
        assert "Radiances" in store.list_all_products()

    def test_no_duplicates(self):
        """list_all_products contains no duplicate entries."""
        store = _make_store()
        products = store.list_all_products()
        assert len(products) == len(set(products))

    def test_non_empty(self):
        """list_all_products returns at least one product."""
        store = _make_store()
        assert len(store.list_all_products()) > 0