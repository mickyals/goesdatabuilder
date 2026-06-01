import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path
from goesdatabuilder.data.goes.multicloudcatalog import GOESMetadataCatalog
from goesdatabuilder.utils.config import ConfigError



# ---- Fixtures ----

@pytest.fixture
def empty_catalog():
    return GOESMetadataCatalog()

@pytest.fixture
def populated_catalog():
    catalog = GOESMetadataCatalog()
    catalog._observations = pd.DataFrame({
        "file_path": ["/a.nc", "/b.nc", "/c.nc", "/d.nc"],
        "platform_id": ["G16", "G18", "G16", "G18"],
        "orbital_slot": ["GOES-East", "GOES-West", "GOES-East", "GOES-West"],
        "scene_id": ["Full Disk", "Full Disk", "CONUS", "CONUS"],
        "time_coverage_start": pd.to_datetime([
            "2024-01-01", "2024-06-15", "2024-03-10", "2024-09-20"
        ]),
        "time_coverage_end": pd.to_datetime([
            "2024-01-01T00:10", "2024-06-15T00:10", "2024-03-10T00:05", "2024-09-20T00:05"
        ]),
        "file_size_mb": [150.0, 200.0, 75.0, 80.0],
    })
    catalog._validation_errors = pd.DataFrame({
        "file_path": ["/bad.nc"],
        "error_message": ["Invalid GOES filename pattern: bad.nc"],
        "timestamp": [pd.Timestamp.now()],
    })
    return catalog


# ---- Tests ----

class Test_init:
    """Test GOESMetadataCatalog.__init__"""

    def test_observations_is_empty_dataframe(self):
        catalog = GOESMetadataCatalog()
        assert isinstance(catalog._observations, pd.DataFrame)
        assert catalog._observations.empty

    def test_band_statistics_is_empty_dataframe(self):
        catalog = GOESMetadataCatalog()
        assert isinstance(catalog._band_statistics, pd.DataFrame)
        assert catalog._band_statistics.empty

    def test_data_quality_is_empty_dataframe(self):
        catalog = GOESMetadataCatalog()
        assert isinstance(catalog._data_quality, pd.DataFrame)
        assert catalog._data_quality.empty

    def test_validation_errors_is_empty_dataframe(self):
        catalog = GOESMetadataCatalog()
        assert isinstance(catalog._validation_errors, pd.DataFrame)
        assert catalog._validation_errors.empty

    def test_pending_errors_is_empty_list(self):
        catalog = GOESMetadataCatalog()
        assert isinstance(catalog._pending_errors, list)
        assert len(catalog._pending_errors) == 0


class Test_scan_file:
    """Test GOESMetadataCatalog.scan_file"""
    def test_valid_scan(self, goes_factory):
        nc_path = goes_factory()
        catalog = GOESMetadataCatalog()
        assert catalog.scan_file(nc_path) is not None

    def test_invalid_scan(self, goes_factory):
        nc_path = "./temp/files"
        catalog = GOESMetadataCatalog()
        assert catalog.scan_file(nc_path) is None

    def test_missing_global_attrs(self, goes_factory):
        nc_path = goes_factory(drop_attrs=["title", "scene_id"])
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)
        assert result  is not None
        global_attrs = result['global_attrs']
        assert global_attrs['title'] is None
        assert global_attrs['scene_id'] is None
        assert global_attrs['platform_id'] is not None

    def test_invalid_time(self, goes_factory):
        nc_path = goes_factory(corrupt_time=True)
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)

        assert result is not None
        global_attrs = result['global_attrs']
        assert global_attrs['time'] is None

    def test_invalid_platform_id(self, goes_factory):
        nc_path = goes_factory(override_attrs={"platform_ID": "UNREAL ENGINE"})
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)
        print(result)

        assert result is None

    def test_missing_cmi(self, goes_factory):
        nc_path = goes_factory(include_cmi=False)
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)
        print(result)
        assert result is not None

    def test_invalid_filename(self, goes_factory):
        nc_path = goes_factory(filename_override="invalid.nc")
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)
        assert result is None

    def test_invalid_orbital_slot(self, goes_factory):
        nc_path = goes_factory(override_attrs={"orbital_slot": "UNREAL ENGINE"})
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)
        assert result is None

    def test_invalid_scene_id(self, goes_factory):
        nc_path = goes_factory(override_attrs={"scene_id": "UNREAL ENGINE"})
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)
        assert result is None

    def test_missing_stats_variable(self, goes_factory):
        nc_path = goes_factory(drop_vars=["mean_brightness_temperature_C11"])
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)
        assert result is not None
        band_stats = result['band_statistics'][10] # C11

        assert band_stats['mean_brightness_temp'] is None

    def test_missing_quality_variable(self, goes_factory):
        nc_path = goes_factory(drop_vars=["percent_uncorrectable_GRB_errors"])
        catalog = GOESMetadataCatalog()
        result = catalog.scan_file(nc_path)
        assert result is not None
        quality = result['data_quality']

        assert quality['grb_errors_percent'] is None



class Test_scan_files:
    """Test GOESMetadataCatalog.scan_files"""

    def test_populates_observations(self, goes_factory):
        nc_path = goes_factory()
        catalog = GOESMetadataCatalog()
        catalog.scan_files([nc_path])
        assert not catalog._observations.empty
        assert len(catalog) == 1

    def test_populates_band_statistics(self, goes_factory):
        nc_path = goes_factory()
        catalog = GOESMetadataCatalog()
        catalog.scan_files([nc_path])
        assert not catalog._band_statistics.empty
        assert len(catalog._band_statistics) == 16

    def test_populates_data_quality(self, goes_factory):
        nc_path = goes_factory()
        catalog = GOESMetadataCatalog()
        catalog.scan_files([nc_path])
        assert not catalog._data_quality.empty

    def test_returns_self(self, goes_factory):
        nc_path = goes_factory()
        catalog = GOESMetadataCatalog()
        result = catalog.scan_files([nc_path])
        assert result is catalog

    def test_time_columns_are_datetime(self, goes_factory):
        nc_path = goes_factory()
        catalog = GOESMetadataCatalog()
        catalog.scan_files([nc_path])
        obs = catalog._observations
        for col in ["time_coverage_start", "time_coverage_end"]:
            if col in obs.columns:
                assert pd.api.types.is_datetime64_any_dtype(obs[col])

    def test_mixed_valid_and_invalid(self, goes_factory, tmp_path):
        valid_path = goes_factory()
        invalid_path = tmp_path / "not_a_goes_file.nc"
        invalid_path.touch()
        catalog = GOESMetadataCatalog()
        catalog.scan_files([valid_path, invalid_path])
        assert len(catalog._observations) == 1
        assert len(catalog._validation_errors) >= 1

    def test_empty_list(self):
        catalog = GOESMetadataCatalog()
        result = catalog.scan_files([])
        assert result is catalog
        assert catalog._observations.empty
        assert catalog._band_statistics.empty

    def test_accumulates_across_calls(self, goes_factory):
        path_a = goes_factory()
        path_b = goes_factory()
        catalog = GOESMetadataCatalog()
        catalog.scan_files([path_a])
        assert len(catalog) == 1
        catalog.scan_files([path_b])
        assert len(catalog) == 2

    def test_tracks_validation_errors(self, goes_factory):
        invalid_path = goes_factory(filename_override="garbage.nc")
        catalog = GOESMetadataCatalog()
        catalog.scan_files([invalid_path])
        assert len(catalog._observations) == 0
        assert len(catalog._validation_errors) == 1

class Test_scan_directory:
    """Test GOESMetadataCatalog.scan_directory"""

    def test_nonexistent_directory_raises(self):
        catalog = GOESMetadataCatalog()
        with pytest.raises(ValueError):
            catalog.scan_directory(Path("/this/does/not/exist"))

    def test_none_directory_raises(self):
        catalog = GOESMetadataCatalog()
        with pytest.raises(ConfigError):
            catalog.scan_directory(None)

    def test_empty_directory(self, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        catalog = GOESMetadataCatalog()
        result = catalog.scan_directory(empty_dir)
        assert result is catalog
        assert catalog._observations.empty

    def test_valid_directory(self, goes_factory, tmp_path):
        # goes_factory should place the file in tmp_path or a known location
        nc_path = goes_factory(output_dir=tmp_path)
        catalog = GOESMetadataCatalog()
        catalog.scan_directory(tmp_path)
        assert len(catalog) >= 1

    def test_returns_self(self, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        catalog = GOESMetadataCatalog()
        result = catalog.scan_directory(empty_dir)
        assert result is catalog

    def test_custom_glob_pattern(self, goes_factory, tmp_path):
        nc_path = goes_factory(output_dir=tmp_path)
        # create a non-nc file that should be ignored
        (tmp_path / "readme.txt").write_text("ignore me")
        catalog = GOESMetadataCatalog()
        catalog.scan_directory(tmp_path, pattern="*.nc")
        assert len(catalog) >= 1
        # confirm the txt file didn't sneak in
        for fp in catalog.get_valid_files():
            assert fp.endswith(".nc")



class Test_csv_methods:
    """Test GOESMetadataCatalog.csv_associated_methods"""

    def test_to_csv_writes_files(self, tmp_path):
        catalog = GOESMetadataCatalog()

        catalog._observations = pd.DataFrame({
            "file_path": ["/a.nc", "/b.nc"],
            "platform_id": ["G16", "G18"],
            "time_coverage_start": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "time_coverage_end": pd.to_datetime(["2024-01-01T00:10", "2024-01-02T00:10"]),
        })

        catalog._band_statistics = pd.DataFrame({
            "observation_id": ["obs1", "obs1"],
            "band": [1, 2],
            "mean_reflectance": [0.35, 0.42],
            "has_cmi": [True, True],
        })

        catalog._data_quality = pd.DataFrame({
            "grb_errors_percent": [0.01],
            "l0_errors_percent": [0.02],
        })

        catalog._validation_errors = pd.DataFrame({
            "file_path": ["/bad.nc"],
            "error_message": ["Invalid GOES filename pattern: bad.nc"],
            "timestamp": [pd.Timestamp.now()],
        })

        catalog.to_csv(tmp_path)

        assert (tmp_path / "observations.csv").exists()
        assert (tmp_path / "band_statistics.csv").exists()
        assert (tmp_path / "global_data_quality.csv").exists()
        assert (tmp_path / "validation_errors.csv").exists()

    def test_to_csv_skips_empty(self, tmp_path):
        catalog = GOESMetadataCatalog()
        catalog._observations = pd.DataFrame({
            "file_path": ["/a.nc"],
            "platform_id": ["G16"],
        })
        catalog.to_csv(tmp_path)

        assert (tmp_path / "observations.csv").exists()
        assert not (tmp_path / "band_statistics.csv").exists()
        assert not (tmp_path / "global_data_quality.csv").exists()
        assert not (tmp_path / "validation_errors.csv").exists()

    def test_to_csv_after_scan(self, goes_factory, tmp_path):
        nc_path = goes_factory()
        catalog = GOESMetadataCatalog()
        catalog.scan_files([nc_path])
        catalog.to_csv(tmp_path)
        assert (tmp_path / "observations.csv").exists()

    def test_from_csv_with_valid_path(self, goes_factory, tmp_path):
        nc_path = goes_factory()
        catalog = GOESMetadataCatalog()
        catalog.scan_files([nc_path])
        catalog.to_csv(tmp_path)
        catalog.from_csv(tmp_path)
        assert catalog._observations is not None

    def test_from_csv_with_no_csvs(self, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        catalog = GOESMetadataCatalog.from_csv(empty_dir)

        assert isinstance(catalog, GOESMetadataCatalog)
        assert isinstance(catalog._observations, pd.DataFrame)
        assert catalog._observations.empty
        assert catalog._band_statistics.empty
        assert catalog._data_quality.empty
        assert catalog._validation_errors.empty


    def test_files_from_csv_valid(self, tmp_path):
        catalog = GOESMetadataCatalog()
        catalog._observations = pd.DataFrame({
            "file_path": ["/a.nc", "/b.nc"],
            "platform_id": ["G16", "G18"],
        })
        catalog.to_csv(tmp_path)

        files = GOESMetadataCatalog.files_from_csv(tmp_path)
        assert len(files) == 2
        assert isinstance(files, list)

    def test_files_from_csv_invalid(self, tmp_path):
        catalog = GOESMetadataCatalog()
        catalog._observations = pd.DataFrame({
            "file_path": ["/a.nc", "/b.nc"],
            "platform_id": ["G16", "G18"],
        })
        catalog.to_csv(tmp_path)

        files = GOESMetadataCatalog.files_from_csv("/not/real/path")
        assert isinstance(files, list)
        assert files == []

    def test_csv_exists_valid_observations_path(self, tmp_path):
        catalog = GOESMetadataCatalog().csv_exists(tmp_path)
        assert catalog == (tmp_path / "observations.csv").exists()

    def test_csv_exists_valid_path_all_files_true(self, tmp_path):
        catalog = GOESMetadataCatalog().csv_exists(tmp_path, all_files=True)
        assert catalog == (tmp_path / "observations.csv").exists()
        assert catalog == (tmp_path / "band_statistics.csv").exists()
        assert catalog == (tmp_path / "global_data_quality.csv").exists()
        assert catalog == (tmp_path / "validation_errors.csv").exists()

    def test_csv_exists_invalid_path(self):
        catalog = GOESMetadataCatalog().csv_exists("/not/real/path")
        assert catalog == False

    def test_csv_exists_invalid_path_all_files(self):
        catalog = GOESMetadataCatalog().csv_exists("/not/real/path", all_files=True)
        assert catalog == False

    def test_append_to_csv_valid_dataframes(self, tmp_path):
        # Create initial catalog and write to CSV
        catalog1 = GOESMetadataCatalog()
        catalog1._observations = pd.DataFrame({
            "file_path": ["/a.nc"],
            "platform_id": ["G16"],
        })
        catalog1.to_csv(tmp_path)

        # Create second catalog with new data
        catalog2 = GOESMetadataCatalog()
        catalog2._observations = pd.DataFrame({
            "file_path": ["/b.nc"],
            "platform_id": ["G18"],
        })
        catalog2.append_to_csv(tmp_path)

        # Verify data was appended
        files = GOESMetadataCatalog.files_from_csv(tmp_path)
        assert len(files) == 2
        assert "/a.nc" in files
        assert "/b.nc" in files

    def test_append_to_csv_incompatible_dataframes(self, tmp_path):
        # Create initial catalog and write to CSV
        catalog1 = GOESMetadataCatalog()
        catalog1._observations = pd.DataFrame({
            "file_path": ["/a.nc"],
            "platform_id": ["G16"],
        })
        catalog1.to_csv(tmp_path)

        # Create second catalog with new data
        catalog2 = GOESMetadataCatalog()
        catalog2._observations = pd.DataFrame({
            "file_name": ["/b.nc"],
            "platform_of_satellite": ["G18"],
        })

        # Try to append incompatible dataframes
        with pytest.raises(ValueError):
            catalog2.append_to_csv(tmp_path)



class Test_properties:
    """Test that properties return DataFrame copies, not references."""

    def test_observations_returns_dataframe(self, empty_catalog):
        result = empty_catalog.observations
        assert isinstance(result, pd.DataFrame)

    def test_observations_empty_is_usable(self, empty_catalog):
        result = empty_catalog.observations
        assert result.empty
        assert list(result.columns) == []

    def test_observations_returns_copy(self, populated_catalog):
        copy = populated_catalog.observations
        copy["junk"] = 999
        assert "junk" not in populated_catalog.observations.columns

    def test_band_statistics_returns_dataframe(self, empty_catalog):
        result = empty_catalog.band_statistics
        assert isinstance(result, pd.DataFrame)

    def test_band_statistics_returns_copy(self, empty_catalog):
        copy = empty_catalog.band_statistics
        copy["junk"] = 999
        assert "junk" not in empty_catalog.band_statistics.columns

    def test_validation_errors_returns_dataframe(self, empty_catalog):
        result = empty_catalog.validation_errors
        assert isinstance(result, pd.DataFrame)

    def test_validation_errors_returns_copy(self, empty_catalog):
        empty_catalog._validation_errors = pd.DataFrame({
            "file_path": ["/bad.nc"],
            "error_message": ["some error"],
            "timestamp": [pd.Timestamp.now()],
        })
        copy = empty_catalog.validation_errors
        copy.drop(index=0, inplace=True)
        assert len(empty_catalog.validation_errors) == 1

    def test_data_quality_returns_dataframe(self, empty_catalog):
        result = empty_catalog.data_quality
        assert isinstance(result, pd.DataFrame)

    def test_data_quality_returns_copy(self, empty_catalog):
        copy = empty_catalog.data_quality
        copy["junk"] = 999
        assert "junk" not in empty_catalog.data_quality.columns


class Test_query_methods:
    """Test query methods: get_files_for_period, get_files_for_platform, get_valid_files, get_invalid_files"""

    # -- get_files_for_period --

    def test_period_filters_correctly(self, populated_catalog):
        files = populated_catalog.get_files_for_period(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 2, 1),
        )
        assert len(files) == 1
        assert "/a.nc" in files

    def test_period_captures_all(self, populated_catalog):
        files = populated_catalog.get_files_for_period(
            start=datetime(2023, 1, 1),
            end=datetime(2025, 1, 1),
        )
        assert len(files) == 4

    def test_period_no_match(self, populated_catalog):
        files = populated_catalog.get_files_for_period(
            start=datetime(2020, 1, 1),
            end=datetime(2020, 12, 31),
        )
        assert files == []

    def test_period_start_greater_than_end(self, populated_catalog):
        files = populated_catalog.get_files_for_period(
            start=datetime(2025, 1, 1),
            end=datetime(2023, 1, 1),
        )
        assert files == []

    def test_period_exact_match(self, populated_catalog):
        files = populated_catalog.get_files_for_period(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 1),
        )
        assert len(files) == 1
        assert "/a.nc" in files

    def test_period_with_filter(self, populated_catalog):
        files = populated_catalog.get_files_for_period(
            start=datetime(2023, 1, 1),
            end=datetime(2025, 1, 1),
            platform_id="G16",
        )
        assert len(files) == 2
        assert all(f in ["/a.nc", "/c.nc"] for f in files)

    def test_period_invalid_filter_raises(self, populated_catalog):
        with pytest.raises(KeyError):
            populated_catalog.get_files_for_period(
                start=datetime(2023, 1, 1),
                end=datetime(2025, 1, 1),
                nonexistent_column="value",
            )

    def test_period_empty_catalog(self, empty_catalog):
        files = empty_catalog.get_files_for_period(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 12, 31),
        )
        assert files == []

    # -- get_files_for_platform --

    def test_platform_g16(self, populated_catalog):
        files = populated_catalog.get_files_for_platform("G16")
        assert len(files) == 2
        assert "/a.nc" in files
        assert "/c.nc" in files

    def test_platform_g18(self, populated_catalog):
        files = populated_catalog.get_files_for_platform("G18")
        assert len(files) == 2
        assert "/b.nc" in files
        assert "/d.nc" in files

    def test_platform_nonexistent(self, populated_catalog):
        files = populated_catalog.get_files_for_platform("G99")
        assert files == []

    def test_platform_empty_catalog(self, empty_catalog):
        files = empty_catalog.get_files_for_platform("G16")
        assert files == []

    # -- get_valid_files --

    def test_valid_files_returns_all(self, populated_catalog):
        files = populated_catalog.get_valid_files()
        assert len(files) == 4
        assert files == populated_catalog._observations["file_path"].tolist()

    def test_valid_files_empty_catalog(self, empty_catalog):
        assert empty_catalog.get_valid_files() == []

    # -- get_invalid_files --

    def test_invalid_files_returns_errors(self, populated_catalog):
        result = populated_catalog.get_invalid_files()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["file_path"] == "/bad.nc"

    def test_invalid_files_empty_catalog(self, empty_catalog):
        result = empty_catalog.get_invalid_files()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_invalid_files_returns_copy(self, populated_catalog):
        copy = populated_catalog.get_invalid_files()
        copy.drop(index=0, inplace=True)
        assert len(populated_catalog.get_invalid_files()) == 1


class Test_summary:
    """Test GOESMetadataCatalog.summary"""

    def test_empty_catalog(self, empty_catalog):
        result = empty_catalog.summary()
        assert result["total_scanned"] == 0
        assert result["valid_files"] == 0
        assert result["invalid_files"] == 0
        assert "platforms" not in result
        assert "orbital_slots" not in result
        assert "scenes" not in result
        assert "time_range" not in result
        assert "total_size_gb" not in result

    def test_valid_files_count(self, populated_catalog):
        result = populated_catalog.summary()
        assert result["valid_files"] == 4

    def test_invalid_files_count(self, populated_catalog):
        result = populated_catalog.summary()
        assert result["invalid_files"] == 1

    def test_total_scanned(self, populated_catalog):
        result = populated_catalog.summary()
        assert result["total_scanned"] == 5  # 4 valid + 1 error

    def test_platforms(self, populated_catalog):
        result = populated_catalog.summary()
        assert result["platforms"] == {"G16": 2, "G18": 2}

    def test_orbital_slots(self, populated_catalog):
        result = populated_catalog.summary()
        assert result["orbital_slots"] == {"GOES-East": 2, "GOES-West": 2}

    def test_scenes(self, populated_catalog):
        result = populated_catalog.summary()
        assert result["scenes"] == {"Full Disk": 2, "CONUS": 2}

    def test_time_range(self, populated_catalog):
        result = populated_catalog.summary()
        start, end = result["time_range"]
        assert start == pd.Timestamp("2024-01-01")
        assert end == pd.Timestamp("2024-09-20T00:05")

    def test_total_size_gb(self, populated_catalog):
        result = populated_catalog.summary()
        expected_gb = (150.0 + 200.0 + 75.0 + 80.0) / 1024
        assert result["total_size_gb"] == pytest.approx(expected_gb)

    def test_no_size_column(self, empty_catalog):
        empty_catalog._observations = pd.DataFrame({
            "file_path": ["/a.nc"],
            "platform_id": ["G16"],
            "orbital_slot": ["GOES-East"],
            "scene_id": ["Full Disk"],
            "time_coverage_start": pd.to_datetime(["2024-01-01"]),
            "time_coverage_end": pd.to_datetime(["2024-01-01"]),
        })
        result = empty_catalog.summary()
        assert "total_size_gb" not in result

class Test_len:
    """Test GOESMetadataCatalog.__len__"""

    def test_empty_catalog(self, empty_catalog):
        assert len(empty_catalog) == 0

    def test_with_observations(self, populated_catalog):
        assert len(populated_catalog) == 2


class Test_repr:
    """Test GOESMetadataCatalog.__repr__"""

    def test_empty_catalog(self, empty_catalog):
        result = repr(empty_catalog)
        assert result == "GOESMetadataCatalog(observations=0)"

    def test_with_observations_and_time_range(self, populated_catalog):
        result = repr(populated_catalog)
        assert "observations=2" in result
        assert "time_range=2024-01-01..2024-06-15" in result

    def test_with_errors(self, empty_catalog):
        empty_catalog._validation_errors = pd.DataFrame({
            "file_path": ["/bad.nc"],
            "error_message": ["Invalid GOES filename pattern"],
            "timestamp": [pd.Timestamp.now()],
        })
        result = repr(empty_catalog)
        assert "errors=1" in result
