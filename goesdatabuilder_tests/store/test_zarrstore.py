from __future__ import annotations

from pathlib import Path

import pytest
import zarr
import numpy as np
import xarray as xr

from goesdatabuilder.store.zarrstore import ZarrStoreBuilder


# ---------------------------------------------------------------------------
# Shared config
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
    "store": {
        "type": "memory",
        "path": None,
        "storage_options": {},
    },
    "zarr": {
        "field": MINIMAL_CODEC,
        "coordinate": {**MINIMAL_CODEC, "chunks": [512]},
    },
}

LOCAL_STORE_CONFIG = {
    "store": {
        "type": "local",
        "path": None,
        "storage_options": {},
    },
    "zarr": {
        "field": MINIMAL_CODEC,
        "coordinate": {**MINIMAL_CODEC, "chunks": [512]},
    },
}

# kwargs passed to from_existing so its internal cls() call gets valid config
_FROM_EXISTING_KWARGS = {
    "store": LOCAL_STORE_CONFIG["store"],
    "zarr": LOCAL_STORE_CONFIG["zarr"],
}


def _memory_builder() -> ZarrStoreBuilder:
    return ZarrStoreBuilder(
        store=MEMORY_STORE_CONFIG["store"],
        zarr=MEMORY_STORE_CONFIG["zarr"],
    )


def _local_builder(tmp_path: Path) -> ZarrStoreBuilder:
    return ZarrStoreBuilder(
        store={**LOCAL_STORE_CONFIG["store"], "path": str(tmp_path / "test.zarr")},
        zarr=LOCAL_STORE_CONFIG["zarr"],
    )


def _from_existing(store_path: Path, mode: str = "r+") -> ZarrStoreBuilder:
    """Wrapper around from_existing that passes valid config kwargs."""
    return ZarrStoreBuilder.from_existing(store_path, mode=mode, **_FROM_EXISTING_KWARGS)


def _open_store() -> ZarrStoreBuilder:
    """Return a memory-backed store that is already open."""
    builder = ZarrStoreBuilder(
        store=MEMORY_STORE_CONFIG["store"],
        zarr=MEMORY_STORE_CONFIG["zarr"],
    )
    builder.create_store()
    return builder


"""Unit tests for ZarrStoreBuilder store lifecycle.

Covers the public interface for Group 1:
  - create_store      (creates store, sets is_open, store_path)
  - from_existing     (classmethod, opens existing local store)
  - close_store       (releases resources, idempotent)
  - is_open           (property)
  - store_path        (property)
  - __enter__/__exit__ (context manager)
  - __repr__          (string representation)
"""

# ---------------------------------------------------------------------------
# is_open: before any store is created
# ---------------------------------------------------------------------------

class TestIsOpenInitial:
    def test_not_open_on_construction(self):
        """is_open is False immediately after construction before create_store."""
        builder = _memory_builder()
        assert builder.is_open is False

    def test_store_is_none_on_construction(self):
        """_store is None before create_store is called."""
        builder = _memory_builder()
        assert builder._store is None

    def test_root_is_none_on_construction(self):
        """_root is None before create_store is called."""
        builder = _memory_builder()
        assert builder._root is None


# ---------------------------------------------------------------------------
# create_store: memory backend
# ---------------------------------------------------------------------------

class TestCreateStoreMemory:
    def test_is_open_after_create(self):
        """is_open is True after create_store on a memory backend."""
        builder = _memory_builder()
        builder.create_store()
        assert builder.is_open is True

    def test_root_is_zarr_group(self):
        """root is a zarr.Group after create_store."""
        builder = _memory_builder()
        builder.create_store()
        assert isinstance(builder.root, zarr.Group)

    def test_store_path_is_none_for_memory(self):
        """store_path is None for a memory-backed store."""
        builder = _memory_builder()
        builder.create_store()
        assert builder.store_path is None

    def test_store_property_is_not_none(self):
        """store property returns a non-None object after create_store."""
        builder = _memory_builder()
        builder.create_store()
        assert builder.store is not None

    def test_root_is_zarr_format_3(self):
        """The created root group uses Zarr format 3."""
        builder = _memory_builder()
        builder.create_store()
        assert builder.root.metadata.zarr_format == 3


# ---------------------------------------------------------------------------
# create_store: local backend
# ---------------------------------------------------------------------------

class TestCreateStoreLocal:
    def test_creates_directory(self, tmp_path):
        """create_store creates the store directory on disk."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        assert store_path.exists()

    def test_store_path_property_set(self, tmp_path):
        """store_path property reflects the path passed to create_store."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        assert builder.store_path == store_path

    def test_raises_if_exists_and_no_overwrite(self, tmp_path):
        """create_store raises FileExistsError if store exists and overwrite=False."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        builder.close_store()
        builder2 = _local_builder(tmp_path)
        with pytest.raises(FileExistsError):
            builder2.create_store(store_path=str(store_path), overwrite=False)

    def test_overwrites_existing_store(self, tmp_path):
        """create_store with overwrite=True replaces an existing store."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        builder.create_group("initial_group")
        builder.close_store()
        builder2 = _local_builder(tmp_path)
        builder2.create_store(store_path=str(store_path), overwrite=True)
        assert not builder2.group_exists("initial_group")

    def test_is_open_after_local_create(self, tmp_path):
        """is_open is True after create_store on a local backend."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        assert builder.is_open is True


# ---------------------------------------------------------------------------
# from_existing
# ---------------------------------------------------------------------------

class TestFromExisting:
    def test_opens_existing_local_store(self, tmp_path):
        """from_existing opens a store that was previously created."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        builder.create_group("test_group")
        builder.close_store()

        reopened = _from_existing(store_path)
        assert reopened.is_open is True
        assert reopened.group_exists("test_group")

    def test_from_existing_returns_zarrstore_instance(self, tmp_path):
        """from_existing returns a ZarrStoreBuilder instance."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        builder.close_store()

        reopened = _from_existing(store_path)
        assert isinstance(reopened, ZarrStoreBuilder)

    def test_from_existing_root_is_zarr_group(self, tmp_path):
        """from_existing root property is a zarr.Group."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        builder.close_store()

        reopened = _from_existing(store_path)
        assert isinstance(reopened.root, zarr.Group)

    def test_from_existing_preserves_attrs(self, tmp_path):
        """Attributes written before close are readable after from_existing."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        builder.set_attrs("/", {"title": "test dataset"})
        builder.close_store()

        reopened = _from_existing(store_path)
        assert reopened.get_attrs("/")["title"] == "test dataset"

    def test_from_existing_store_path_set(self, tmp_path):
        """from_existing sets store_path correctly."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        builder.close_store()

        reopened = _from_existing(store_path)
        assert reopened.store_path == Path(store_path)

    def test_from_existing_default_mode_read_write(self, tmp_path):
        """from_existing opens in r+ mode by default -- writes are allowed."""
        store_path = tmp_path / "test.zarr"
        builder = _local_builder(tmp_path)
        builder.create_store(store_path=str(store_path))
        builder.close_store()

        reopened = _from_existing(store_path)
        reopened.create_group("new_group")
        assert reopened.group_exists("new_group")


# ---------------------------------------------------------------------------
# close_store
# ---------------------------------------------------------------------------

class TestCloseStore:
    def test_is_open_false_after_close(self):
        """is_open is False after close_store."""
        builder = _memory_builder()
        builder.create_store()
        builder.close_store()
        assert builder.is_open is False

    def test_store_is_none_after_close(self):
        """_store is None after close_store."""
        builder = _memory_builder()
        builder.create_store()
        builder.close_store()
        assert builder._store is None

    def test_root_is_none_after_close(self):
        """_root is None after close_store."""
        builder = _memory_builder()
        builder.create_store()
        builder.close_store()
        assert builder._root is None

    def test_store_path_is_none_after_close(self):
        """store_path is None after close_store."""
        builder = _memory_builder()
        builder.create_store()
        builder.close_store()
        assert builder.store_path is None

    def test_close_is_idempotent(self):
        """Calling close_store twice does not raise."""
        builder = _memory_builder()
        builder.create_store()
        builder.close_store()
        builder.close_store()

    def test_close_before_create_does_not_raise(self):
        """Calling close_store before create_store does not raise."""
        builder = _memory_builder()
        builder.close_store()


# ---------------------------------------------------------------------------
# context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    def test_enter_returns_self(self):
        """__enter__ returns the builder instance."""
        builder = _memory_builder()
        builder.create_store()
        with builder as ctx:
            assert ctx is builder

    def test_store_closed_after_exit(self):
        """__exit__ closes the store."""
        builder = _memory_builder()
        builder.create_store()
        with builder:
            assert builder.is_open is True
        assert builder.is_open is False

    def test_exit_on_exception_still_closes(self):
        """__exit__ closes the store even when the body raises."""
        builder = _memory_builder()
        builder.create_store()
        with pytest.raises(RuntimeError):
            with builder:
                raise RuntimeError("deliberate")
        assert builder.is_open is False

    def test_operations_inside_context(self):
        """Groups and arrays created inside a with block are accessible."""
        builder = _memory_builder()
        builder.create_store()
        with builder:
            builder.create_group("inside_group")
            assert builder.group_exists("inside_group")

    def test_context_without_prior_create_store(self):
        """Entering a context without calling create_store first still exits cleanly."""
        builder = _memory_builder()
        with builder:
            pass
        assert builder.is_open is False


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------

class TestRepr:
    def test_repr_not_initialized(self):
        """__repr__ on an unopened store mentions 'not initialized'."""
        builder = _memory_builder()
        r = repr(builder)
        assert "not initialized" in r.lower()

    def test_repr_initialized(self):
        """__repr__ on an open store mentions ZarrStoreBuilder."""
        builder = _memory_builder()
        builder.create_store()
        r = repr(builder)
        assert "ZarrStoreBuilder" in r

    def test_repr_shows_memory_path(self):
        """__repr__ for a memory store mentions 'memory'."""
        builder = _memory_builder()
        builder.create_store()
        r = repr(builder)
        assert "memory" in r.lower()


"""Unit tests for ZarrStoreBuilder group management.

Covers the public interface for Group 2:
  - create_group  (creates group, attaches attrs, raises on duplicate/closed store)
  - get_group     (retrieves group, raises on missing/wrong type/closed store)
  - group_exists  (bool check, raises on closed store)
  - list_groups   (lists immediate child groups at a path)
"""

# ---------------------------------------------------------------------------
# create_group
# ---------------------------------------------------------------------------

class TestCreateGroup:
    def test_returns_zarr_group(self):
        """create_group returns a zarr.Group."""
        builder = _open_store()
        result = builder.create_group("my_group")
        assert isinstance(result, zarr.Group)

    def test_group_exists_after_create(self):
        """A group is findable via group_exists after creation."""
        builder = _open_store()
        builder.create_group("my_group")
        assert builder.group_exists("my_group") is True

    def test_create_group_with_attrs(self):
        """Attrs passed to create_group are attached to the group."""
        builder = _open_store()
        builder.create_group("meta_group", attrs={"region": "GOES-East"})
        group = builder.get_group("meta_group")
        assert group.attrs["region"] == "GOES-East"

    def test_create_group_without_attrs(self):
        """create_group with no attrs creates an empty attrs dict."""
        builder = _open_store()
        builder.create_group("bare_group")
        group = builder.get_group("bare_group")
        assert dict(group.attrs) == {}

    def test_raises_on_duplicate_group(self):
        """create_group raises ValueError when the group already exists."""
        builder = _open_store()
        builder.create_group("dup_group")
        with pytest.raises(ValueError):
            builder.create_group("dup_group")

    def test_raises_when_store_not_open(self):
        """create_group raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.create_group("any_group")

    def test_multiple_groups_at_root(self):
        """Multiple groups can be created at the root level."""
        builder = _open_store()
        builder.create_group("alpha")
        builder.create_group("beta")
        builder.create_group("gamma")
        assert builder.group_exists("alpha")
        assert builder.group_exists("beta")
        assert builder.group_exists("gamma")

    def test_nested_group_path(self):
        """A nested group path can be created after the parent exists."""
        builder = _open_store()
        builder.create_group("parent")
        builder.create_group("parent/child")
        assert builder.group_exists("parent/child")

    def test_attrs_are_dict_not_xarray(self):
        """Attrs are stored as plain dict values, not wrapped objects."""
        builder = _open_store()
        builder.create_group("typed_group", attrs={"count": 42, "label": "test"})
        group = builder.get_group("typed_group")
        assert group.attrs["count"] == 42
        assert group.attrs["label"] == "test"


# ---------------------------------------------------------------------------
# get_group
# ---------------------------------------------------------------------------

class TestGetGroup:
    def test_returns_zarr_group(self):
        """get_group returns a zarr.Group for an existing group."""
        builder = _open_store()
        builder.create_group("fetch_me")
        result = builder.get_group("fetch_me")
        assert isinstance(result, zarr.Group)

    def test_raises_on_missing_group(self):
        """get_group raises KeyError when the group does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.get_group("nonexistent")

    def test_raises_when_path_is_array(self):
        """get_group raises KeyError when the path points to an array, not a group."""
        builder = _open_store()
        builder.create_array(
            path="my_array",
            shape=(10,),
            dtype="float32",
            preset="field",
            dimension_names=['lat'],
        )
        with pytest.raises(KeyError):
            builder.get_group("my_array")

    def test_raises_when_store_not_open(self):
        """get_group raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.get_group("any_group")

    def test_retrieved_group_has_correct_attrs(self):
        """get_group returns a group whose attrs match what was written."""
        builder = _open_store()
        builder.create_group("attr_group", attrs={"mission": "GOES-R"})
        group = builder.get_group("attr_group")
        assert group.attrs["mission"] == "GOES-R"

    def test_get_nested_group(self):
        """get_group retrieves a nested group by its full path."""
        builder = _open_store()
        builder.create_group("outer")
        builder.create_group("outer/inner")
        group = builder.get_group("outer/inner")
        assert isinstance(group, zarr.Group)


# ---------------------------------------------------------------------------
# group_exists
# ---------------------------------------------------------------------------

class TestGroupExists:
    def test_returns_false_for_missing_group(self):
        """group_exists returns False when the group has not been created."""
        builder = _open_store()
        assert builder.group_exists("phantom") is False

    def test_returns_true_for_existing_group(self):
        """group_exists returns True after create_group."""
        builder = _open_store()
        builder.create_group("real_group")
        assert builder.group_exists("real_group") is True

    def test_returns_false_for_array_path(self):
        """group_exists returns False when the path points to an array."""
        builder = _open_store()
        builder.create_array(
            path="some_array",
            shape=(5,),
            dtype="float32",
            preset="field",
            dimension_names=['lat'],
        )
        assert builder.group_exists("some_array") is False

    def test_raises_when_store_not_open(self):
        """group_exists raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.group_exists("any_group")

    def test_nested_group_exists(self):
        """group_exists works correctly for nested paths."""
        builder = _open_store()
        builder.create_group("outer")
        builder.create_group("outer/inner")
        assert builder.group_exists("outer/inner") is True
        assert builder.group_exists("outer/missing") is False

    def test_returns_bool_type(self):
        """group_exists returns a plain Python bool."""
        builder = _open_store()
        result = builder.group_exists("anything")
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# list_groups
# ---------------------------------------------------------------------------

class TestListGroups:
    def test_empty_store_returns_empty_list(self):
        """list_groups returns an empty list when no groups exist."""
        builder = _open_store()
        assert builder.list_groups() == []

    def test_lists_root_groups(self):
        """list_groups at root lists all top-level groups."""
        builder = _open_store()
        builder.create_group("alpha")
        builder.create_group("beta")
        result = builder.list_groups()
        assert set(result) == {"alpha", "beta"}

    def test_returns_list_type(self):
        """list_groups returns a list."""
        builder = _open_store()
        assert isinstance(builder.list_groups(), list)

    def test_does_not_include_arrays(self):
        """list_groups does not include array names -- only groups."""
        builder = _open_store()
        builder.create_group("a_group")
        builder.create_array(
            path="an_array",
            shape=(5,),
            dtype="float32",
            preset="field",
            dimension_names=['lat']
        )
        result = builder.list_groups()
        assert "an_array" not in result
        assert "a_group" in result

    def test_lists_groups_at_subpath(self):
        """list_groups at a subpath lists only that group's immediate children."""
        builder = _open_store()
        builder.create_group("parent")
        builder.create_group("parent/child_a")
        builder.create_group("parent/child_b")
        builder.create_group("sibling")
        result = builder.list_groups("parent")
        assert set(result) == {"child_a", "child_b"}
        assert "sibling" not in result

    def test_does_not_recurse(self):
        """list_groups only lists immediate children, not deeper descendants."""
        builder = _open_store()
        builder.create_group("top")
        builder.create_group("top/mid")
        builder.create_group("top/mid/deep")
        result = builder.list_groups("top")
        assert "mid" in result
        assert "deep" not in result

    def test_raises_when_store_not_open(self):
        """list_groups raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.list_groups()

    def test_raises_for_nonexistent_subpath(self):
        """list_groups raises KeyError when the path does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.list_groups("nonexistent_parent")


"""Unit tests for ZarrStoreBuilder array management.

Covers the public interface for Group 3:
  - create_array   (creates array with preset, shape, dtype, attrs, dimension_names)
  - get_array      (retrieves array, raises on missing/wrong type/closed store)
  - array_exists   (bool check)
  - array_list     (lists arrays at a path)
  - resize_array   (changes shape along an axis)
  - append_array   (appends data, optional return_location)
  - write_array    (full and partial writes)
"""
# ---------------------------------------------------------------------------
# create_array
# ---------------------------------------------------------------------------

class TestCreateArray:
    def test_returns_zarr_array(self):
        """create_array returns a zarr.Array."""
        builder = _open_store()
        arr = builder.create_array("data", shape=(10,), dtype=np.float32, preset="field",
                                   dimension_names=["x"])
        assert isinstance(arr, zarr.Array)

    def test_shape_is_correct(self):
        """Created array has the requested shape."""
        builder = _open_store()
        arr = builder.create_array("data", shape=(5, 8), dtype=np.float32, preset="field",
                                   dimension_names=["y", "x"])
        assert arr.shape == (5, 8)

    def test_dtype_is_correct(self):
        """Created array has the requested dtype."""
        builder = _open_store()
        arr = builder.create_array("data", shape=(4,), dtype=np.float64, preset="field",
                                   dimension_names=["x"])
        assert np.dtype(arr.dtype) == np.dtype(np.float64)

    def test_attrs_attached(self):
        """Attrs passed to create_array are stored on the array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                              attrs={"units": "K"}, dimension_names=["x"])
        arr = builder.get_array("data")
        assert arr.attrs["units"] == "K"

    def test_no_attrs_gives_empty_attrs(self):
        """create_array with no attrs results in empty attrs dict."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        assert dict(builder.get_array("data").attrs) == {}

    def test_dimension_names_stored(self):
        """Dimension names are stored on the array metadata."""
        builder = _open_store()
        arr = builder.create_array("data", shape=(3, 4), dtype=np.float32, preset="field",
                                   dimension_names=["time", "space"])
        assert list(arr.metadata.dimension_names) == ["time", "space"]

    def test_array_exists_after_create(self):
        """array_exists returns True immediately after create_array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        assert builder.array_exists("data") is True

    def test_raises_on_duplicate(self):
        """create_array raises ValueError when the path already has an array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        with pytest.raises(ValueError):
            builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                                 dimension_names=["x"])

    def test_raises_when_store_not_open(self):
        """create_array raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                                 dimension_names=["x"])

    def test_array_under_group(self):
        """create_array works when parent group already exists."""
        builder = _open_store()
        builder.create_group("region")
        builder.create_array("region/lat", shape=(10,), dtype=np.float64, preset="coordinate",
                              dimension_names=["lat"])
        assert builder.array_exists("region/lat")

    def test_extensible_time_array(self):
        """A shape=(0,) array can be created for extensible time dimensions."""
        builder = _open_store()
        arr = builder.create_array("time", shape=(0,), dtype="datetime64[ns]", preset="coordinate",
                                   dimension_names=["time"])
        assert arr.shape == (0,)

    def test_3d_array_shape(self):
        """create_array correctly handles 3D shapes."""
        builder = _open_store()
        arr = builder.create_array("cmi", shape=(0, 5, 8), dtype=np.float32, preset="field",
                                   dimension_names=["time", "lat", "lon"])
        assert arr.shape == (0, 5, 8)


# ---------------------------------------------------------------------------
# get_array
# ---------------------------------------------------------------------------

class TestGetArray:
    def test_returns_zarr_array(self):
        """get_array returns a zarr.Array for an existing array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        assert isinstance(builder.get_array("data"), zarr.Array)

    def test_raises_on_missing(self):
        """get_array raises KeyError when the path does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.get_array("nonexistent")

    def test_raises_when_path_is_group(self):
        """get_array raises KeyError when the path points to a group."""
        builder = _open_store()
        builder.create_group("my_group")
        with pytest.raises(KeyError):
            builder.get_array("my_group")

    def test_raises_when_store_not_open(self):
        """get_array raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.get_array("data")

    def test_retrieved_array_has_correct_shape(self):
        """get_array returns the array with its original shape."""
        builder = _open_store()
        builder.create_array("data", shape=(7, 3), dtype=np.float32, preset="field",
                              dimension_names=["y", "x"])
        arr = builder.get_array("data")
        assert arr.shape == (7, 3)


# ---------------------------------------------------------------------------
# array_exists
# ---------------------------------------------------------------------------

class TestArrayExists:
    def test_returns_false_when_missing(self):
        """array_exists returns False when no array exists at the path."""
        builder = _open_store()
        assert builder.array_exists("phantom") is False

    def test_returns_true_after_create(self):
        """array_exists returns True after create_array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        assert builder.array_exists("data") is True

    def test_returns_false_for_group_path(self):
        """array_exists returns False when the path points to a group."""
        builder = _open_store()
        builder.create_group("my_group")
        assert builder.array_exists("my_group") is False

    def test_returns_bool_type(self):
        """array_exists returns a plain Python bool."""
        builder = _open_store()
        assert isinstance(builder.array_exists("anything"), bool)

    def test_raises_when_store_not_open(self):
        """array_exists raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.array_exists("data")

    def test_nested_array_path(self):
        """array_exists works for arrays nested inside groups."""
        builder = _open_store()
        builder.create_group("region")
        builder.create_array("region/lat", shape=(5,), dtype=np.float64, preset="coordinate",
                              dimension_names=["lat"])
        assert builder.array_exists("region/lat") is True
        assert builder.array_exists("region/lon") is False


# ---------------------------------------------------------------------------
# array_list
# ---------------------------------------------------------------------------

class TestArrayList:
    def test_empty_store_returns_empty_list(self):
        """array_list returns an empty list when no arrays exist."""
        builder = _open_store()
        assert builder.array_list() == []

    def test_lists_root_arrays(self):
        """array_list lists all arrays at root."""
        builder = _open_store()
        builder.create_array("alpha", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        builder.create_array("beta", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        assert set(builder.array_list()) == {"alpha", "beta"}

    def test_does_not_include_groups(self):
        """array_list does not include group names."""
        builder = _open_store()
        builder.create_group("a_group")
        builder.create_array("an_array", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        result = builder.array_list()
        assert "an_array" in result
        assert "a_group" not in result

    def test_lists_arrays_at_subpath(self):
        """array_list at a subpath lists only arrays in that group."""
        builder = _open_store()
        builder.create_group("region")
        builder.create_array("region/lat", shape=(5,), dtype=np.float64, preset="coordinate",
                              dimension_names=["lat"])
        builder.create_array("region/lon", shape=(5,), dtype=np.float64, preset="coordinate",
                              dimension_names=["lon"])
        builder.create_array("other", shape=(5,), dtype=np.float64, preset="coordinate",
                              dimension_names=["x"])
        result = builder.array_list("region")
        assert set(result) == {"lat", "lon"}
        assert "other" not in result

    def test_returns_list_type(self):
        """array_list returns a list."""
        builder = _open_store()
        assert isinstance(builder.array_list(), list)

    def test_raises_when_store_not_open(self):
        """array_list raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.array_list()


# ---------------------------------------------------------------------------
# resize_array
# ---------------------------------------------------------------------------

class TestResizeArray:
    def test_resizes_1d_array(self):
        """resize_array changes the shape of a 1D array."""
        builder = _open_store()
        builder.create_array("data", shape=(0,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        builder.resize_array("data", (10,))
        assert builder.get_array("data").shape == (10,)

    def test_resizes_first_axis_of_3d_array(self):
        """resize_array can grow the time axis of a 3D array."""
        builder = _open_store()
        builder.create_array("cmi", shape=(0, 5, 8), dtype=np.float32, preset="field",
                              dimension_names=["time", "lat", "lon"])
        builder.resize_array("cmi", (3, 5, 8))
        assert builder.get_array("cmi").shape == (3, 5, 8)

    def test_spatial_axes_unchanged(self):
        """resize_array preserves spatial dimensions when only time axis grows."""
        builder = _open_store()
        builder.create_array("cmi", shape=(0, 5, 8), dtype=np.float32, preset="field",
                              dimension_names=["time", "lat", "lon"])
        builder.resize_array("cmi", (4, 5, 8))
        arr = builder.get_array("cmi")
        assert arr.shape[1] == 5
        assert arr.shape[2] == 8

    def test_raises_on_missing_array(self):
        """resize_array raises KeyError when the array does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.resize_array("nonexistent", (10,))


# ---------------------------------------------------------------------------
# append_array
# ---------------------------------------------------------------------------

class TestAppendArray:
    def test_appends_to_1d_array(self):
        """append_array grows a 1D array by the length of the appended data."""
        builder = _open_store()
        builder.create_array("data", shape=(0,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        builder.append_array("data", np.array([1.0, 2.0, 3.0], dtype=np.float32))
        assert builder.get_array("data").shape == (3,)

    def test_appended_values_correct(self):
        """Values written by append_array are readable from the array."""
        builder = _open_store()
        builder.create_array("data", shape=(0,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        data = np.array([4.0, 5.0], dtype=np.float32)
        builder.append_array("data", data)
        np.testing.assert_array_equal(builder.get_array("data")[:], data)

    def test_second_append_extends_array(self):
        """A second append extends the array further."""
        builder = _open_store()
        builder.create_array("data", shape=(0,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        builder.append_array("data", np.array([1.0], dtype=np.float32))
        builder.append_array("data", np.array([2.0, 3.0], dtype=np.float32))
        assert builder.get_array("data").shape == (3,)

    def test_return_location_gives_start_end(self):
        """return_location=True returns (start_idx, end_idx) of the written region."""
        builder = _open_store()
        builder.create_array("data", shape=(0,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        loc = builder.append_array("data", np.array([1.0, 2.0], dtype=np.float32),
                                   return_location=True)
        assert loc == (0, 2)

    def test_return_location_second_append(self):
        """return_location reflects correct indices for a second append."""
        builder = _open_store()
        builder.create_array("data", shape=(0,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        builder.append_array("data", np.array([1.0, 2.0], dtype=np.float32))
        loc = builder.append_array("data", np.array([3.0], dtype=np.float32),
                                   return_location=True)
        assert loc == (2, 3)

    def test_return_location_false_returns_none(self):
        """return_location=False (default) returns None."""
        builder = _open_store()
        builder.create_array("data", shape=(0,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        result = builder.append_array("data", np.array([1.0], dtype=np.float32))
        assert result is None

    def test_append_along_time_axis_3d(self):
        """append_array along axis=0 grows the time dimension of a 3D array."""
        builder = _open_store()
        builder.create_array("cmi", shape=(0, 3, 4), dtype=np.float32, preset="field",
                              dimension_names=["time", "lat", "lon"])
        chunk = np.ones((1, 3, 4), dtype=np.float32)
        builder.append_array("cmi", chunk, axis=0)
        assert builder.get_array("cmi").shape == (1, 3, 4)

    def test_accepts_dask_array(self):
        """append_array accepts a Dask array and computes it before writing."""
        import dask.array as da
        builder = _open_store()
        builder.create_array("data", shape=(0,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        dask_data = da.from_array(np.array([1.0, 2.0], dtype=np.float32), chunks=2)
        builder.append_array("data", dask_data)
        assert builder.get_array("data").shape == (2,)


# ---------------------------------------------------------------------------
# write_array
# ---------------------------------------------------------------------------

class TestWriteArray:
    def test_full_write(self):
        """write_array with no selection overwrites the entire array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        data = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)
        builder.write_array("data", data)
        np.testing.assert_array_equal(builder.get_array("data")[:], data)

    def test_partial_write_with_selection(self):
        """write_array with a selection writes only to the specified slice."""
        builder = _open_store()
        builder.create_array("data", shape=(5,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        builder.write_array("data", np.zeros(5, dtype=np.float32))
        builder.write_array("data", np.array([9.0, 9.0], dtype=np.float32),
                            selection=(slice(1, 3),))
        result = builder.get_array("data")[:]
        np.testing.assert_array_equal(result[1:3], [9.0, 9.0])
        assert result[0] == 0.0
        assert result[3] == 0.0

    def test_write_2d_array(self):
        """write_array works for 2D arrays."""
        builder = _open_store()
        builder.create_array("data", shape=(3, 4), dtype=np.float32, preset="field",
                              dimension_names=["y", "x"])
        data = np.arange(12, dtype=np.float32).reshape(3, 4)
        builder.write_array("data", data)
        np.testing.assert_array_equal(builder.get_array("data")[:], data)

    def test_raises_on_missing_array(self):
        """write_array raises KeyError when the array does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.write_array("nonexistent", np.array([1.0]))

    def test_accepts_xarray_dataarray(self):
        """write_array accepts an xarray DataArray and extracts its values."""
        import xarray as xr
        builder = _open_store()
        builder.create_array("data", shape=(3,), dtype=np.float32, preset="field",
                              dimension_names=["x"])
        da = xr.DataArray(np.array([1.0, 2.0, 3.0], dtype=np.float32))
        builder.write_array("data", da)
        np.testing.assert_array_equal(builder.get_array("data")[:], [1.0, 2.0, 3.0])


"""Unit tests for ZarrStoreBuilder metadata management.

Covers the public interface for Group 4:
  - get_attrs  (returns dict of attrs for group or array)
  - set_attrs  (merge and replace modes)
  - del_attrs  (removes specific keys, tolerates missing keys)
"""

# ---------------------------------------------------------------------------
# get_attrs
# ---------------------------------------------------------------------------

class TestGetAttrs:
    def test_root_attrs_empty_on_new_store(self):
        """Root attrs are empty on a freshly created store."""
        builder = _open_store()
        assert builder.get_attrs("/") == {}

    def test_returns_dict(self):
        """get_attrs returns a plain dict."""
        builder = _open_store()
        assert isinstance(builder.get_attrs("/"), dict)

    def test_returns_attrs_written_at_create_group(self):
        """get_attrs returns attrs that were set during create_group."""
        builder = _open_store()
        builder.create_group("region", attrs={"mission": "GOES-R"})
        assert builder.get_attrs("region")["mission"] == "GOES-R"

    def test_returns_attrs_written_at_create_array(self):
        """get_attrs returns attrs that were set during create_array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype="float32", preset="field",
                              attrs={"units": "K"}, dimension_names=["x"])
        assert builder.get_attrs("data")["units"] == "K"

    def test_raises_on_missing_path(self):
        """get_attrs raises KeyError when the path does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.get_attrs("nonexistent")

    def test_raises_when_store_not_open(self):
        """get_attrs raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.get_attrs("/")

    def test_multiple_attrs_returned(self):
        """get_attrs returns all keys written to a node."""
        builder = _open_store()
        builder.set_attrs("/", {"title": "GOES data", "version": "1.0"})
        attrs = builder.get_attrs("/")
        assert attrs["title"] == "GOES data"
        assert attrs["version"] == "1.0"


# ---------------------------------------------------------------------------
# set_attrs
# ---------------------------------------------------------------------------

class TestSetAttrs:
    def test_set_attrs_on_root(self):
        """set_attrs writes attrs to the root group."""
        builder = _open_store()
        builder.set_attrs("/", {"title": "test"})
        assert builder.get_attrs("/")["title"] == "test"

    def test_set_attrs_on_group(self):
        """set_attrs writes attrs to a named group."""
        builder = _open_store()
        builder.create_group("region")
        builder.set_attrs("region", {"satellite": "G18"})
        assert builder.get_attrs("region")["satellite"] == "G18"

    def test_set_attrs_on_array(self):
        """set_attrs writes attrs to an array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype="float32", preset="field",
                              dimension_names=["x"])
        builder.set_attrs("data", {"units": "1"})
        assert builder.get_attrs("data")["units"] == "1"

    def test_merge_true_adds_new_keys(self):
        """merge=True adds new keys without removing existing ones."""
        builder = _open_store()
        builder.set_attrs("/", {"existing": "value"})
        builder.set_attrs("/", {"new_key": "new_value"}, merge=True)
        attrs = builder.get_attrs("/")
        assert attrs["existing"] == "value"
        assert attrs["new_key"] == "new_value"

    def test_merge_true_updates_existing_key(self):
        """merge=True overwrites the value of an existing key."""
        builder = _open_store()
        builder.set_attrs("/", {"key": "old"})
        builder.set_attrs("/", {"key": "new"}, merge=True)
        assert builder.get_attrs("/")["key"] == "new"

    def test_merge_false_replaces_all_attrs(self):
        """merge=False clears existing attrs and sets only the new ones."""
        builder = _open_store()
        builder.set_attrs("/", {"old_key": "old_value"})
        builder.set_attrs("/", {"new_key": "new_value"}, merge=False)
        attrs = builder.get_attrs("/")
        assert "old_key" not in attrs
        assert attrs["new_key"] == "new_value"

    def test_raises_on_missing_path(self):
        """set_attrs raises KeyError when the path does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.set_attrs("nonexistent", {"key": "val"})

    def test_raises_when_store_not_open(self):
        """set_attrs raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.set_attrs("/", {"key": "val"})

    def test_set_multiple_types(self):
        """set_attrs stores int, float, str, and list values correctly."""
        builder = _open_store()
        builder.set_attrs("/", {
            "count": 5,
            "pi": 3.14,
            "label": "test",
            "flags": [0, 1, 2],
        })
        attrs = builder.get_attrs("/")
        assert attrs["count"] == 5
        assert attrs["pi"] == pytest.approx(3.14)
        assert attrs["label"] == "test"
        assert attrs["flags"] == [0, 1, 2]


# ---------------------------------------------------------------------------
# del_attrs
# ---------------------------------------------------------------------------

class TestDelAttrs:
    def test_deletes_existing_key(self):
        """del_attrs removes a key that exists."""
        builder = _open_store()
        builder.set_attrs("/", {"to_delete": "gone", "keep": "here"})
        builder.del_attrs("/", ["to_delete"])
        attrs = builder.get_attrs("/")
        assert "to_delete" not in attrs
        assert attrs["keep"] == "here"

    def test_deletes_multiple_keys(self):
        """del_attrs removes multiple keys in one call."""
        builder = _open_store()
        builder.set_attrs("/", {"a": 1, "b": 2, "c": 3})
        builder.del_attrs("/", ["a", "b"])
        attrs = builder.get_attrs("/")
        assert "a" not in attrs
        assert "b" not in attrs
        assert attrs["c"] == 3

    def test_tolerates_missing_key(self):
        """del_attrs does not raise when a key to delete does not exist."""
        builder = _open_store()
        builder.set_attrs("/", {"real_key": "value"})
        builder.del_attrs("/", ["nonexistent_key"])
        assert builder.get_attrs("/")["real_key"] == "value"

    def test_empty_keys_list_is_no_op(self):
        """del_attrs with an empty list does not change attrs."""
        builder = _open_store()
        builder.set_attrs("/", {"key": "value"})
        builder.del_attrs("/", [])
        assert builder.get_attrs("/")["key"] == "value"

    def test_deletes_from_group(self):
        """del_attrs works on a named group."""
        builder = _open_store()
        builder.create_group("region", attrs={"mission": "GOES-R", "band": 7})
        builder.del_attrs("region", ["band"])
        attrs = builder.get_attrs("region")
        assert "band" not in attrs
        assert attrs["mission"] == "GOES-R"

    def test_deletes_from_array(self):
        """del_attrs works on an array."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype="float32", preset="field",
                              attrs={"units": "K", "long_name": "temp"},
                              dimension_names=["x"])
        builder.del_attrs("data", ["long_name"])
        attrs = builder.get_attrs("data")
        assert "long_name" not in attrs
        assert attrs["units"] == "K"

    def test_raises_when_store_not_open(self):
        """del_attrs raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.del_attrs("/", ["key"])


"""Unit tests for ZarrStoreBuilder utilities.

Covers the public interface for Group 5:
  - tree           (string representation of hierarchy)
  - info           (info string for a node)
  - info_complete  (detailed info for arrays only)
  - validate       (store integrity check)
  - array_pipelines (dict of pipeline configs from zarr config)
  - _load_codec    (codec instantiation from config dict)
  - _ensure_numpy  (data type normalisation)
"""



# ---------------------------------------------------------------------------
# tree
# ---------------------------------------------------------------------------

class TestTree:
    def test_returns_string(self):
        """tree returns a string."""
        builder = _open_store()
        assert isinstance(builder.tree(), str)

    def test_empty_store_shows_root(self):
        """tree on an empty store contains a root marker."""
        builder = _open_store()
        result = builder.tree()
        assert "/" in result

    def test_groups_appear_in_tree(self):
        """Groups created in the store appear in the tree output."""
        builder = _open_store()
        builder.create_group("region_a")
        builder.create_group("region_b")
        result = builder.tree()
        assert "region_a" in result
        assert "region_b" in result

    def test_arrays_appear_in_tree(self):
        """Arrays created in the store appear in the tree output."""
        builder = _open_store()
        builder.create_array("my_array", shape=(5,), dtype=np.float32,
                              preset="field", dimension_names=["x"])
        result = builder.tree()
        assert "my_array" in result

    def test_nested_structure_appears_in_tree(self):
        """Nested groups and arrays appear in the tree output."""
        builder = _open_store()
        builder.create_group("outer")
        builder.create_group("outer/inner")
        builder.create_array("outer/inner/data", shape=(3,), dtype=np.float32,
                              preset="field", dimension_names=["x"])
        result = builder.tree()
        assert "outer" in result
        assert "inner" in result
        assert "data" in result

    def test_tree_at_subpath(self):
        """tree at a subpath shows only that subtree."""
        builder = _open_store()
        builder.create_group("region")
        builder.create_array("region/lat", shape=(5,), dtype=np.float64,
                              preset="coordinate", dimension_names=["lat"])
        builder.create_group("other")
        result = builder.tree("region")
        assert "lat" in result
        assert "other" not in result

    def test_raises_when_store_not_open(self):
        """tree raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.tree()

    def test_array_dtype_and_shape_in_tree(self):
        """tree output includes dtype and shape information for arrays."""
        builder = _open_store()
        builder.create_array("data", shape=(5, 8), dtype=np.float32,
                              preset="field", dimension_names=["y", "x"])
        result = builder.tree()
        assert "float32" in result or "5" in result


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------

class TestInfo:
    def test_returns_string(self):
        """info returns a string."""
        builder = _open_store()
        assert isinstance(builder.info(), str)

    def test_info_at_root(self):
        """info at root returns a non-empty string."""
        builder = _open_store()
        result = builder.info("/")
        assert len(result) > 0

    def test_info_at_group(self):
        """info at a named group returns a non-empty string."""
        builder = _open_store()
        builder.create_group("region")
        result = builder.info("region")
        assert len(result) > 0

    def test_info_at_array(self):
        """info at an array path returns a non-empty string."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32,
                              preset="field", dimension_names=["x"])
        result = builder.info("data")
        assert len(result) > 0

    def test_raises_when_store_not_open(self):
        """info raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.info()

    def test_raises_on_missing_path(self):
        """info raises KeyError when the path does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.info("nonexistent")


# ---------------------------------------------------------------------------
# info_complete
# ---------------------------------------------------------------------------

class TestInfoComplete:
    def test_returns_string_for_array(self):
        """info_complete returns a string for an array path."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32,
                              preset="field", dimension_names=["x"])
        result = builder.info_complete("data")
        assert isinstance(result, str)

    def test_raises_for_group_path(self):
        """info_complete raises TypeError when the path points to a group."""
        builder = _open_store()
        builder.create_group("region")
        with pytest.raises(TypeError):
            builder.info_complete("region")

    def test_raises_when_store_not_open(self):
        """info_complete raises RuntimeError when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        with pytest.raises(RuntimeError):
            builder.info_complete("data")

    def test_raises_on_missing_path(self):
        """info_complete raises KeyError when the path does not exist."""
        builder = _open_store()
        with pytest.raises(KeyError):
            builder.info_complete("nonexistent")

    def test_result_is_non_empty(self):
        """info_complete returns a non-empty string."""
        builder = _open_store()
        builder.create_array("data", shape=(4,), dtype=np.float32,
                              preset="field", dimension_names=["x"])
        assert len(builder.info_complete("data")) > 0


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

class TestValidate:
    def test_returns_dict_with_required_keys(self):
        """validate returns a dict with 'valid' and 'issues' keys."""
        builder = _open_store()
        result = builder.validate()
        assert "valid" in result
        assert "issues" in result

    def test_valid_is_bool(self):
        """The 'valid' value is a bool."""
        builder = _open_store()
        assert isinstance(builder.validate()["valid"], bool)

    def test_issues_is_list(self):
        """The 'issues' value is a list."""
        builder = _open_store()
        assert isinstance(builder.validate()["issues"], list)

    def test_empty_store_is_valid(self):
        """A freshly created empty store is valid."""
        builder = _open_store()
        result = builder.validate()
        assert result["valid"] is True
        assert result["issues"] == []

    def test_store_with_groups_and_arrays_is_valid(self):
        """A store with groups and arrays passes validation."""
        builder = _open_store()
        builder.create_group("region")
        builder.create_array("region/lat", shape=(5,), dtype=np.float64,
                              preset="coordinate", dimension_names=["lat"])
        result = builder.validate()
        assert result["valid"] is True

    def test_closed_store_returns_invalid(self):
        """validate returns invalid when the store is not open."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        result = builder.validate()
        assert result["valid"] is False
        assert len(result["issues"]) > 0

    def test_valid_false_when_issues_present(self):
        """'valid' is False whenever 'issues' is non-empty."""
        builder = ZarrStoreBuilder(
            store=MEMORY_STORE_CONFIG["store"],
            zarr=MEMORY_STORE_CONFIG["zarr"],
        )
        result = builder.validate()
        assert result["valid"] is (len(result["issues"]) == 0)


# ---------------------------------------------------------------------------
# array_pipelines
# ---------------------------------------------------------------------------

class TestArrayPipelines:
    def test_returns_dict(self):
        """array_pipelines returns a dict."""
        builder = _open_store()
        assert isinstance(builder.array_pipelines, dict)

    def test_contains_field_preset(self):
        """array_pipelines contains the 'field' preset from config."""
        builder = _open_store()
        assert "field" in builder.array_pipelines

    def test_contains_coordinate_preset(self):
        """array_pipelines contains the 'coordinate' preset from config."""
        builder = _open_store()
        assert "coordinate" in builder.array_pipelines

    def test_does_not_contain_zarr_format(self):
        """array_pipelines does not include the reserved 'zarr_format' key."""
        builder = _open_store()
        assert "zarr_format" not in builder.array_pipelines

    def test_preset_has_compressor_key(self):
        """Each preset dict contains a 'compressor' key."""
        builder = _open_store()
        for preset_name, preset in builder.array_pipelines.items():
            assert "compressor" in preset, f"preset '{preset_name}' missing 'compressor'"

    def test_preset_values_are_dicts(self):
        """Each value in array_pipelines is a dict."""
        builder = _open_store()
        for name, val in builder.array_pipelines.items():
            assert isinstance(val, dict), f"pipeline '{name}' is {type(val).__name__}"

    def test_pipelines_are_independent_copies(self):
        """Mutating a returned pipeline dict does not affect the store config."""
        builder = _open_store()
        pipelines = builder.array_pipelines
        pipelines["field"]["injected"] = "mutation"
        fresh = builder.array_pipelines
        assert "injected" not in fresh["field"]


# ---------------------------------------------------------------------------
# _load_codec
# ---------------------------------------------------------------------------

class TestLoadCodec:
    def test_none_codec_returns_none(self):
        """A config with codec=None returns None."""
        builder = _open_store()
        result = builder._load_codec({"codec": None})
        assert result is None

    def test_missing_codec_key_returns_auto(self):
        """A config dict with no 'codec' key returns 'auto'."""
        builder = _open_store()
        result = builder._load_codec({})
        assert result == "auto"

    def test_valid_codec_string_instantiates(self):
        """A valid 'module:class' codec string returns an instantiated codec."""
        builder = _open_store()
        result = builder._load_codec({"codec": "zarr.codecs.bytes:BytesCodec", "kwargs": {}})
        assert result is not None

    def test_invalid_format_raises_config_error(self):
        """A codec string without ':' raises ConfigError."""
        from goesdatabuilder.utils.config import ConfigError
        builder = _open_store()
        with pytest.raises(ConfigError):
            builder._load_codec({"codec": "invalid_no_colon"})

    def test_unknown_module_raises_config_error(self):
        """An unimportable module name raises ConfigError."""
        from goesdatabuilder.utils.config import ConfigError
        builder = _open_store()
        with pytest.raises(ConfigError):
            builder._load_codec({"codec": "nonexistent.module:SomeCodec"})

    def test_unknown_class_raises_config_error(self):
        """A valid module but unknown class raises ConfigError."""
        from goesdatabuilder.utils.config import ConfigError
        builder = _open_store()
        with pytest.raises(ConfigError):
            builder._load_codec({"codec": "zarr.codecs.bytes:NonExistentCodec"})


# ---------------------------------------------------------------------------
# _ensure_numpy
# ---------------------------------------------------------------------------

class TestEnsureNumpy:
    def test_numpy_array_passthrough(self):
        """A numpy array is returned unchanged."""
        arr = np.array([1.0, 2.0, 3.0])
        result = ZarrStoreBuilder._ensure_numpy(arr)
        assert isinstance(result, np.ndarray)
        np.testing.assert_array_equal(result, arr)

    def test_list_converted_to_numpy(self):
        """A plain Python list is converted to a numpy array."""
        result = ZarrStoreBuilder._ensure_numpy([1, 2, 3])
        assert isinstance(result, np.ndarray)

    def test_xarray_dataarray_converted(self):
        """An xarray DataArray is converted to a numpy array."""
        da = xr.DataArray(np.array([1.0, 2.0, 3.0]))
        result = ZarrStoreBuilder._ensure_numpy(da)
        assert isinstance(result, np.ndarray)
        np.testing.assert_array_equal(result, [1.0, 2.0, 3.0])

    def test_dask_array_computed(self):
        """A Dask array is computed and returned as numpy."""
        import dask.array as da
        dask_arr = da.from_array(np.array([4.0, 5.0, 6.0]), chunks=3)
        result = ZarrStoreBuilder._ensure_numpy(dask_arr)
        assert isinstance(result, np.ndarray)
        np.testing.assert_array_equal(result, [4.0, 5.0, 6.0])

    def test_scalar_converted(self):
        """A Python scalar is converted to a numpy array."""
        result = ZarrStoreBuilder._ensure_numpy(42.0)
        assert isinstance(result, np.ndarray)

    def test_values_preserved_for_xarray(self):
        """_ensure_numpy preserves values when converting from xarray."""
        data = np.arange(12, dtype=np.float32).reshape(3, 4)
        da = xr.DataArray(data)
        result = ZarrStoreBuilder._ensure_numpy(da)
        np.testing.assert_array_equal(result, data)

    def test_values_preserved_for_dask(self):
        """_ensure_numpy preserves values when computing from dask."""
        import dask.array as da
        data = np.linspace(0, 1, 10)
        dask_arr = da.from_array(data, chunks=5)
        result = ZarrStoreBuilder._ensure_numpy(dask_arr)
        np.testing.assert_array_almost_equal(result, data)