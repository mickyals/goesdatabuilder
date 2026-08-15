import importlib
import logging
import os
from collections.abc import Callable, Mapping
from functools import wraps
from pathlib import Path
from typing import Any, Literal, TypedDict

import dask.array as da
import numpy as np
import xarray as xr
import zarr
from zarr.abc.codec import Codec
from zarr.abc.store import Store
from zarr.core.array import ShardsLike
from zarr.storage import FsspecStore, LocalStore, MemoryStore, ObjectStore, ZipStore

from goesdatabuilder.utils.config import ConfigDefault, ConfigError, ConfigMixin

logger = logging.getLogger(__name__)


class CodecDefinition(TypedDict):
    """Type definition for dictionary that defines a zarr codec."""

    codec: str | None
    kwargs: dict[str, Any]


class ArrayPreset(TypedDict):
    """Type definition for configuration for building a zarr array."""

    compressor: CodecDefinition
    serializer: CodecDefinition
    filters: list[CodecDefinition]
    fill_value: Any
    chunks: tuple[int, ...] | Literal["auto"]
    shards: ShardsLike | None


type ArrayPresetLike = ArrayPreset | Literal["auto"] | None


def _require_open(func: Callable) -> Callable:
    @wraps(func)
    def _(self: "ZarrStoreBuilder", *args, **kwargs) -> Any:
        if not self.is_open:
            raise RuntimeError("Store not open. Call open_store first.")
        return func(self, *args, **kwargs)

    return _


class ZarrStoreBuilder(ConfigMixin):
    """
    Config-driven builder for Zarr V3 datasets.

    Handles store lifecycle, groups, arrays, coordinates, and metadata.
    Domain-agnostic — subclasses add semantic meaning.
    """

    ############################################################################################
    # CLASS VARIABLES
    ############################################################################################

    _VALID_STORE_TYPES = {
        "local": LocalStore,
        "memory": MemoryStore,
        "fsspec": FsspecStore,
        "zip": ZipStore,
        "object": ObjectStore,
    }

    ############################################################################################
    # INITIALIZATION & CONFIG
    ############################################################################################
    def __init__(
        self,
        store_path: str | os.PathLike | None = ConfigDefault("store", "path"),
        store_type: str = ConfigDefault("store", "type"),
        object_store_backend: str | None = ConfigDefault("store", "object_store_backend"),
        storage_options: dict[str, Any] = ConfigDefault("store", "storage_options"),
    ) -> None:
        """
        Initialize a ZarrStoreBuilder.

        :param config_path: Path to the configuration file.
        :raises ConfigError: If the configuration file is invalid.
        """
        # Initialize instance variables
        # self._store: An instance of a Zarr V3 store
        # self._root: The root group of the Zarr V3 store
        # self._store_path: The path to the Zarr V3 store file
        self._root = None
        self._store, self._store_path = self._resolve_store(
            store_path, store_type, object_store_backend, storage_options
        )

    ############################################################################################
    # PROPERTIES
    ############################################################################################

    @property
    def store(self) -> Store:
        """
        The ZarrStore object associated with this configuration.

        :return: The ZarrStore object associated with this configuration.
        :rtype: ZarrStore
        """
        return self._store

    @property
    def root(self) -> zarr.Group:
        """
        The root group of the Zarr store.

        :return: The root group of the Zarr store.
        :rtype: zarr.Group
        """
        return self._root

    @property
    def array_pipelines(self) -> dict:
        """
        All array pipeline configurations defined in the config.

        Returns a dictionary mapping preset names to their pipeline
        configurations (compression, chunks, fill_value, etc.).

        :return: Dictionary of {preset_name: pipeline_config}.
        :rtype: dict
        """
        pipelines = {}
        zarr_config = self._config["store"]["presets"]
        reserved_keys = {"zarr_format"}

        for key, value in zarr_config.items():
            if key not in reserved_keys and isinstance(value, dict):
                pipelines[key] = value.copy()

        return pipelines

    @property
    def is_open(self) -> bool:
        """
        Whether the store is currently open.

        The store is considered open if it has been initialized and
        has not been closed.

        :return: Whether the store is currently open.
        :rtype: bool
        """
        return self._store is not None and self._root is not None

    @property
    def store_path(self) -> Path | None:
        """
        The path to the Zarr store.

        :return: The path to the store if initialized, None otherwise.
        :rtype: Optional[Path]
        """
        return self._store_path

    ############################################################################################
    # STORE LIFECYCLE
    ############################################################################################

    def open_store(self, mode: str = "r+") -> zarr.Group:
        """Open the zarr store at it's root."""
        self._root = zarr.open_group(self._store, mode=mode, zarr_format=3)
        return self._root

    def close_store(self) -> None:
        """
        Close the store and release any system resources.

        This method closes the store and releases any system resources. If the store has
        already been closed, this method does nothing.

        """
        if self._store is not None:
            if hasattr(self._store, "close"):
                self._store.close()  # Close the store
            self._root = None  # Release the root group object

    def __enter__(self) -> "ZarrStoreBuilder":
        """
        Enter the runtime context related to this object.

        This method is called when the execution passes to the line right after an object of this class is used in a with statement.

        :return: The object itself.
        :rtype: ZarrStoreBuilder
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """
        Exit the runtime context related to this object.

        This method is called when the execution passes to the line right after an object of this class is used in a with statement.

        This method closes the store and releases any system resources.

        :param exc_type: The type of the exception that was thrown (if any).
        :param exc_val: The value of the exception that was thrown (if any).
        :param exc_tb: The traceback of the exception that was thrown (if any).
        """
        self.close_store()

    def _resolve_store(
        self,
        store_path: str | os.PathLike | None,
        store_type: str,
        object_store_backend: str | None,
        storage_options: dict[str, Any],
    ) -> Store:
        """
        Resolve and instantiate a writable store backend from config.

        :param store_path: custom path for the store (cannot be None except for memory stores)
        :return: Tuple of (store_instance, store_path)
        :raises ConfigError: If store type is invalid
        :raises FileExistsError: If store exists and overwrite is False
        """
        if store_type not in self._VALID_STORE_TYPES:
            raise ConfigError(f"Invalid store type: {store_type}")

        if store_type == "memory":
            return MemoryStore(), None

        elif store_type == "local":
            if store_path is None:
                raise ValueError("store_path required for LocalStore")
            store_path = Path(store_path)
            return LocalStore(root=store_path), store_path

        elif store_type == "zip":
            if store_path is None:
                raise ValueError("store_path required for ZipStore")
            store_path = Path(store_path)
            # mode="a" in case the store already exists
            return ZipStore(path=str(store_path), mode="a"), store_path

        elif store_type == "fsspec":
            if store_path is None:
                raise ValueError("store_path (URL) required for FsspecStore")
            return FsspecStore.from_url(store_path, **storage_options), store_path

        elif store_type == "object":
            logger.info("Store type = object. This is experimental and error free functionality is not guaranteed.")
            obstore_instance = self._build_obstore(object_store_backend, storage_options)
            return ObjectStore(store=obstore_instance), store_path

    ############################################################################################
    # GROUP MANAGEMENT
    ############################################################################################

    @_require_open
    def create_group(self, path: str, attrs: dict = None) -> zarr.Group:
        """
        Create a new group in the store.

        This method creates a new group in the store with the given path and attributes.
        If the group already exists, a ValueError is raised.

        :param path: The path of the group to create.
        :param attrs: The attributes of the group.
        :return: The newly created group.
        :rtype: zarr.Group
        :raises ValueError: If the group already exists.
        :raises RuntimeError: If the store is not open.
        """
        if self.group_exists(path):
            raise ValueError(f"Group already exists at '{path}'")

        group = self._root.create_group(path)

        if attrs:
            group.attrs.update(attrs)

        return group

    @_require_open
    def get_group(self, path: str) -> zarr.Group:
        """
        Get a group from the store.

        This method retrieves a group from the store with the given path.

        :param path: The path of the group to retrieve.
        :return: The retrieved group.
        :rtype: zarr.Group
        :raises RuntimeError: If the store is not open.
        :raises KeyError: If the group does not exist.
        """
        try:
            node = self._root[path]
            if not isinstance(node, zarr.Group):
                raise KeyError(f"Path '{path}' is not a group")
            return node
        except KeyError:
            raise KeyError(f"Group not found at '{path}'")

    @_require_open
    def group_exists(self, path: str) -> bool:
        """
        Check if a group exists in the store.

        This method checks if a group exists in the store with the given path.

        :param path: The path of the group to check.
        :return: True if the group exists, False otherwise.
        :raises RuntimeError: If the store is not open.
        """
        try:
            node = self._root[path]
            # Check if the node is a group
            return isinstance(node, zarr.Group)
        except KeyError:
            # If the path does not exist, return False
            return False

    @_require_open
    def list_groups(self, path: str = "/") -> list[str]:
        """
        List all groups at a given path.

        This method lists all groups at a given path in the store.

        :param path: The path of the parent group.
        :return: A list of group names.
        :raises RuntimeError: If the store is not open.
        """
        if path == "/":
            parent = self._root
        else:
            parent = self.get_group(path)

        # Get all group names from the parent group
        group_names = []
        for name, _ in parent.groups():
            group_names.append(name)

        return group_names

    ############################################################################################
    # ARRAY MANAGEMENT
    ############################################################################################

    @_require_open
    def create_array(
        self,
        path: str,
        shape: tuple,
        dtype: np.dtype,
        attrs: dict = None,
        dimension_names: list = None,
        preset: ArrayPresetLike = None,
        **overrides,
    ) -> zarr.Array:
        """
        Create a new array in the store using an array pipeline preset from the config.

        Wraps zarr's create_array, resolving the codec pipeline (compressor, filters,
        serializer) and array parameters (chunks, shards, fill_value) from the named
        preset. Any keyword arguments passed via **overrides will take precedence over
        the preset values.

        :param path: Hierarchical path for the array (e.g. "data/temperature").
                If the path contains "/", intermediate groups must already exist.
        :param shape: Shape of the array.
        :param dtype: NumPy-compatible data type.
        :param attrs: Optional CF-compliant or user-defined metadata to attach to the array.
        :param preset: Name of the array pipeline preset defined under the "presets" key in
                the config (e.g. "field", "coordinate").
        :param dimension_names: Dimension labels for the array axes.
                Defaults to ["t", "lat", "lon"] if not provided.
        :param overrides: Additional keyword arguments that override individual fields
            in the preset (e.g. chunks=(100, 100), fill_value=-9999).\

            :return: The newly created zarr array.

        """
        if self.array_exists(path):
            raise ValueError(f"Array already exists at '{path}'")

        # Get defaults from config
        array_config = {**self._get_array_configuration(preset), **overrides}

        logger.debug(
            f"create_array: path={path}, shape={shape}, preset='{preset}', "
            f"chunks={array_config.get('chunks')}, shards={array_config.get('shards')}"
        )

        # Determine parent group
        if "/" in path:
            parent_path, array_name = path.rsplit("/", 1)
            parent = self.get_group(parent_path)
        else:
            parent = self._root
            array_name = path

        arr = parent.create_array(
            name=array_name,
            shape=shape,
            dtype=dtype,
            chunks=array_config.get("chunks", "auto"),
            shards=array_config.get("shards"),
            compressors=self._load_codec(array_config.get("compressors", "auto")),
            serializer=self._load_codec(array_config.get("serializer", "auto")),
            filters=self._load_codec(array_config.get("filters", "auto")),
            fill_value=array_config.get("fill_value"),
            dimension_names=dimension_names or ["t", "lat", "lon"],
        )

        if attrs:
            arr.attrs.update(attrs)

        logger.info(f"New array created at path {path}")

        return arr

    @_require_open
    def get_array(self, path: str) -> zarr.Array:
        """
        Get an array from the store.

        This method gets an array from the store with the given path.

        :param path: The path of the array to retrieve.
        :return: The retrieved array.
        :rtype: zarr.Array
        :raises RuntimeError: If the store is not open.
        :raises KeyError: If the array does not exist.
        """
        try:
            node = self._root[path]
            if not isinstance(node, zarr.Array):
                raise KeyError(f"Path '{path}' is not an array")
            # Check if the node is an array
            return node
        except KeyError:
            # If the path does not exist, raise a KeyError
            raise KeyError(f"Array not found at '{path}'")

    @_require_open
    def array_exists(self, path: str) -> bool:
        """
        Check if an array exists in the store.

        This method checks if an array exists in the store with the given path.

        :param path: The path of the array to check.
        :return: True if the array exists, False otherwise.
        :raises RuntimeError: If the store is not open.
        """
        try:
            node = self._root[path]
            # Check if the node is an array
            return isinstance(node, zarr.Array)
        except KeyError:
            # If the path does not exist, return False
            return False

    @_require_open
    def array_list(self, path: str = "/") -> list[str]:
        """
        Get a list of array names in the given path.

        :param path: The path to get the array names from. Defaults to "/".
        :return: A list of array names.
        :raises RuntimeError: If the store is not open.
        """
        # Determine parent group
        if path == "/":
            parent = self._root
        else:
            parent = self.get_group(path)

        # Get all group names from the parent group
        array_names = []
        for name, _ in parent.arrays():
            array_names.append(name)

        return array_names

    @_require_open
    def write_array(self, path: str, data: Any, selection: int | slice | tuple[int | slice, ...] | None = None) -> None:
        """
        Write data to array. If selection is None, writes to entire array.

        Writes the given data to the array at the given path. If selection is None,
        the data is written to the entire array. Otherwise, the data is written to
        the specified selection of the array.

        :param path: The path of the array to write to.
        :param data: The data to write (numpy array, dask array, or xarray DataArray).
        :param selection: The selection of the array to write to. If None, writes to entire array.
        :raises RuntimeError: If the store is not open.
        :raises KeyError: If the array does not exist.
        """
        arr = self.get_array(path)

        # Ensure data is a numpy array
        data = self._ensure_numpy(data)

        if selection is None:
            # Write data to entire array
            selection = ...

        arr[selection] = data

    @_require_open
    def is_empty(self, path: str, selection: tuple[int | slice, ...] = None) -> bool:
        """Return True if no data has been written to the selection for the given array."""
        if selection is None:
            selection = ...

        arr = self.get_array(path)

        if arr.nchunks_initialized == 0:
            return True

        data = arr[selection]
        return np.array_equal(data, np.full(data.shape, arr.fill_value), equal_nan=arr.dtype.kind not in "USTOV")

    ############################################################################################
    # METADATA MANAGEMENT
    ############################################################################################

    @_require_open
    def get_attrs(self, path: str = "/") -> dict:
        """
        Get the attributes of a node.

        :param path: The path of the node to get attributes from. Defaults to "/".
        :return: A dictionary of the node's attributes.
        :raises RuntimeError: If the store is not open.
        """
        node = self._get_node(path)
        return dict(node.attrs)

    @_require_open
    def set_attrs(self, path: str, attrs: dict, merge: bool = True) -> None:
        """
        Set attributes of a node.

        Attributes are key-value pairs that store metadata about the node.
        If merge is True, the attributes are merged with the existing attributes.
        If merge is False, the existing attributes are cleared and replaced with the new attributes.

        :param path: The path of the node to set attributes for.
        :param attrs: The attributes to set.
        :param merge: If True, merge the attributes with the existing attributes. If False, clear the existing attributes before setting the new attributes.
        :raises RuntimeError: If the store is not open.
        """
        node = self._get_node(path)

        if merge:
            # Merge with existing attributes
            node.attrs.update(attrs)
        else:
            # Clear existing and set new
            node.attrs.clear()
            node.attrs.update(attrs)

    @_require_open
    def del_attrs(self, path: str, keys: list[str]) -> None:
        """
        Delete attributes from a node.

        :param path: The path of the node to delete attributes from.
        :param keys: A list of attribute names to delete.
        :raises RuntimeError: If the store is not open.
        """
        node = self._get_node(path)

        # Iterate over the keys and try to delete each attribute
        # If the attribute does not exist, a KeyError is raised
        # We ignore this error since it means the attribute was not found
        for key in keys:
            try:
                del node.attrs[key]
            except KeyError:
                # Ignore KeyError if attribute does not exist
                logger.info(f"{key} is not present within attrbites, skipping. ")
                pass

    ############################################################################################
    # INFO & UTILITIES
    ############################################################################################

    @_require_open
    def tree(self, path: str = "/") -> str:
        """
        Generate a tree view of the hierarchy.

        This method generates a tree view of the hierarchy starting from the given path.
        The tree view is a string representation of the hierarchy with each level indented.

        :param path: The path of the node to start the tree view from. Defaults to "/".
        :return: A string representation of the tree view.
        :raises RuntimeError: If the store is not open.
        """
        # Initialize an empty list to store the tree view lines
        lines = []

        # Define a nested function to walk the hierarchy
        def _walk(node: zarr.Group | zarr.Array, prefix: str = "", name: str = "") -> None:
            """
            Walk the hierarchy and generate the tree view.

            :param node: The node to walk.
            :param prefix: The prefix string to use for indentation.
            :param name: The name of the node.
            """
            if isinstance(node, zarr.Group):
                # If the node is a group, add a line to the tree view
                lines.append(f"{prefix}{name}/" if name else "/")
                # Get the children of the group
                children = list(node.groups()) + list(node.arrays())
                # Iterate over the children and walk them
                for i, (child_name, child) in enumerate(children):
                    is_last = i == len(children) - 1
                    connector = "└── " if is_last else "├── "
                    new_prefix = prefix + ("    " if is_last else "│   ")
                    _walk(child, new_prefix, connector + child_name)
            else:
                # If the node is an array, add a line to the tree view
                lines.append(f"{prefix}{name} [{node.dtype}, {node.shape}]")

        # Get the starting node
        start_node = self._get_node(path)
        # Walk the hierarchy starting from the starting node
        _walk(start_node)

        # Join the tree view lines with newline characters
        return "\n".join(lines)

    @_require_open
    def info(self, path: str = "/") -> str:
        """
        Get the info string for the node at the given path.

        The info string is a string representation of the node's metadata.
        It contains information such as the node's name, type, and shape.

        :param path: The path of the node to get the info string from. Defaults to "/".
        :return: The info string of the node.
        :raises RuntimeError: If the store is not open.
        """
        node = self._get_node(path)
        # Get the info string of the node
        return str(node.info)

    @_require_open
    def info_complete(self, path: str) -> str:
        """
        Get detailed storage statistics for an array.

        This method returns a detailed info string for the array at the given path.
        The info string contains information such as the array's name, type, shape, and storage statistics.

        Note: This method can be slow for large arrays since it needs to traverse the entire array to gather the statistics.

        :param path: The path of the array to get the detailed storage statistics for.
        :return: The detailed info string of the array.
        :raises RuntimeError: If the store is not open.
        :raises TypeError: If the node at the given path is not an array.
        """
        node = self._get_node(path)

        if not isinstance(node, zarr.Array):
            raise TypeError(f"info_complete only supports arrays, got group at '{path}'")

        # Get the detailed info string of the array
        # This string contains information such as the array's name, type, shape, and storage statistics
        return str(node.info_complete())

    def validate(self) -> dict:
        """
        Check store integrity.

        This method checks the integrity of the store and returns a dictionary
        containing the validation result and a list of issues found.

        :return: A dictionary containing the validation result and a list of issues found.
        :rtype: dict[str, bool | list[str]]
        """
        issues = []

        if not self.is_open:
            return {"valid": False, "issues": ["Store not open"]}

        # Define a nested function to recursively check the nodes in the store
        def _check_node(node: zarr.Group | zarr.Array, path: str) -> None:
            """
            Recursively check the nodes in the store.

            :param node: The node to check.
            :param path: The path of the node.
            """
            try:
                if isinstance(node, zarr.Group):
                    # If the node is a group, recursively check its children
                    for name, child in node.groups():
                        _check_node(child, f"{path}/{name}")
                    for name, child in node.arrays():
                        _check_node(child, f"{path}/{name}")
                elif isinstance(node, zarr.Array):
                    # If the node is an array, check its shape and dtype
                    _ = node.shape
                    _ = node.dtype
            except Exception as e:
                # If an error occurs, add the error message to the issues list
                issues.append(f"Error at '{path}': {e}")

        # Start the recursive check from the root node
        _check_node(self._root, "")

        # Return the validation result and the list of issues found
        return {"valid": len(issues) == 0, "issues": issues}

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        if not self.is_open:
            return "ZarrStoreBuilder(not initialized)"

        num_groups = len(list(self._root.groups()))
        num_arrays = len(list(self._root.arrays()))
        store_path = self._store_path or "memory"

        return f"ZarrStoreBuilder(store={store_path}, groups={num_groups}, arrays={num_arrays})"

    def _get_array_configuration(self, preset: ArrayPresetLike) -> dict:
        """
        Get array pipeline configuration from config.

        If the preset is None then return an empty dictionary, the calling function
        is then responsible for filling in the preset values.

        If the preset is a dictionary then return it as is since we assume this
        represents the preset dictionary already.

        If the preset is a string then retrieve the configuration with the given
        name from the zarr configuation.

        :param preset: preset name or array configuration.
        :return: array configuration configuration.
        :raises ConfigError: If the preset name is not found in the configuration.
        """
        if preset is None:
            return {}
        if isinstance(preset, dict):
            return preset
        try:
            return self._config["store"]["presets"][preset]
        except KeyError as e:
            raise ConfigError(f"Array pipeline preset '{preset}' not found in config") from e

    # this doesn't actually return a zarr.Store it returns a obstore Store but since obstore
    # is an optional dependency we can annotate this as a zarr.Store because the interface is
    # the same for the purposes of this code.
    def _build_obstore(self, backend: str, storage_options: dict[str, Any]) -> Store:
        """
        Build object store backend from configuration.

        Creates and configures an object store backend based on the store
        configuration. Supports multiple cloud storage providers and local
        storage through the obstore package.

        Supported Backends:
            - s3: Amazon S3 storage (requires bucket, region, credentials)
            - gcs: Google Cloud Storage (requires bucket, credentials)
            - azure: Azure Blob Storage (requires container, account credentials)
            - memory: In-memory storage for testing and temporary operations

        Configuration Requirements:
            - store.backend: Backend type to use
            - store.bucket: Bucket/container name (for cloud backends)
            - store.region: Geographic region (for S3)
            - store.account: Account name (for Azure)
            - store.storage_options: Additional backend-specific options
            - store.anonymous: Whether to use anonymous access

        Args:
            storage_options: Dictionary containing additional keyword arguments to pass to
                             the obstore class' initializer.

        Returns
        -------
            Configured obstore instance ready for use with Zarr

        Raises
        ------
            ConfigError: If obstore package is unavailable or backend is unknown
            ImportError: If required backend packages are not installed
        """
        try:
            from obstore.store import AzureStore, GCSStore, S3Store  # type: ignore
            from obstore.store import MemoryStore as ObMemoryStore  # type: ignore
        except ImportError as e:
            raise ConfigError(
                "obstore package not available, please install it or use a different storage type."
            ) from e
        if backend == "s3":
            return S3Store(**storage_options)
        elif backend == "gcs":
            return GCSStore(**storage_options)
        elif backend == "azure":
            return AzureStore(**storage_options)
        elif backend == "memory":
            return ObMemoryStore(**storage_options)
        else:
            raise ConfigError(f"Unknown obstore backend: {backend}")

    ############################################################################################
    # CODEC LOADING UTILITIES
    ############################################################################################

    def _load_codec_from_definition(self, config: CodecDefinition) -> Codec:
        """
        Load and instantiate a codec from a dict definition of the codec.

        Codecs are specified as 'module:class_name' in the configuration
        and are dynamically imported and instantiated. This allows for flexible
        codec selection without hard-coding specific implementations.

        Codec Format:
            'module_name:ClassName' - e.g., 'numcodecs:Zstd'
            'zarr.codecs.bytes:Blosc' - e.g., for Blosc compression

        Args:
            config: Dictionary containing codec configuration with keys:
                - codec: String in format 'module:class_name'
                - kwargs: Additional keyword arguments for codec initialization

        Returns
        -------
            Instantiated codec object or None if codec is 'auto'

        Raises
        ------
            ConfigError: If codec format is invalid
            ImportError: If codec module cannot be imported
            AttributeError: If codec class is not found in module
            Exception: If codec cannot be initialized with provided arguments
        """
        codec = config["codec"]
        if ":" not in codec:
            raise ConfigError(f"Unknown codec '{codec}'. Codecs must be specified as 'module:class_name'")
        mod_name, class_name = codec.rsplit(":", 1)
        try:
            mod = importlib.import_module(mod_name)
        except ImportError as e:
            raise ConfigError(f"Unable to import codec module '{mod_name}'.") from e
        try:
            codec_class = getattr(mod, class_name)
        except AttributeError as e:
            raise ConfigError(f"Unable to find codec named '{class_name}' in module '{mod_name}'.") from e
        kwargs = config.get("kwargs", {})
        try:
            return codec_class(**kwargs)
        except Exception as e:
            raise ConfigError(f"Codec class '{codec_class}' cannot be initialized with arguments: {kwargs}") from e

    def _load_codec(
        self, config: CodecDefinition | Codec | list[CodecDefinition | Codec] | Literal["auto"] | None
    ) -> Codec | list[Codec]:
        """Return a codec argument to pass to zarr.create_array."""
        if config == "auto" or config is None:
            return config
        if isinstance(config, Codec):
            return config
        if isinstance(config, Mapping):
            return self._load_codec_from_definition(config)
        return [c if isinstance(c, Codec) else self._load_codec_from_definition(c) for c in config]

    def _get_node(self, path: str) -> zarr.Group | zarr.Array:
        """
        Get group or array at path.

        :param path: Path to the node (use "/" for root)
        :return: zarr.Group or zarr.Array at the specified path
        :raises KeyError: If path is not found
        """
        if path == "/":
            return self._root

        try:
            return self._root[path]
        except KeyError:
            raise KeyError(f"Path not found: '{path}'")

    ############################################################################################
    # DATA UTILITIES
    ############################################################################################

    @staticmethod
    def _ensure_numpy(data: Any) -> np.ndarray:
        """
        Convert Dask arrays to NumPy. Pass through NumPy arrays unchanged.

        This utility function ensures data is in NumPy format for Zarr operations.
        It handles multiple input types gracefully:

        Input Types:
            - np.ndarray: Returned unchanged (already NumPy)
            - xr.DataArray: Extracts underlying values (triggers compute if Dask-backed)
            - da.Array: Computes the array (converts Dask to NumPy)
            - Other types: Converted to NumPy using np.asarray()

        Use Cases:
            - Writing data to Zarr arrays (requires NumPy format)
            - Preparing data for compression/encoding operations
            - Ensuring compatibility with Zarr's storage requirements

        Parameters
        ----------
            data: Input data (np.ndarray, da.Array, xr.DataArray, or array-like)

        Returns
        -------
            np.ndarray: Data guaranteed to be in NumPy format

        Note:
            - This function may trigger Dask computation for DataArrays
            - Use judiciously with large datasets to avoid memory issues
        """
        # Check if input is a DataArray (xarray)
        # If so, extract the underlying data array
        if isinstance(data, xr.DataArray):
            data = data.values  # This triggers compute if Dask-backed

        # Check if input is a Dask array
        # If so, compute the array (i.e., convert it to a NumPy array)
        elif isinstance(data, da.Array):
            data = data.compute()  # Trigger computation

        # Check if input is already a NumPy array
        # If so, do nothing (just pass it through)
        elif isinstance(data, np.ndarray):
            pass  # Already NumPy, do nothing

        # If none of the above conditions are true, try converting to NumPy as a fallback
        else:
            data = np.asarray(data)  # Convert to NumPy as a fallback

        return data
