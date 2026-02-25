import json
import os
from pathlib import Path
from typing import Any

import yaml
import zarr
from zarr.storage import LocalStore, ZipStore, FsspecStore, MemoryStore, ObjectStore

class ConfigError(Exception):
    """Raised when config validation fails."""
    pass

class ZarrStoreBuilder:
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
        "object": ObjectStore
    }

    ############################################################################################
    # INITIALIZATION & CONFIG
    ############################################################################################
    def __init__(self, config_path: str | Path):
        """
        Initialize a ZarrDatasetBuilder with a configuration file.

        :param config_path: Path to the configuration file.
        :raises ConfigError: If the configuration file is invalid.
        """
        self._config = self._load_config(Path(config_path))
        self._validate_config(self._config)

        # Initialize instance variables
        # self._store: An instance of a Zarr V3 store
        # self._root: The root group of the Zarr V3 store
        # self._store_path: The path to the Zarr V3 store file
        self._store = None
        self._root = None
        self._store_path = None

    @classmethod
    def from_existing(cls, store_path: str | Path, config_path: str | Path):
        """
        Open an existing Zarr V3 store with the given config.

        This method is used to open an existing Zarr V3 store with the same configuration
        as when it was created. The store_path parameter is used to locate the store file.

        :param store_path: Path to the store file.
        :param config_path: Path to the configuration file.
        :return: An instance of the ZarrDatasetBuilder.
        """
        instance = cls(config_path)  # normal init: load & validate config
        instance.open_store(store_path, mode="r+")
        return instance

    def _load_config(self, config_path: Path) -> dict:
        """
        Load a configuration file into a Python dictionary.

        :param config_path: Path to the configuration file.
        :raises FileNotFoundError: If the configuration file does not exist.
        :raises ValueError: If the configuration file format is not supported.
        :return: A dictionary containing the configuration.
        """
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")

        content = config_path.read_text()
        suffix = config_path.suffix.lower()

        if suffix in {".yaml", ".yml"}:
            parsed = yaml.safe_load(content)
        elif suffix == ".json":
            parsed = json.loads(content)
        else:
            raise ValueError(f"Unsupported configuation file format: {suffix}. Use .yaml, .yml or .json")

        # Expand environment variables in the config
        return self._expand_env_vars(parsed)

    def _validate_config(self, config: dict) -> None:
        """
        Validate the given configuration dictionary.

        Raise a ConfigError if the configuration is invalid.

        :param config: The configuration dictionary to validate.
        :raises ConfigError: If the configuration is invalid.
        """
        # First: Required top level keys
        required_keys = {"store", "zarr"}
        missing = required_keys - config.keys()
        if missing:
            raise ConfigError(
                f"Missing required config keys: {missing}"
            )

        # Second: zarr store level validations
        store_type = config.get("store", {}).get("type", {})
        if store_type not in self._VALID_STORE_TYPES.keys():
            raise ConfigError(
                f"Invalid store.type: {store_type}. Type must be one of {self._VALID_STORE_TYPES.keys()}"
            )

        # Third: zarr arrays configuations validations
        ## ensure zarr format is 3
        zarr_format = config.get("zarr", {}).get("zarr_format")
        if zarr_format != 3:
            raise ConfigError(f"Only zarr_format=3 supported, got {zarr_format}")

        ## ensure compression configuration is added with a at least a default configuration
        compression = config.get("zarr", {}).get("compression", {})
        if compression is None:
            raise ConfigError("Missing zarr.compression in config")
        default_compression = compression.get("default", {})

        if "compressor" not in default_compression:
            raise ConfigError("zarr.compression.default must have a 'compressor' key")




    ############################################################################################
    # PROPERTIES
    ############################################################################################

    @property
    def store(self):
        """
        The ZarrStore object associated with this configuration.

        :return: The ZarrStore object associated with this configuration.
        :rtype: ZarrStore
        """
        return self._store

    @property
    def root(self):
        """
        The root group of the Zarr store.

        :return: The root group of the Zarr store.
        :rtype: zarr.Group
        """
        return self._root

    @property
    def config(self):
        """
        A deep copy of the configuration dictionary.

        This property returns a deep copy of the configuration dictionary
        associated with this ZarrDatasetBuilder. The configuration dictionary is
        immutable and cannot be changed.

        :return: A deep copy of the configuration dictionary.
        :rtype: dict
        """
        return self._config.copy()

    @property
    def default_compression(self):
        """
        The default compression pipeline.

        The default compression pipeline is the one used for created
        arrays. It is a dictionary containing the following keys:
        - codec: The name of the codec to use.
        - cname: The name of the compressor to use.
        - clevel: The compression level to use.
        - shuffle: Whether to shuffle the data before compressing.
        - serializer: The serializer to use.
        - filter: The filter to use.

        :return: The default compression pipeline.
        :rtype: dict
        """
        return self._get_codec_pipeline("default")

    @property
    def secondary_compression(self):
        """
        The secondary compression pipeline.

        The secondary compression pipeline is the one used for arrays that
        do not meet the primary compression goal. It is a dictionary containing
        the following keys:

        - codec: The name of the codec to use.
        - cname: The name of the compressor to use.
        - clevel: The compression level to use.
        - shuffle: Whether to shuffle the data before compressing.
        - serializer: The serializer to use.
        - filter: The filter to use.

        :return: The secondary compression pipeline.
        :rtype: dict
        """
        # Get the secondary compression pipeline from the config
        return self._get_codec_pipeline("secondary")

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

    ############################################################################################
    # STORE LIFECYCLE
    ############################################################################################

    def create_store(self, store_path: str | Path = None, overwrite: bool = False):
        """
        Create a ZarrStore from the given configuration.

        :param store_path: Path to the store file. If not provided, the value from the configuration will be used.
        :param overwrite: Whether to overwrite an existing store at the given path. If True, an existing store will be overwritten.
        :raises ConfigError: If the store type is not supported.
        :raises ValueError: If store_path is required but not provided.
        :raises FileExistsError: If the store already exists at the given path and overwrite is False.
        """
        store_type = self._config["store"]["type"]
        if store_type not in self._VALID_STORE_TYPES:
            raise ConfigError(f"Invalid store type: {store_type}")

        # Arg overrides config
        if store_path is None:
            store_path = self._config["store"].get("path")

        if store_type == "memory":
            # MemoryStore is a store that resides in memory
            self._store = MemoryStore()
            self._store_path = None

        elif store_type == "local":
            # LocalStore is a store that resides on disk
            if store_path is None:
                raise ValueError("store_path required for LocalStore")
            store_path = Path(store_path)

            if store_path.exists():
                if overwrite:
                    import shutil
                    shutil.rmtree(store_path)
                else:
                    raise FileExistsError(f"Store already exists at {store_path}")

            self._store = LocalStore(root=store_path)
            self._store_path = store_path

        elif store_type == "zip":
            # ZipStore is a store that resides in a zip file
            if store_path is None:
                raise ValueError("store_path required for ZipStore")
            store_path = Path(store_path)

            if store_path.exists() and not overwrite:
                raise FileExistsError(f"Store already exists at {store_path}")

            self._store = ZipStore(path=str(store_path), mode="w")
            self._store_path = store_path

        elif store_type == "fsspec":
            # FsspecStore is a store that resides in an FSSPEC-compatible storage system
            if store_path is None:
                raise ValueError("store_path (URL) required for FsspecStore")

            storage_options = self._config["store"].get("storage_options", {})
            self._store = FsspecStore.from_url(store_path, **storage_options)
            self._store_path = store_path

        elif store_type == "object":
            # ObjectStore is a store that resides in an object storage system
            print("You have selected Store Type = object. Note this is still experimental and functionality is not assured")
            obstore_instance = self._build_obstore()
            self._store = ObjectStore(store=obstore_instance)
            self._store_path = store_path

        self._root = zarr.open_group(store=self._store, mode="w", zarr_format=3)


    def open_store(self, store_path: str | Path = None, mode: str = "r+"):
        """
        Open an existing store.

        This method is used to open an existing store with the given configuration.
        The store type is determined by the configuration.

        :param store_path: The path to the store file.
        :param mode: The mode to open the store in. Must be "r" for read-only or "r+" for read-write.
        :raises ValueError: If the store type is invalid.
        :raises FileNotFoundError: If the store path does not exist.
        """
        store_type = self._config["store"]["type"]

        if store_path is None:
            store_path = self._config["store"].get("path")

        if store_type == "memory":
            raise ValueError("Cannot open a MemoryStore; use create_store instead")

        elif store_type == "local":
            if store_path is None:
                raise ValueError("store_path required for LocalStore")
            store_path = Path(store_path)
            if not store_path.exists():
                raise FileNotFoundError(f"Store not found at {store_path}")

            read_only = (mode == "r")
            self._store = LocalStore(root=store_path, read_only=read_only)
            self._store_path = store_path

        elif store_type == "zip":
            if store_path is None:
                raise ValueError("store_path required for ZipStore")
            store_path = Path(store_path)
            if not store_path.exists():
                raise FileNotFoundError(f"Store not found at {store_path}")

            zip_mode = "r" if mode == "r" else "a"  # append for r+
            self._store = ZipStore(path=str(store_path), mode=zip_mode)
            self._store_path = store_path

        elif store_type == "fsspec":
            if store_path is None:
                raise ValueError("store_path (URL) required for FsspecStore")

            storage_options = self._config["store"].get("storage_options", {})
            read_only = (mode == "r")
            self._store = FsspecStore.from_url(store_path, read_only=read_only, **storage_options)
            self._store_path = store_path

        elif store_type == "object":
            read_only = (mode == "r")
            obstore_instance = self._build_obstore()
            self._store = ObjectStore(store=obstore_instance, read_only=read_only)
            self._store_path = store_path

        self._root = zarr.open_group(store=self._store, mode=mode)

    def close_store(self):
        """
        Close the store and release any system resources.

        This method closes the store and releases any system resources. If the store has
        already been closed, this method does nothing.

        :raises ValueError: If the store type is invalid.
        """
        if self._store is not None:
            if hasattr(self._store, 'close'):
                self._store.close()  # Close the store
            self._store = None  # Release the store object
            self._root = None  # Release the root group object
            self._store_path = None  # Release the store path string


    def __enter__(self):
        """
        Enter the runtime context related to this object.

        This method is called when the execution passes to the line right after an object of this class is used in a with statement.

        :return: The object itself.
        :rtype: ZarrDatasetBuilder
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context related to this object.

        This method is called when the execution passes to the line right after an object of this class is used in a with statement.

        This method closes the store and releases any system resources.

        :param exc_type: The type of the exception that was thrown (if any).
        :param exc_val: The value of the exception that was thrown (if any).
        :param exc_tb: The traceback of the exception that was thrown (if any).
        """
        self.close_store()

    ############################################################################################
    # GROUP MANAGEMENT
    ############################################################################################

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
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        if self.group_exists(path):
            raise ValueError(f"Group already exists at '{path}'")

        group = self._root.create_group(path)

        if attrs:
            group.attrs.update(attrs)

        return group

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
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        try:
            node = self._root[path]
            if not isinstance(node, zarr.Group):
                raise KeyError(f"Path '{path}' is not a group")
            return node
        except KeyError:
            raise KeyError(f"Group not found at '{path}'")

    def group_exists(self, path: str) -> bool:
        """
        Check if a group exists in the store.

        This method checks if a group exists in the store with the given path.

        :param path: The path of the group to check.
        :return: True if the group exists, False otherwise.
        :raises RuntimeError: If the store is not open.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        try:
            node = self._root[path]
            # Check if the node is a group
            return isinstance(node, zarr.Group)
        except KeyError:
            # If the path does not exist, return False
            return False

    def list_groups(self, path: str = "/") -> list[str]:
        """
        List all groups at a given path.

        This method lists all groups at a given path in the store.

        :param path: The path of the parent group.
        :return: A list of group names.
        :raises RuntimeError: If the store is not open.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

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

    def create_array(self, path: str, shape: tuple, dtype, chunks: tuple = None, shards: tuple = None,
                     compressor = None, fill_value = None, attrs: dict = None, preset: str = "default"
                     ) -> zarr.Array:
        """
        Create a new array in the store.

        This method creates a new array in the store with the given path, shape, dtype, chunks, compressor, fill_value, and attributes.

        :param path: The path of the array to create.
        :param shape: The shape of the array.
        :param dtype: The data type of the array.
        :param chunks: The chunk size of the array.
        :param compressor: The compressor to use.
        :param fill_value: The fill value of the array.
        :param attrs: The attributes of the array.
        :param preset: The compression preset to use.
        :return: The newly created array.
        :rtype: zarr.Array
        :raises RuntimeError: If the store is not open.
        :raises ValueError: If the array already exists.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        if self.array_exists(path):
            raise ValueError(f"Array already exists at '{path}'")

        # Get defaults from config
        compression_config = self._get_codec_pipeline(preset)

        if chunks is None:
            chunks = compression_config.get("chunks", "auto")
        if shards is None:
            shards = compression_config.get("shards")
        if fill_value is None:
            fill_value = compression_config.get("fill_value", -999)

        # Build codec pipeline - note the nested keys now
        compressor = self._build_compressor(compression_config.get("compressor", {})) #todo, handle config exceptions here
        filters = self._build_filters(compression_config.get("filter", {})) #todo, handle config exceptions here
        serializer = self._build_serializer(compression_config.get("serializer", {})) #todo, handle config exceptions here

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
            chunks=chunks,
            shards=shards,
            compressors=compressor,
            serializer=serializer,
            filters=filters,
            fill_value=fill_value,
        )

        if attrs:
            arr.attrs.update(attrs)

        return arr

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
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        try:
            node = self._root[path]
            if not isinstance(node, zarr.Array):
                raise KeyError(f"Path '{path}' is not an array")
            # Check if the node is an array
            return node
        except KeyError:
            # If the path does not exist, raise a KeyError
            raise KeyError(f"Array not found at '{path}'")

    def array_exists(self, path: str) -> bool:
        """
        Check if an array exists in the store.

        This method checks if an array exists in the store with the given path.

        :param path: The path of the array to check.
        :return: True if the array exists, False otherwise.
        :raises RuntimeError: If the store is not open.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        try:
            node = self._root[path]
            # Check if the node is an array
            return isinstance(node, zarr.Array)
        except KeyError:
            # If the path does not exist, return False
            return False

    def array_list(self, path: str = "/") -> list[str]:
        """
        Get a list of array names in the given path.

        :param path: The path to get the array names from. Defaults to "/".
        :return: A list of array names.
        :raises RuntimeError: If the store is not open.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

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

    def resize_array(self, path: str, new_shape: tuple):
        """
        Resize an array in the store.

        This method resizes an array in the store with the given path to the new shape.

        :param path: The path of the array to resize.
        :param new_shape: The new shape of the array.
        :raises RuntimeError: If the store is not open.
        :raises KeyError: If the array does not exist.
        """
        arr = self.get_array(path)
        # Resize the array
        arr.resize(new_shape)

    def append_array(self, path: str, data, axis: int = 0, return_location: bool = False) -> tuple[int, int] | None:
        """
        Append data along the given axis. Returns (start_idx, end_idx) of the written region.

        This method appends data to an array in the store along the given axis.
        It first retrieves the array from the store and then resizes it to make room
        for the new data. It then writes the data to the end of the array along the
        given axis. If return_location is True, it returns a tuple containing the start
        index and end index of the written region.

        :param path: The path of the array to append to.
        :param data: The data to append.
        :param axis: The axis to append along. Defaults to 0.
        :param return_location: If True, returns the start and end indices of the written region.
        :return: A tuple containing the start index and end index of the written region if return_location is True, otherwise None.
        :raises RuntimeError: If the store is not open.
        :raises KeyError: If the array does not exist.
        """
        arr = self.get_array(path)

        # Ensure data is a numpy array
        data = self._ensure_numpy(data)

        # Get old shape of array
        old_shape = arr.shape

        # Calculate new length of array
        new_len = old_shape[axis] + data.shape[axis]

        # Build new shape
        new_shape = list(old_shape)
        new_shape[axis] = new_len

        # Resize array
        arr.resize(tuple(new_shape))

        # Calculate start and end indices
        start_idx = old_shape[axis]
        end_idx = new_len

        # Build slices for writing
        slices = [slice(None)] * len(old_shape)
        slices[axis] = slice(start_idx, end_idx)

        # Write data at end
        arr[tuple(slices)] = data

        # Return start and end indices if requested
        if return_location:
            return (start_idx, end_idx)

    def write_array(self, path: str, data, selection: tuple = None):
        """
        Write data to array. If selection is None, writes to entire array.

        Writes the given data to the array at the given path. If selection is None,
        the data is written to the entire array. Otherwise, the data is written to
        the specified selection of the array.

        :param path: The path of the array to write to.
        :param data: The data to write.
        :param selection: The selection of the array to write to. If None, writes to entire array.
        """
        arr = self.get_array(path)

        # Ensure data is a numpy array
        data = self._ensure_numpy(data)

        if selection is None:
            # Write data to entire array
            arr[...] = data
        else:
            # Write data to specified selection of array
            arr[selection] = data

    ############################################################################################
    # METADATA MANAGEMENT
    ############################################################################################

    def get_attrs(self, path: str = "/") -> dict:
        """
        Get the attributes of a node.

        :param path: The path of the node to get attributes from. Defaults to "/".
        :return: A dictionary of the node's attributes.
        :raises RuntimeError: If the store is not open.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        node = self._get_node(path)
        return dict(node.attrs)

    def set_attrs(self, path: str, attrs: dict, merge: bool = True):
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
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        node = self._get_node(path)

        if merge:
            # Merge with existing attributes
            node.attrs.update(attrs)
        else:
            # Clear existing and set new
            node.attrs.clear()
            node.attrs.update(attrs)


    def del_attrs(self, path: str, keys: list[str]):
        """
        Delete attributes from a node.

        :param path: The path of the node to delete attributes from.
        :param keys: A list of attribute names to delete.
        :raises RuntimeError: If the store is not open.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        node = self._get_node(path)

        # Iterate over the keys and try to delete each attribute
        # If the attribute does not exist, a KeyError is raised
        # We ignore this error since it means the attribute was not found
        for key in keys:
            try:
                del node.attrs[key]
            except KeyError:
                # Ignore KeyError if attribute does not exist
                pass


    ############################################################################################
    # INFO & UTILITIES
    ############################################################################################

    def tree(self, path: str = "/") -> str:
        """
        Generate a tree view of the hierarchy.

        This method generates a tree view of the hierarchy starting from the given path.
        The tree view is a string representation of the hierarchy with each level indented.

        :param path: The path of the node to start the tree view from. Defaults to "/".
        :return: A string representation of the tree view.
        :raises RuntimeError: If the store is not open.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        # Initialize an empty list to store the tree view lines
        lines = []

        # Define a nested function to walk the hierarchy
        def _walk(node, prefix="", name=""):
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
                    is_last = (i == len(children) - 1)
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

    def info(self, path: str = "/") -> str:
        """
        Get the info string for the node at the given path.

        The info string is a string representation of the node's metadata.
        It contains information such as the node's name, type, and shape.

        :param path: The path of the node to get the info string from. Defaults to "/".
        :return: The info string of the node.
        :raises RuntimeError: If the store is not open.
        """
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

        node = self._get_node(path)
        # Get the info string of the node
        return str(node.info)

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
        if not self.is_open:
            raise RuntimeError("Store not open. Call create_store or open_store first.")

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
        def _check_node(node, path):
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
        if not self.is_open:
            return "ZarrDatasetBuilder(not initialized)"

        num_groups = len(list(self._root.groups()))
        num_arrays = len(list(self._root.arrays()))
        store_path = self._store_path or "memory"

        return f"ZarrDatasetBuilder(store={store_path}, groups={num_groups}, arrays={num_arrays})"


    def _expand_env_vars(self, obj):
        if isinstance(obj, str):
            return os.path.expandvars(obj)
        elif isinstance(obj, dict):
            return {k: self._expand_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._expand_env_vars(item) for item in obj]
        return obj

    def _get_codec_pipeline(self, preset: str = "default") -> dict:
        compression_config = self._config["zarr"]["compression"].get(preset)

        if compression_config is None:
            raise ConfigError(f"Compression preset '{preset}' not found in config")

        return compression_config

    def _build_obstore(self):
        """Build obstore backend from config."""
        from obstore.store import S3Store, GCSStore, AzureStore, MemoryStore as ObMemoryStore

        store_config = self._config["store"]
        backend = store_config.get("backend")

        if backend == "s3":
            return S3Store(
                bucket=store_config["bucket"],
                region=store_config.get("region"),
                skip_signature=store_config.get("anonymous", False),
            )
        elif backend == "gcs":
            return GCSStore(bucket=store_config["bucket"])
        elif backend == "azure":
            return AzureStore(
                container=store_config["container"],
                account=store_config["account"],
            )
        elif backend == "memory":
            return ObMemoryStore()
        else:
            raise ConfigError(f"Unknown obstore backend: {backend}")

    def _build_compressor(self, config: dict):
        """ TODO: need to look into them individually """
        pass

    def _build_serializer(self, config: dict):
        """ TODO: need to look into them individually """
        pass

    def _build_filters(self, config: dict):
        """ TODO: need to look into them individually """
        pass

    def _get_node(self, path: str):
        """Get group or array at path."""
        if path == "/":
            return self._root

        try:
            return self._root[path]
        except KeyError:
            raise KeyError(f"Path not found: '{path}'")

    @staticmethod
    def _ensure_numpy(data):
        """
        Convert Dask arrays to NumPy. Pass through NumPy arrays unchanged.

        This function is useful when working with both NumPy and Dask arrays. It
        allows you to write code that works with both types of arrays.

        Parameters:
            data: np.ndarray, da.Array, or xr.DataArray

        Returns:
            np.ndarray
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


