import dataclasses
import functools
import importlib.resources
import inspect
import os
from collections.abc import Callable, Iterable
from copy import deepcopy
from functools import cache
from os import PathLike
from types import FunctionType
from typing import Any

import jsonschema.exceptions
import jsonschema.validators
import yaml

JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None


class ConfigError(Exception):
    """Configuration validation error."""

    ...


class JSONLoader(yaml.SafeLoader):
    """Yaml loader that ensures that object keys are always strings."""

    def construct_mapping(self, *args, **kwargs) -> dict[str, JSON]:
        """Return a mapping that is guaranteed to contain strings as keys."""
        mapping = super().construct_mapping(*args, **kwargs)
        for key in list(mapping.keys()):
            if not isinstance(key, str):
                mapping[str(key)] = mapping.pop(key)
        return mapping


def load_resource_file(file_name: str | PathLike) -> JSON:
    """Load a file from the configs resources."""
    with importlib.resources.open_text("goesdatabuilder.configs", file_name) as f:
        return yaml.load(f, Loader=JSONLoader)


class Config:
    """
    Configuration object used to store configuration data used by objects that inherit from ConfigMixin.

    The default configuration values are determined by the file at `configs/default.yaml`. These can be
    overridden for every new instance of this class by calling the set_defaults class method or for a
    single instance when that instance is initialized.

    Overrides can either be paths to JSON or Yaml files (passed as the configs_path argument) or a
    dictionary (passed as the config_dict) argument. All configuration files passed in this way will
    be merged. Values that are not merged (arrays and non-scalar JSON types) will replace values from
    configuration sources with lower precedence.

    The environment variables in the _ENV_VAR_OVERRIDES attribute can be used to override specific values
    if the environment variable is set.

    The precedence order is:

    - environment variables
    - config_dict
    - config_paths (from right to left)
        - e.g. in `Config("file1.json", "file2.yaml", "file3.json")` file3.json will have the highest precedence
    - the default configuration

    Configuration values will be validated against the json schema defined in the file at
    `configs/config.schema.json` unless initialized with the validate=False agument set.

    For example overriding the defaults on instance creation:

    >>> Config()["data_access"]["chunk_size"]
    "auto"
    >>> Config(config_dict={"data_access": {"chunk_size": 10}})["data_access"]["chunk_size"]
    10
    >>> import json
    >>> with open("file.json", "w") as f:
            json.dump({"data_access": {"chunk_size": 20}}, f)
    >>> Config("file.json")["data_access"]["chunk_size"]
    20
    >>> Config(config_dict={"data_access": None})  # raises ConfigError because this is invalid according to the schema
    >>> Config(config_dict={"data_access": None}, validate=False)  # doesn't raise an error because it was not validated

    For example overriding the defaults for every new instance:

    >>> Config.set_defaults(config_dict={"data_access": {"chunk_size": 10}})
    >>> Config()["data_access"]["chunk_size"]
    10
    >>> Config.set_defaults("file.json")
    >>> Config()["data_access"]["chunk_size"]
    20

    For example overriding defaults with environment variables:

    >>> import os
    >>> os.environ["GOES_FILE_DIR"] = "file_dir/with/goes/in/it"
    >>> Config()["data_access"]["file_dir"]
    "file_dir/with/goes/in/it"
    """

    _ENV_VAR_OVERRIDES: dict[str, tuple[str]] = {
        "GOES_FILE_DIR": ("data_access", "file_dir"),
        "GOES_WEIGHTS_DIR": ("regridding", "weights_dir"),
        "GOES_OUTPUT_DIR": ("pipeline", "output_path"),
        "GOES_STORE_DIR": ("store", "path"),
    }
    _base_config = load_resource_file("default.yaml")

    def __init__(
        self, *config_paths: Iterable[str | PathLike], config_dict: dict | None = None, validate: bool = True
    ) -> None:
        self._config = deepcopy(self._base_config)
        for config_path in config_paths:
            with open(config_path) as f:
                self._config = self._merge_configs(self._config, yaml.load(f, Loader=JSONLoader))
        if config_dict:
            self._config = self._merge_configs(self._config, config_dict)
        self._set_env_var_overrides()
        self._set_dependant_defaults()
        if validate:
            self._validate()

    @staticmethod
    def _convert_pathlike(obj: Any) -> Any:
        if isinstance(obj, PathLike):
            obj = str(obj)
        return obj

    @classmethod
    def _merge_configs(cls, config_1: JSON, config_2: JSON) -> JSON:
        """
        Deep merge the dictionaries in two configuration JSONs.

        Replaces values in config_1 with values in config_2. Extra keys in
        config_2 are added.

        Note: Lists are replaced, not merged.
        Note: PathLike objects are converted to strings so they can be validated properly
        """
        if isinstance(config_1, dict) and isinstance(config_2, dict):
            shared_keys = config_1.keys() & config_2.keys()
            return {key: cls._merge_configs(config_1[key], config_2[key]) for key in shared_keys} | {
                key: cls._convert_pathlike(val) for key, val in (config_1 | config_2).items() if key not in shared_keys
            }
        else:
            return cls._convert_pathlike(config_2)

    def _set_env_var_overrides(self) -> None:
        for env_var, config_path in self._ENV_VAR_OVERRIDES.items():
            if env_var in os.environ:
                conf = self._config
                for path in config_path[:-1]:
                    conf = conf[path]
                conf[config_path[-1]] = os.getenv(env_var)

    def _set_dependant_defaults(self) -> None:
        """Set default config values that depend on other config values."""
        output_path = self._config["pipeline"]["output_path"]
        if output_path is None:
            return
        conf = self._config["pipeline"]["catalog"]
        if conf["output_dir"] is None:
            conf["output_dir"] = os.path.join(output_path, "catalog")
        conf = self._config["pipeline"]["checkpoints"]
        if conf["directory"] is None:
            conf["directory"] = os.path.join(output_path, "checkpoints")
        conf = self._config["pipeline"]["logging"]
        if conf["log_file"] is None:
            conf["log_file"] = os.path.join(output_path, "logs", "pipeline.log")

    def _validate(self) -> None:
        schema = _cached_schema()
        validator = jsonschema.validators.validator_for(schema)(schema)
        errors = validator.iter_errors(self._config, _cached_schema())
        best_error = jsonschema.exceptions.best_match(errors)
        if best_error:
            raise ConfigError(f"Invalid Configuration: {best_error}") from best_error

    def __getitem__(self, key: str) -> Any:
        """Get an item from the internal config dictionary."""
        return self._config[key]

    def __getattr__(self, name: str) -> Any:
        """
        Fallback to getting attributes from the internal config dictionary.

        This allows access to methods such as Config.items and Config.keys, etc.
        """
        return getattr(self._config, name)

    @classmethod
    def set_defaults(cls, *config_paths: Iterable[str | PathLike], config_dict: dict | None = None) -> None:
        """
        Update the default base configuration used by this class.

        By default it is the configuration found in the `configs/default.yaml` file.
        Users should not call this directly and should use the `set_config` function instead.
        """
        cls._base_config = load_resource_file("default.yaml")
        for config_path in config_paths:
            with open(config_path) as f:
                cls._base_config = cls._merge_configs(cls._base_config, yaml.load(f, Loader=JSONLoader))
        if config_dict:
            cls._base_config = cls._merge_configs(cls._base_config, config_dict)


@cache
def _cached_config(
    *config_paths: Iterable[str | PathLike], config_dict: dict | None = None, validate: bool = True
) -> Config:
    return Config(*config_paths, config_dict=config_dict, validate=validate)


@cache
def _cached_schema() -> JSON:
    return load_resource_file("config.schema.json")


def get_config() -> Config:
    """Get the default configuration used by all instances in this library that inherit from ConfigMixin."""
    return _cached_config(validate=False)


def set_config(
    *config_paths: Iterable[str | PathLike], config_dict: dict | None = None, validate: bool = False
) -> None:
    """Set the default configuration used by all instances in this library that inherit from ConfigMixin."""
    Config.set_defaults(*config_paths, config_dict)
    if validate:
        Config(validate=True)


class ConfigMixin:
    """
    Mixin for classes that need to be configurable based on the settings as defined by the Config class.

    Inheriting from this class will do the following:

    - create an instance attribute _config that contains an instance of the Config class.
        - _config is created before __init__ so it can be used in initializing the class
        - arguments to __init__ will be treated as configuration overrides allowing for customization
          at initialization time.
    - allow all methods to use ConfigDefault instances as parameter defaults
    - create class method `from_configs` that allows for the creation of the instance directly from
      configuration files.

    Classes that inherit from this class can set the _config_subsection class variable that points to a
    subsection of the configuration dictionary. This has two effects:

    - arguments to __init__ will update the configuration at the level indicated by the _config_subsection
      attribute
    - the path argument to ConfigDefault for this class will behave as if the _config_subsection is prepended
      to it (unless from_subsection=False)

    For example:

    >>> class Example(ConfigMixin):
            def __init__(self, data_access = {}):
                ...
            def get_chunk(self):
                return self._config["data_access"]["chunk_size"]
    >>> Example().get_chunk()
    "auto" # this is the default configuration value
    >>> Example(data_access={"chunk_size": 10}).get_chunk()
    10
    >>> Example(data_access={"chunk_size": {"invalid chunk"}})  # raises a ConfigError

    >>> class Example2(ConfigMixin):
        _config_subsection = ("data_access",)
        def __init__(self, chunk_size = ConfigDefault("chunk_size")):
            ...
        def get_chunk(self):
            return self._config["data_access"]["chunk_size"]
    >>> Example().get_chunk():
    "auto" # this is the default configuration value
    >>> Example(chunk_size=10).get_chunk()
    10
    >>> Example(chunk_size={"invalid chunk"})  # raises a ConfigError
    """

    _config: Config = get_config()
    _config_subsection: Iterable[str] | None = None

    def _set_config(self, **kwargs) -> None:
        config_dict = sub_dict = {}
        if self._config_subsection:
            for path in self._config_subsection:
                sub_dict[path] = sub_dict = {}
        sub_dict.update(kwargs)
        self._config = Config(config_dict=config_dict)

    def __init_subclass__(cls, *args, **kwargs) -> None:
        """Wrap each method in the subclass with the resolve_config_defaults function."""
        super().__init_subclass__(*args, **kwargs)
        for attr, value in cls.__dict__.items():
            if isinstance(value, FunctionType):
                setattr(cls, attr, resolve_config_defaults(value))

    @classmethod
    def from_configs(
        cls, *config_paths: Iterable[str | PathLike], config_dict: dict | None = None, check_cache: bool = True
    ) -> "ConfigMixin":
        """Initialize this class based on configuration settings."""
        if check_cache:
            config = _cached_config(*config_paths, config_dict=config_dict)
        else:
            config = Config(*config_paths, config_dict=config_dict)
        if cls._config_subsection:
            for path in cls._config_subsection:
                config = config[path]
        field_names = {field.name for field in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in config.items() if k in field_names})


def resolve_config_defaults(func: Callable) -> Callable:
    """Allow wrapped function to use ConfigDefault instance as default parameter values."""

    @functools.wraps(func)
    def _(*args, **kwargs) -> Any:
        bound_args = inspect.signature(func).bind(*args, **kwargs)
        bound_args.apply_defaults()
        arguments = bound_args.arguments
        self = arguments.get("self", arguments.get("cls", ConfigMixin))
        resolved_kwargs = {k: (v.resolve(self) if isinstance(v, ConfigDefault) else v) for k, v in arguments.items()}
        if func.__name__ == "__init__":
            ConfigMixin._set_config(**resolved_kwargs)
        return func(**resolved_kwargs)

    return _


class ConfigDefault:
    """
    Used as a default parameter value to access a value specified in a Config instance.

    Automatically resolves the configuration value if used as a parameter to a method
    of a class that inherits from ConfigMixin.

    For example:

    >>> class Example(ConfigMixin):
            _config_subsection = ("data_access",)
            def get_chunk(self, chunk = ConfigDefault("chunk_size")):
                return chunk
    >>> Example().get_chunk()
    "auto" # this is the default configuration value
    >>> Example().get_chunk(220)
    220

    This will use the configuration defined for the class in case the class overrides the default
    configuration during initialization:

    >>> class Example2(Example):
            def __init__(self, chunk_size):
                ...
    >>> x = Example2(chunk_size=100)
    >>> x.get_chunk()
    (100, 100)
    >>> x.get_chunk(220)
    220
    """

    def __init__(self, *path, from_subsection: bool = True) -> None:
        self._path = path
        self._from_subsection = from_subsection

    def resolve(self, configurable_inst: ConfigMixin) -> Any:
        """Resolve the value at the path of the configuration settings of the configurable_inst."""
        try:
            config = configurable_inst._config
        except AttributeError as e:
            raise AttributeError("Unable to resolve Config instance to handle default values.") from e
        path = self._path
        if self._from_subsection and configurable_inst._config_subsection:
            path = configurable_inst._config_subsection + path
        for p in path:
            config = config[p]
        return config
