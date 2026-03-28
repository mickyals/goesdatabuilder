from collections.abc import Iterable
import dataclasses
from functools import cache
import functools
import inspect
from os import PathLike
import os
from types import FunctionType
from typing import Any

import jsonschema
import yaml
import importlib.resources


JSON = dict[str, 'JSON'] | list['JSON'] | str | int | float | bool | None

class ConfigError(Exception):
    """Configuration validation error"""
    ...

class JSONLoader(yaml.SafeLoader):
    def construct_mapping(self, *args, **kwargs):
        mapping = super().construct_mapping(*args, **kwargs)
        for key in list(mapping.keys()):
            if not isinstance(key, str):
                mapping[str(key)] = mapping.pop(key)
        return mapping

class Config:
    _ENV_VAR_OVERRIDES: dict[str, tuple[str]] = {
        "GOES_FILE_DIR": ("data_access", "file_dir"),
        "GOES_WEIGHTS_DIR": ("regridding", "weights_dir"),
        "GOES_OUTPUT_DIR": ("pipeline", "output_path"),
        "GOES_STORE_DIR": ("store", "path")
    }

    def __init__(self, *config_paths: Iterable[str | PathLike], config_dict: dict | None = None, validate: bool = True) -> None:
        self._config = self.load_resource_file("default.yaml")
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

    def _merge_configs(self, config_1: JSON, config_2: JSON) -> JSON:
        """
        Deep merge the dictionaries in two configuration JSONs. 
        
        Replaces values in config_1 with values in config_2. Extra keys in
        config_2 are added.

        Note: Lists are replaced, not merged.
        Note: PathLike objects are converted to strings so they can be validated properly
        """
        if isinstance(config_1, dict) and isinstance(config_2, dict):
            shared_keys = config_1.keys() & config_2.keys()
            return {
                key: self._merge_configs(config_1[key], config_2[key])
                for key in shared_keys
            } | {
                key: self._convert_pathlike(val) for key, val in (config_1 | config_2).items()
                if key not in shared_keys
            }
        else:
            return self._convert_pathlike(config_2)

    @staticmethod
    def load_resource_file(file_name):
        """
        Load a file from the configs resources.
        """
        with importlib.resources.open_text("goesdatabuilder.configs", file_name) as f:
            return yaml.load(f, Loader=JSONLoader)
    
    def _set_env_var_overrides(self):
        for env_var, config_path in self._ENV_VAR_OVERRIDES.items():
            if env_var in os.environ:
                conf = self._config
                for path in config_path[:-1]:
                    conf = conf[path]
                conf[config_path[-1]] = os.getenv(env_var)

    def _set_dependant_defaults(self):
        """
        Set default config values that depend on other config values.
        """
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

    def _validate(self):
        jsonschema.validate(self._config, self.load_resource_file("config.schema.json"))

    def __getitem__(self, key):
        return self._config[key]

    def __getattr__(self, name):
        return getattr(self._config, name)


@cache
def _cached_config(*config_paths: Iterable[str | PathLike], config_dict: dict | None = None, validate: bool = True) -> Config:
    return Config(*config_paths, config_dict=config_dict, validate=validate)


@cache
def _cached_schema():
    Config.load_resource_file("config.schema.json")


def default_config():
    return  _cached_config(validate=False)


class ConfigMixin:
    _config: Config = default_config()
    _config_subsection: Iterable[str] | None = None

    def _set_config(self, **kwargs):
        config_dict = sub_dict = {}
        if self._config_subsection:
            for path in self._config_subsection:
                sub_dict[path] = sub_dict = {}
        sub_dict.update(kwargs)
        self._config = Config(config_dict=config_dict)

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        for attr, value in cls.__dict__.items():
            if isinstance(value, FunctionType):
                setattr(cls, attr, resolve_config_defaults(value))


    def validate(self, config: dict[str, Any]) -> None:
        schema = _cached_schema()
        if self._config_subsection:
            for path in self._config_subsection:
                schema = schema["properties"][path]
        jsonschema.validate({k: v for k,v in config.items() if k in schema["properties"]}, schema)

    @classmethod
    def from_configs(cls, *config_paths: Iterable[str | PathLike], config_dict: dict | None = None, check_cache: bool = True):
        """
        Initialize this class based on configuration settings
        """
        if check_cache:
            config = _cached_config(*config_paths, config_dict=config_dict)
        else:
            config  = Config(*config_paths, config_dict=config_dict)
        if cls._config_subsection:
            for path in cls._config_subsection:
                config = config[path]
        field_names = {field.name for field in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in config.items() if k in field_names})


def resolve_config_defaults(func):
    @functools.wraps(func)
    def _(*args, **kwargs):
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
    def __init__(self, *path, from_subsection: bool = True):
        self._path = path
        self._from_subsection = from_subsection

    def resolve(self, configurable_inst: ConfigMixin):
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
