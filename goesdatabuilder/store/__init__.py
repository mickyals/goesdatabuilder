"""Storage Module.

This module provides classes for building and managing Zarr stores with
CF-compliant metadata, compression, and data organization capabilities.
"""

from .datasets.goesmulticloudzarr import GOESZarrStore
from .zarrstore import ConfigError, ZarrStoreBuilder

__all__ = [
    "ZarrStoreBuilder",
    "GOESZarrStore",
    "ConfigError",
]
