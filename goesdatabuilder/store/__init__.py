"""Storage Module

This module provides classes for building and managing Zarr stores with
CF-compliant metadata, compression, and data organization capabilities.
"""

from .zarrstore import ZarrStoreBuilder, ConfigError
from .datasets.goes import GOESZarrStore

__all__ = [
    'ZarrStoreBuilder',
    'GOESZarrStore',
    'ConfigError',
]