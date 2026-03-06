"""GOES Dataset Storage Module

This module provides GOES-specific Zarr store implementations with full CF compliance,
ACDD metadata, and specialized handling for GOES ABI bands and quality flags.
"""

from .goesmulticloudzarr import GOESZarrStore

__all__ = [
    'GOESZarrStore',
]