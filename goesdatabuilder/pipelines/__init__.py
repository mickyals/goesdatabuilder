"""Pipeline Orchestration Module

This module provides high-level orchestration classes for managing end-to-end
GOES data processing pipelines from raw files to final Zarr stores.
"""

from .goesmulticloudpipeline import GOESPipelineOrchestrator

__all__ = [
    'GOESPipelineOrchestrator',
]