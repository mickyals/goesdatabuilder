"""GOES Data Builder.

A comprehensive Python package for processing GOES ABI L2+ data from raw NetCDF files
to CF-compliant Zarr stores with full metadata management and regridding capabilities.
"""

from .data.goes.multicloud import GOESMultiCloudObservation
from .data.goes.multicloudcatalog import GOESMetadataCatalog
from .pipelines.goesmulticloudpipeline import GOESPipelineOrchestrator
from .regrid.geostationary import GeostationaryRegridder
from .store.datasets.goesmulticloudzarr import GOESZarrStore
from .store.zarrstore import ZarrStoreBuilder
from .utils.config import ConfigError, get_config, set_config

__version__ = "1.0.0"
__author__ = "GOES Data Builder Team"
__email__ = "contact@example.com"
__license__ = "MIT"
__description__ = "A comprehensive Python package for processing GOES ABI L2+ data from raw NetCDF files to CF-compliant Zarr stores with full metadata management and regridding capabilities."
__url__ = "https://github.com/mickyals/goesdatabuilder"

__all__ = [
    # Core classes
    "GOESMultiCloudObservation",
    "GOESMetadataCatalog",
    "GeostationaryRegridder",
    "ZarrStoreBuilder",
    "GOESZarrStore",
    "GOESPipelineOrchestrator",
    "set_config",
    "get_config",
    # Exceptions
    "ConfigError",
    # Package metadata
    "__version__",
    "__author__",
    "__email__",
    "__license__",
    "__description__",
    "__url__",
]
