"""GOES Data Builder

A comprehensive Python package for processing GOES ABI L2+ data from raw NetCDF files 
to CF-compliant Zarr stores with full metadata management and regridding capabilities.
"""

from .data.goes.multicloud import GOESMultiCloudObservation, ConfigError
from .data.goes.multicloudcatalog import GOESMetadataCatalog
from .regrid.geostationary import GeostationaryRegridder
from .store.zarrstore import ZarrStoreBuilder, ConfigError as ZarrConfigError
from .store.datasets.goesmulticloudzarr import GOESZarrStore
from .pipelines.goesmulticloudpipeline import GOESPipelineOrchestrator

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
    
    # Exceptions
    "ConfigError",
    "ZarrConfigError",
    
    # Package metadata
    "__version__",
    "__author__",
    "__email__",
    "__license__",
    "__description__",
    "__url__",
]