from ..zarrstore import ZarrStoreBuilder
from pathlib import Path


class GOESMultiCloudDataset(ZarrStoreBuilder):
    def __init__(self, config_path: str | Path):
        super().__init__(config_path)
        pass

    ############################################################################################
    # CLASS VARIABLES / BAND METADATA
    ############################################################################################
    _CMI_UNIVERSAL_BAND_METADATA_FIELDS = {}

    _DQF_UNIVERSAL_BAND_METADATA_FIELDS = {}


    # Class constants
    PLATFORMS = ("GOES-East", "GOES-West")
    BANDS = tuple(f"CMI_C{i:02d}" for i in range(1, 17))