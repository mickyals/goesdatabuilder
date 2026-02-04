from ..zarrstore import ZarrDatasetBuilder
from pathlib import Path


class GOESZarrDataset(ZarrDatasetBuilder):
    def __init__(self, config_path: str | Path):
        super().__init__(config_path)
        pass