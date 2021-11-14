import os
from pathlib import Path
from argparse import ArgumentParser
from typing import Union, Optional, Tuple, List, Dict, Type

import pandas as pd


class ProcBase:

    CACHE_PATH = Path(__file__).resolve().parent.parent.parent / "cache"

    NAME = None

    PROCESSORS: Dict[str, Type["ProcBase"]] = dict()

    def __init_subclass__(cls, **kwargs):
        if cls.NAME:
            ProcBase.PROCESSORS[cls.NAME] = cls

    @classmethod
    def add_arguments(cls, parser: ArgumentParser):
        pass

    def run(self):
        raise NotImplementedError

    def cache_path(self, filename: Optional[Union[str, Path]] = None) -> Path:
        path = self.CACHE_PATH / self.NAME
        if filename is not None:
            sub_path = Path(filename).parent
            if sub_path:
                path = path / sub_path
        return path

    def cache_filename(self, filename: Union[str, Path]) -> Path:
        return self.cache_path(filename) / filename

    def cache_exists(self, filename: Union[str, Path]) -> bool:
        return self.cache_filename(filename).exists()

    def cache_makedirs(self, filename: Union[str, Path]):
        os.makedirs(str(self.cache_path(filename)), exist_ok=True)

    def cached_csv(self, filename: Union[str, Path]) -> Optional[pd.DataFrame]:
        fn = self.cache_filename(filename)
        if fn.exists():
            return pd.read_csv(fn)
