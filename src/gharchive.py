from pathlib import Path
import glob
import gzip
import json
import os
from typing import Union, Generator, List, Optional


class GHArchive:

    def __init__(
            self,
            raw_path: Union[str, Path],
            year: str = "*",
            month: Optional[int] = None,
            day: Optional[int] = None,
    ):
        self.raw_path = Path(raw_path)
        self.year = year
        self.month = month
        self.day = day

    def raw_filenames(
            self,
    ) -> List[str]:
        query = f"{self.year}"
        if self.month is not None:
            query += f"-{self.month:02d}"
        if self.day is not None:
            query += f"-{self.day:02d}"

        query = f"{query}*.json.gz"
        return sorted(glob.glob(str(self.raw_path / str(self.year) / query)), key=self._sort_key)

    def iter_events(
            self,
    ) -> Generator[dict, None, None]:
        id_set = set()
        for fn in self.raw_filenames():
            with gzip.open(fn, "rt") as zf:
                for line in zf.readlines():
                    event = json.loads(line)

                    if event["id"] not in id_set:
                        yield event
                        id_set.add(event["id"])

                        # remove the oldest IDs
                        if len(id_set) >= 1_000_000:
                            for id in sorted(id_set)[:500_000]:
                                id_set.remove(id)

    def _sort_key(self, k: str):
        "YYYY-mm-dd-h"
        idx = k.rfind(os.sep) + 1
        if k[idx+12] == ".":
            return k[:idx+11] + "0" + k[idx+11:]
        return k
