import json
import gzip
import glob
from pathlib import Path
from typing import Union, Generator, IO


def iter_ndjson(file: Union[str, Path, IO]) -> Generator[dict, None, None]:
    if isinstance(file, (str, Path)):
        filename = str(file)

        if "*" in filename or "?" in filename:
            for fn in sorted(glob.glob(filename)):
                yield from iter_ndjson(fn)

        else:
            if filename.lower().endswith(".gz"):
                with gzip.open(filename, "rt") as fp:
                    yield from iter_ndjson(fp)

            else:
                with open(file, "rt") as fp:
                    yield from iter_ndjson(fp)

    else:
        for line in file.readlines():
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                print(f"\n\nJSON ERROR '{e}' for line '{line}'\n")

