import json
import gzip
from pathlib import Path
from typing import Union, Generator, IO


def iter_ndjson(file: Union[str, Path, IO]) -> Generator[dict, None, None]:
    if isinstance(file, (str, Path)):
        filename = str(file)

        if filename.lower().endswith(".gz"):
            with gzip.open(filename, "rt") as fp:
                yield from iter_ndjson(fp)

        else:
            with open(file, "rt") as fp:
                yield from iter_ndjson(fp)

    else:
        for line in file.readlines():
            yield json.loads(line)
