import json
import gzip
import glob
import csv
from pathlib import Path
from typing import Union, Generator, IO


def iter_ndjson(file: Union[str, Path, IO], raise_error: bool = False) -> Generator[dict, None, None]:
    for line in iter_lines(file):
        try:
            yield json.loads(line)
        except json.JSONDecodeError as e:
            if raise_error:
                raise
            print(f"\n\nJSON ERROR '{e}' for line '{line}'\n")


def iter_csv(file: Union[str, Path, IO]) -> Generator[dict, None, None]:
    iterable = iter_lines(file)
    reader = csv.DictReader(iterable)
    yield from reader


def iter_lines(file: Union[str, Path, IO]) -> Generator[dict, None, None]:
    if isinstance(file, (str, Path)):
        filename = str(file)

        if "*" in filename or "?" in filename:
            for fn in sorted(glob.glob(filename)):
                yield from iter_lines(fn)

        else:
            if filename.lower().endswith(".gz"):
                with gzip.open(filename, "rt") as fp:
                    yield from iter_lines(fp)

            else:
                with open(file, "rt") as fp:
                    yield from iter_lines(fp)

    else:
        yield from file.readlines()


def iter_csv_XXX(file: Union[str, Path, IO]) -> Generator[dict, None, None]:
    if isinstance(file, (str, Path)):
        filename = str(file)

        if "*" in filename or "?" in filename:
            for fn in sorted(glob.glob(filename)):
                yield from iter_csv(fn)

        else:
            if filename.lower().endswith(".gz"):
                with gzip.open(filename, "rt") as fp:
                    yield from iter_csv(fp)

            else:
                with open(file, "rt") as fp:
                    yield from iter_csv(fp)

    else:
        reader = csv.DictReader(file)
        yield from reader


def iter_file(file: Union[str, Path]) -> Generator[dict, None, None]:
    filename = str(file).lower()
    if filename.endswith(".csv") or filename.endswith(".csv.gz"):
        yield from iter_csv(file)

    elif filename.endswith(".ndjson") or filename.endswith(".ndjson.gz"):
        yield from iter_ndjson(file)

    else:
        raise ValueError(f"Unsupported file format for '{file}'")
