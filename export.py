import argparse
from pathlib import Path
import datetime

from src.gharchive import GHArchive
from src.exporters import *


PATH = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", type=str,
        choices=["export"],
        help="Command",
    )
    parser.add_argument(
        "-o", "--output", type=str, default=".",
        help="Output path",
    )
    parser.add_argument(
        "--raw", type=str, nargs="?", default="raw",
        help="Path to raw data",
    )
    parser.add_argument(
        "--year", type=str, nargs="?", default="*",
        help="Year to process, can be a wildcard",
    )
    parser.add_argument(
        "-e", "--export", type=str, nargs="*",
        help=f"Things to export: {{ID}}/{{freq}}/{{format}}"
             f", IDs: {sorted(ExporterBase.exporters)}"
             f", freqs: {DateBucketExporter.FREQUENCIES}",
    )

    return parser.parse_args()


def add_extension(filename: str, ext: str) -> str:
    if not filename.lower().endswith("." + ext.lower()):
        filename += "." + ext
    return filename


def main(args):
    archive = GHArchive(
        raw_path=args.raw,
        year=args.year,
    )

    if args.command == "export":
        if not args.export:
            print("Need to specify at least one exporter (-e/--export)")
            exit(1)

        exporters = []
        for e in args.export:
            name, freq, format = e.split("/")
            filename = Path(args.output) / f"{name}_{freq}"
            filename = add_extension(str(filename), format)
            exporters.append(ExporterBase.exporters[name](
                filename=filename,
                format=format,
                frequency=freq,
            ))
        export(
            iterable=archive.iter_events(),
            exporters=exporters,
            tqdm={},
        )


if __name__ == "__main__":
    main(parse_args())

