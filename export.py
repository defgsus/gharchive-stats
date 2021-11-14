import argparse
from pathlib import Path
import datetime

from src.gharchive import GHArchive
from src.exporters import *
from src.file_iter import iter_lines


PATH = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", type=str,
        choices=["export", "elastic"],
        help="Command",
    )
    parser.add_argument(
        "-i", "--input", type=str, default=".",
        help="A generated file which should be processed further",
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
             f", ID: {sorted(ExporterBase.exporters)}"
             f", freq: {DateBucketExporter.FREQUENCIES}"
             f", format: {list(ExporterBase.FORMATS) + [f + '.gz' for f in ExporterBase.FORMATS]}",
    )
    parser.add_argument(
        "--skip", type=int, default=0,
        help="Number of entries to skip when exporting",
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

    if args.command == "elastic":
        from src.elastic import export_elastic
        if not args.input:
            print("Need to specify an input file (-i/--input)")
            exit(1)
        export_elastic(filename=args.input, skip=args.skip)

    elif args.command == "export":
        if not args.export:
            print("Need to specify at least one exporter (-e/--export)")
            exit(1)

        exporters = []
        for e in args.export:
            name, freq, format = e.split("/")
            filename = Path(args.output) / f"{name}_{freq}"
            filename = add_extension(str(filename), format.split(".")[0])
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

