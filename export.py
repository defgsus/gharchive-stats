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
        choices=["all", "type", "user"],
        help="Command",
    )
    parser.add_argument(
        "-o", "--output", type=str, default="output",
        help="Output file",
    )
    parser.add_argument(
        "--raw", type=str, nargs="?", default="raw",
        help="Path to raw data",
    )
    parser.add_argument(
        "--year", type=int, nargs="?", default=datetime.datetime.now().year,
        help="Year to process",
    )
    parser.add_argument(
        "--freq", type=str, default="1d",
        choices=DateBucketExporter.FREQUENCIES,
        help="Date bucket frequency",
    )

    return parser.parse_args()


def add_extension(filename: str, ext: str) -> str:
    if not filename.lower().endswith("." + ext.lower()):
        filename += "." + ext
    return filename


def render_csv(filename: str, archive: GHArchive):
    filename = add_extension(filename, "csv")
    RowExporter(archive).render_csv(filename)


def main(args):
    archive = GHArchive(
        raw_path=args.raw,
        year=args.year,
    )

    if args.command == "all":
        render_csv(args.output, archive)
        #print(archive.raw_filenames(args.year))

    elif args.command == "type":
        filename = add_extension(args.output, "csv")
        TypeExporter(archive, frequency=args.freq).render_csv(filename, tqdm={})

    elif args.command == "user":
        filename = add_extension(args.output, "ndjson")
        UserExporter(archive, frequency=args.freq).render_ndjson(filename, tqdm={})


if __name__ == "__main__":
    main(parse_args())

