#!env/bin/python

import argparse
import sys
from pathlib import Path
import datetime

from src.proc import ProcBase


PATH = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()

    args = sys.argv
    if len(args) < 2 or args[1] not in ProcBase.PROCESSORS:

        parser.add_argument(
            "processor", type=str,
            choices=sorted(ProcBase.PROCESSORS.keys()),
            help="Processor from src/proc/",
        )

        parser.parse_args()
        exit(0)

    proc_class = ProcBase.PROCESSORS[args[1]]
    proc_class.add_arguments(parser)

    args = parser.parse_args(args[2:])

    proc = proc_class(**vars(args))
    proc.run()


if __name__ == "__main__":
    main()

