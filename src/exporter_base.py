import os
import csv
import json
import datetime
import warnings
from pathlib import Path
from typing import Generator, Union, Optional, List

from tqdm import tqdm as tqdm_iter


class ExporterBase:

    FORMATS = ("csv", "ndjson")
    NAME = None

    exporters = dict()

    def __init_subclass__(cls, **kwargs):
        if cls.NAME:
            # assert cls.NAME, f"Must define {cls.__name__}.NAME"
            assert cls.NAME not in ExporterBase.exporters, \
                f"{cls.__name__} uses similar name '{cls.NAME}' as {ExporterBase.exporters[cls.NAME].__name__}"
            ExporterBase.exporters[cls.NAME] = cls

    def __init__(self, filename: Union[Path, str], format: str):
        self.filename = str(filename)
        self.format = format
        self._writer = None
        self._fp = None
        self._csv_fieldnames: Optional[List[str]] = None
        self._csv_file_count: int = 0

    def finish(self):
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def columns(self) -> Optional[List[str]]:
        pass

    def digest(self, event: dict, final: bool):
        """
        Overload to process and export event data.

        :param event: original event from raw data.
        :param final: bool, True if this is this the final entry
        """
        self.store_row({
            "date": event["created_at"],
            "id": event["id"],
            "type": event["type"],
        })

    def store_row(self, row: dict):
        if self._fp is None:
            os.makedirs(Path(self.filename).parent, exist_ok=True)
            self._fp = open(self.filename, "wt")

        if self.format == "csv":

            if self._writer is None:
                # create csv with columns as we know it
                self._csv_fieldnames = self.columns() or []
                for key in row.keys():
                    if key not in self._csv_fieldnames:
                        self._csv_fieldnames.append(key)
                self._start_csv_writer()

            else:
                # check if a new column appeared
                missing_keys = set(row.keys()) - set(self._csv_fieldnames)
                if missing_keys:
                    # create a new file
                    self._csv_file_count += 1
                    new_filename = self.filename.split(".")
                    new_filename = ".".join(new_filename[:-1]) + f"-{self._csv_file_count}." + new_filename[-1]
                    warnings.warn(
                        f"Detected new csv columns {missing_keys}, starting new file '{new_filename}"
                    )
                    self._fp.close()

                    # add the new keys to the known ones
                    for key in row:
                        if key not in self._csv_fieldnames:
                            self._csv_fieldnames.append(key)

                    self._fp = open(new_filename, "wt")
                    self._start_csv_writer()

            self._writer.writerow(row)

        elif self.format == "ndjson":
            self._fp.write(json.dumps(row, cls=JsonEncoder) + "\n")

    def _start_csv_writer(self):
        self._writer = csv.DictWriter(self._fp, fieldnames=self._csv_fieldnames)
        self._writer.writeheader()


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        return super().default(o)


class DateBucketExporter(ExporterBase):

    FREQUENCIES = ("d", "h", "m", "10m")

    def __init__(self, filename: Union[Path, str], format: str, frequency: str):
        assert frequency in self.FREQUENCIES
        super().__init__(filename, format)
        self.frequency = frequency
        self._buckets = dict()
        self._yielded_buckets = set()
        self._event_count = 0
        self._bucket_stash_size_threshold = 2
        if self.frequency == "m":
            self._bucket_stash_size_threshold = 120
        elif self.frequency == "10m":
            self._bucket_stash_size_threshold = 12

    def bucket_date(self, date: str) -> str:
        if self.frequency == "d":
            return date[:10] + "T00:00:00Z"
        elif self.frequency == "h":
            return date[:13] + ":00:00Z"
        elif self.frequency == "10m":
            return date[:15] + "0:00Z"
        elif self.frequency == "m":
            return date[:16] + ":00Z"

    def new_bucket(self, date: str) -> dict:
        return {}

    def add_to_bucket(self, date: str, bucket: dict, event: dict):
        key = event["type"]
        bucket[key] = bucket.get(key, 0) + 1

    def bucket_to_row(self, date: str, bucket: dict) -> dict:
        return {
            "date": date,
            **bucket,
        }

    def digest(self, event: dict, final: bool):
        self._event_count += 1
        #if self._event_count % 30000 == 0:
        #    print("    COUNT", self._event_count, "BUCKETSTASH", sorted(self._buckets))

        bucket_date = self.bucket_date(event["created_at"])

        if bucket_date not in self._buckets:
            if bucket_date in self._yielded_buckets:
                raise AssertionError(
                    f"bucket_date {bucket_date} refers to a previously yielded bucket."
                    f"\nCurrent stashed buckets: {sorted(self._buckets)}"
                )
            print(f" start bucket {bucket_date}")
            self._buckets[bucket_date] = self.new_bucket(bucket_date)

        self.add_to_bucket(bucket_date, self._buckets[bucket_date], event)

        if final:
            for k in sorted(self._buckets):
                self.store_row(self.bucket_to_row(k, self._buckets[k]))
                self._yielded_buckets.add(k)
            self._buckets.clear()
            return

        if len(self._buckets) < self._bucket_stash_size_threshold:
            return

        for bd in sorted(self._buckets)[:-self._bucket_stash_size_threshold]:
            self.store_row(self.bucket_to_row(bd, self._buckets[bd]))
            self._yielded_buckets.add(bd)
            del self._buckets[bd]
            print(f" @ {bucket_date} STORED row {bd}, {len(self._buckets)} buckets in stash")

        if len(self._yielded_buckets) >= 2000:
            for b in sorted(self._yielded_buckets)[:1000]:
                self._yielded_buckets.remove(b)


def export(
        iterable: Generator[dict, None, None],
        exporters: List[ExporterBase],
        tqdm: Optional[dict] = None,
):
    from .memory import process_memory

    try:
        previous_event = None

        if tqdm is not None:
            iterable = tqdm_iter(iterable, **tqdm)

        count = 0
        for event in iterable:

            if count % 100000 == 0:
                print(f"\nPROGMEM {process_memory():,} bytes")
            count = count + 1

            if previous_event:
                for e in exporters:
                    e.digest(previous_event, False)

            previous_event = event

        if previous_event:
            for e in exporters:
                e.digest(previous_event, True)

    except:

        for e in exporters:
            e.finish()
        raise
