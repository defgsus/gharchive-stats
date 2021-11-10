import csv
import json
import datetime
from pathlib import Path
from typing import Generator, Union, Optional

from tqdm import tqdm as tqdm_iter

from .gharchive import GHArchive


class RowExporter:

    def __init__(self, archive: GHArchive):
        self.archive = archive

    def iter_rows(self, tqdm: Optional[dict] = None) -> Generator[dict, None, None]:
        previous_event = None

        iterable = self.archive.iter_events()
        if tqdm is not None:
            iterable = tqdm_iter(iterable, **tqdm)

        for event in iterable:

            if previous_event:
                for row in self.yield_rows(previous_event, False):
                    yield row
            previous_event = event

        if previous_event:
            for row in self.yield_rows(previous_event, True):
                yield row

    def yield_rows(self, event: dict, final: bool) -> Generator[dict, None, None]:
        """
        Overload to yield more or less rows per passed event.

        :param event: original event from raw data.
        :param final: bool, True if this is this the final entry
        """
        yield {"id": event["id"], "date": event["created_at"]}

    def render_csv(self, filename: Union[str, Path], tqdm: Optional[dict] = None):
        writer = None

        with open(str(filename), "wt") as fp:
            for row in self.iter_rows(tqdm=tqdm):

                if writer is None:
                    writer = csv.DictWriter(fp, fieldnames=list(row.keys()))
                    writer.writeheader()

                writer.writerow(row)

    def render_ndjson(self, filename: Union[str, Path], tqdm: Optional[dict] = None):
        with open(str(filename), "wt") as fp:
            for row in self.iter_rows(tqdm=tqdm):
                fp.write(json.dumps(row, cls=JsonEncoder) + "\n")


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        return super().default(o)


class DateBucketExporter(RowExporter):

    FREQUENCIES = ("d", "h", "m", "10m")

    def __init__(self, archive: GHArchive, frequency: str):
        assert frequency in self.FREQUENCIES
        super().__init__(archive)
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

    def yield_rows(self, event: dict, final: bool) -> Generator[dict, None, None]:
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
                yield self.bucket_to_row(k, self._buckets[k])
                self._yielded_buckets.add(k)
            self._buckets.clear()
            return

        if len(self._buckets) < self._bucket_stash_size_threshold:
            return

        for bd in sorted(self._buckets)[:-self._bucket_stash_size_threshold]:
            yield self.bucket_to_row(bd, self._buckets[bd])
            self._yielded_buckets.add(bd)
            del self._buckets[bd]
            print(f" @ {bucket_date} YIELDED {bd}, {len(self._buckets)} buckets in stash")
