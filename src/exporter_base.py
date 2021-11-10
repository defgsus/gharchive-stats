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

    BUCKETS_STASH_SIZE_THRESHOLD = 2
    FREQUENCIES = ("d", "h", "5min")

    def __init__(self, archive: GHArchive, frequency: str):
        assert frequency in self.FREQUENCIES
        super().__init__(archive)
        self.frequency = frequency
        self._buckets = dict()
        self._yielded_buckets = set()
        self._event_count = 0

        if self.frequency == "h":
            self._max_delta = datetime.timedelta(hours=2)
        elif self.frequency == "d":
            self._max_delta = datetime.timedelta(days=2)
        elif self.frequency == "5min":
            self._max_delta = datetime.timedelta(hours=1)

    def bucket_date(self, date: str) -> datetime.datetime:
        #return pd.Timestamp(date).round(self.frequency).to_pydatetime()
        if self.frequency == "h":
            return datetime.datetime.strptime(date[:13], "%Y-%m-%dT%H")
        elif self.frequency == "d":
            return datetime.datetime.strptime(date[:10], "%Y-%m-%d")
        elif self.frequency == "5min":
            date = datetime.datetime.strptime(date[:16], "%Y-%m-%dT%H:%M")
            return date.replace(minute=(date.minute // 5) * 5)

    def new_bucket(self, date: datetime.datetime) -> dict:
        return {}

    def add_to_bucket(self, date: datetime.datetime, bucket: dict, event: dict):
        key = event["type"]
        bucket[key] = bucket.get(key, 0) + 1

    def bucket_to_row(self, date: datetime.datetime, bucket: dict) -> dict:
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

        if len(self._buckets) < self.BUCKETS_STASH_SIZE_THRESHOLD:
            return

        for bd in sorted(self._buckets)[:-self.BUCKETS_STASH_SIZE_THRESHOLD]:
            yield self.bucket_to_row(bd, self._buckets[bd])
            self._yielded_buckets.add(bd)
            del self._buckets[bd]
            print(f" @ {bucket_date} YIELDED {bd}, {len(self._buckets)} buckets in stash")
