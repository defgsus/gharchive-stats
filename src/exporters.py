from .exporter_base import *


class TypeExporter(DateBucketExporter):

    def new_bucket(self, date: datetime.datetime) -> dict:
        return {"all": 0}

    def add_to_bucket(self, date: datetime.datetime, bucket: dict, event: dict):
        key = event["type"]
        bucket[key] = bucket.get(key, 0) + 1
        bucket["all"] += 1


class UserExporter(DateBucketExporter):

    def new_bucket(self, date: datetime.datetime) -> dict:
        return {"all": 0}

    def add_to_bucket(self, date: datetime.datetime, bucket: dict, event: dict):
        user = event["actor"]["login"]
        repo = event["repo"]["name"]
        type = event["type"]
        key = f"{type}/{user}/{repo}"
        #key = user
        bucket[key] = bucket.get(key, 0) + 1
        bucket["all"] += 1
