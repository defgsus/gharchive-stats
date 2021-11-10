from .exporter_base import *


def get_event_type(event: dict) -> str:
    t = event["type"]
    if t == "PushEvent":
        t = "PushEvent-%s" % len(event["payload"]["commits"])
    elif event["payload"].get("action"):
        t = "%s-%s" % (t, event["payload"]["action"])

    return t


class TypeExporter(DateBucketExporter):

    NAME = "type"

    def columns(self) -> List[str]:
        # TODO: this is incomplete, e.g. is it called `WatchEvent.stopped`? I don"t know yet
        return [
            "date",
            "all",
            "CommitCommentEvent",
            "CreateEvent",
            "DeleteEvent",
            "ForkEvent",
            "GollumEvent",
            # "IssueCommentEvent",
            "IssueCommentEvent-created",
            # "IssuesEvent",
            "IssuesEvent-closed",
            "IssuesEvent-opened",
            "IssuesEvent-reopened",
            "MemberEvent-added",
            "PublicEvent",
            "PullRequestEvent",
            "PullRequestEvent-closed",
            "PullRequestEvent-opened",
            "PullRequestEvent-reopened",
            # "PullRequestReviewCommentEvent",
            "PullRequestReviewCommentEvent-created",
            "PullRequestReviewEvent-created",
            # "PushEvent",
        ] + [
            f"PushEvent-{i}" for i in range(0, 21)
        ] + [
            # "ReleaseEvent",
            "ReleaseEvent-published",
            # "WatchEvent",
            "WatchEvent-started",
        ]

    def new_bucket(self, date: str) -> dict:
        return {"all": 0}

    def add_to_bucket(self, date: str, bucket: dict, event: dict):
        key = get_event_type(event)
        bucket[key] = bucket.get(key, 0) + 1
        bucket["all"] += 1


class UserExporter(DateBucketExporter):

    NAME = "user"

    def new_bucket(self, date: str) -> dict:
        return {"all": 0}

    def add_to_bucket(self, date: str, bucket: dict, event: dict):
        user = event["actor"].get("login", "???")  # there are cases...
        repo = event["repo"]["name"]
        type = get_event_type(event)
        key = f"{type}/{user}/{repo}"
        #key = user
        bucket[key] = bucket.get(key, 0) + 1
        bucket["all"] += 1
