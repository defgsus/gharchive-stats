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


class PushExporter(DateBucketExporter):

    NAME = "push"

    def new_bucket(self, date: str) -> dict:
        return {}

    def add_to_bucket(self, date: str, bucket: dict, event: dict):
        if event["type"] == "PushEvent":
            p = event["payload"]
            user = event["actor"].get("login", "???")
            repo = event["repo"]["name"]
            ref = p["ref"]
            key = (user, repo, ref)

            if key not in bucket:
                bucket[key] = {}
            b = bucket[key]

            b["events"] = b.get("events", 0) + 1
            b["commits"] = b.get("commits", 0) + p["size"]
            b["distinct_commits"] = b.get("distinct_commits", 0) + p["distinct_size"]
            b["message_len"] = b.get("message_len", 0) + sum(len(c["message"]) for c in p["commits"])
            b["authors"] = b.get("authors", 0) + len(set(c["author"]["name"] for c in p["commits"]))

    def bucket_to_rows(self, date: str, bucket: dict) -> Generator[dict, None, None]:
        for user, repo, ref in sorted(bucket):
            yield {
                "date": date,
                "user": user,
                "repo": repo,
                "ref": ref,
                **bucket[(user, repo, ref)],
            }


class CreateExporter(DateBucketExporter):

    NAME = "create"

    def new_bucket(self, date: str) -> dict:
        return {}

    def add_to_bucket(self, date: str, bucket: dict, event: dict):
        if event["type"] == "CreateEvent":
            p = event["payload"]

            user = event["actor"].get("login", "???")
            org = event.get("org", {}).get("login")
            repo = event["repo"]["name"]
            ref = p["ref"]
            ref_type = p["ref_type"]
            branch = p["master_branch"]
            description = p["description"]

            key = tuple(
                k or ""
                for k in (user, org, repo, ref, ref_type, branch, description)
            )

            if key not in bucket:
                bucket[key] = {}
            b = bucket[key]

            b["events"] = b.get("events", 0) + 1

    def bucket_to_rows(self, date: str, bucket: dict) -> Generator[dict, None, None]:

        for key in sorted(bucket):
            yield {
                "date": date,
                "user": key[0],
                "org": key[1],
                "repo": key[2],
                "ref": key[3],
                "ref_type": key[4],
                "master_branch": key[5],
                "description": key[6],
                **bucket[key],
            }


class DeleteExporter(DateBucketExporter):

    NAME = "delete"

    def new_bucket(self, date: str) -> dict:
        return {}

    def add_to_bucket(self, date: str, bucket: dict, event: dict):
        if event["type"] == "DeleteEvent":
            p = event["payload"]

            user = event["actor"].get("login", "???")
            org = event.get("org", {}).get("login", "")
            repo = event["repo"]["name"]
            ref = p["ref"]
            ref_type = p["ref_type"]
            pusher_type = p["pusher_type"]

            key = (user, org, repo, ref, ref_type, pusher_type)

            if key not in bucket:
                bucket[key] = {}
            b = bucket[key]

            b["events"] = b.get("events", 0) + 1

    def bucket_to_rows(self, date: str, bucket: dict) -> Generator[dict, None, None]:

        for key in sorted(bucket):
            yield {
                "date": date,
                "user": key[0],
                "org": key[1],
                "repo": key[2],
                "ref": key[3],
                "ref_type": key[4],
                "pusher_type": key[5],
                **bucket[key],
            }


class WatchExporter(DateBucketExporter):

    NAME = "watch"

    def new_bucket(self, date: str) -> dict:
        return {}

    def add_to_bucket(self, date: str, bucket: dict, event: dict):
        if event["type"] == "WatchEvent":
            p = event["payload"]

            user = event["actor"].get("login", "???")
            repo = event["repo"]["name"]
            action = p["action"]

            key = (user, repo, action)

            if key not in bucket:
                bucket[key] = {}
            b = bucket[key]

            b["events"] = b.get("events", 0) + 1

    def bucket_to_rows(self, date: str, bucket: dict) -> Generator[dict, None, None]:

        for key in sorted(bucket):
            yield {
                "date": date,
                "user": key[0],
                "repo": key[1],
                "action": key[2],
                **bucket[key],
            }
