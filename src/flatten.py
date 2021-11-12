

def flatten_event(e: dict) -> dict:
    # NOT USED A.T.M.
    row = {
        "id": e["id"],
        "date": e["created_at"],
        "type": e["type"],
        "actor": e["actor"]["login"],
        "repo": e["repo"]["name"],
        "action": None,
        "size": None,
        #"description": None,
        #"body": None,
    }
    if "payload" in e:
        p = e["payload"]
        row["action"] = p.get("action")
        row["size"] = p.get("size")

        if 0:
            row["description"] = p.get("description")
            #print(p["description"])
            if e["type"] == "IssueCommentEvent":
                row["body"] == p["comment"]["body"]
            if "issue" in p:
                p2 = p["issue"]
                row["description"] = p2.get("description")
                row["body"] = p2["body"]
            #print(e["type"], p.keys())

    return row