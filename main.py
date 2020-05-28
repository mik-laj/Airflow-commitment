import datetime
import json
import os
from typing import NamedTuple, Optional

import requests
from tenacity import retry, wait_random_exponential, stop_after_attempt

PULL_REQUEST_QUERY = """
query getPullReueqts($cursor: String) {
  repository(owner: "apache", name: "airflow") {
    pullRequests(states: MERGED, first: 100, orderBy: {field: UPDATED_AT, direction: DESC}, after: $cursor) {
      nodes {
        permalink
        title
        mergedAt
        author {
          login
        }
        mergedBy {
          login
        }
        participants(first: 50) {
          nodes {
            login
          }
        }
        labels(first: 10) {
          nodes {
            name
          }
        }
      }
      totalCount
      pageInfo {
        endCursor
        startCursor
      }
    }
  }
}
"""

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

session = requests.Session()

LABEL_PROVIDER_GOOGLE = "provider:Google"


@retry(
    stop=stop_after_attempt(7),
    wait=wait_random_exponential(multiplier=1, max=120)
)
def send_request(query, variables):
    body = {"query": query}

    if variables:
        body["variables"] = variables

    headers = {}

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Token {GITHUB_TOKEN}"

    response = session.post("https://api.github.com/graphql", json=body, headers=headers)

    data = response.json()

    if "errors" in data:
        raise Exception(str(data["errors"]))

    if 'data' not in data:
        raise Exception(data)

    return data["data"]


POLIDEA_USERS = [
    "potiuk",
    "mschickensoup",
    "mik-laj",
    "turbaszek",
    "michalslowikowski00",
    "olchas",
]

ASTRONOMER_USERS = [
    "schnie", "ashb", "kaxil", "dimberman", "andriisoldatenko", "ryw", "andrewhharmon"
]


def username_to_company(username):
    if username in POLIDEA_USERS:
        return "Polidea"
    if username in ASTRONOMER_USERS:
        return "Astronomer"
    return "Unknowm"


class RepoAction(NamedTuple):
    merged_at: str
    permalink: str
    title: str
    author: str
    merged_by: str
    user_login: Optional[str]
    is_google: bool

    def as_dict(self):
        result = {
            "permalink": self.permalink,
            "title": self.title,
            "author": self.author,
            "author_company": username_to_company(self.author),
            "merged_by": self.merged_by,
            "merged_by_company": username_to_company(self.merged_by),
            "merged_at": self.merged_at,
            "user_login": self.user_login,
            "company": username_to_company(self.user_login),
            "is_google": "Y" if self.is_google else "N"
        }

        return result


print("Start fetching data")


def fetch_collection(collection_name, query):
    print("Start fetching collection: ", collection_name)
    cursor = None
    results = []
    while True:
        data = send_request(query=query, variables={"cursor": cursor})
        collection = data["repository"][collection_name]
        collection_nodes = collection["nodes"]
        results.extend(collection_nodes)
        total_count = collection["totalCount"]
        fetched_count = len(results)
        progress = fetched_count / total_count * 100
        print(f"Progress {progress:.2f}% ({fetched_count}/{total_count})")
        cursor = collection["pageInfo"]["endCursor"]
        if not cursor:
            break
    return results


pull_requests = fetch_collection("pullRequests", PULL_REQUEST_QUERY)

print(f"Fetched {len(pull_requests)} pull requests")

repo_actions = []
for pull_request in pull_requests:
    mergedAt = datetime.datetime.strptime(pull_request["mergedAt"], "%Y-%m-%dT%H:%M:%SZ")
    mergedAt = datetime.datetime.strftime(mergedAt, "%Y-%m-%dT%H:%M")

    for user in pull_request["participants"]["nodes"]:
        label_names = {n['name'] for n in pull_request['labels']['nodes']}
        try:
            repo_actions.append(
                RepoAction(
                    permalink=pull_request["permalink"],
                    title=pull_request["title"],
                    author=(pull_request.get('author', None) or {}).get('login', None),
                    merged_by=pull_request['mergedBy']['login'],
                    user_login=user["login"],
                    merged_at=mergedAt,
                    is_google=LABEL_PROVIDER_GOOGLE in label_names
                )
            )
        except:
            print("pull_request=", pull_request)
            print("user=", user)
            raise

print(f"Created {len(repo_actions)} items")


activity_dicts = [r.as_dict() for r in sorted(repo_actions)]
with open("all-activity.json", "w+") as f:
    json.dump(activity_dicts, f, sort_keys=True, indent=2)

print(f"Finished script")
