import datetime
import logging
import json
import requests
import os
import uuid

import azure.functions as func
from azure.cosmos import CosmosClient


class Database:
    def __init__(self, connection_string, database_name, container_name):
        self._client = CosmosClient.from_connection_string(connection_string)
        self._database = self._client.get_database_client(database_name)
        self._container_client = self._database.get_container_client(
                                 container_name
                                 )

    def upload(self, data):
        self._container_client.upsert_item(data)


owner = "<USER-OR-ORG-OWNER>"

api_token = os.getenv("GithubApiKey")
headers = {"Authorization": "Bearer {}".format(api_token)}
api_url_base = "https://api.github.com"
v4 = "/graphql"
v3 = "/repos/{}/".format(owner)


# Repos to collect metrics from.
# The first variable is a human readabl name, the second needs to match the url
# for the repo exactly.
repos = {
    "<REPO-NAME>": "<REPO-URL-EXTENSION>",
}

# REST api (v3) metrics
traffic_views = "/traffic/views"
clones = "/traffic/clones"


# GaphQL api (v4) query
def build_query(repo):
    repository = '{repository(name: "' + repo + '", owner: "' + owner + '")'
    metrics = "{ \
        forks { \
            totalCount \
        }\
        watchers {\
            totalCount\
        }\
        stargazers {\
            totalCount\
        }\
        pullRequests{\
            totalCount\
        }\
    }\
    }"
    query = repository + metrics
    return query


def get_data(repo, metric):
    response = requests.get(api_url_base + v3 + repo + metric, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode("utf-8"))

    else:
        logging.info(response.status_code)


def parse_data(data, metric):
    parsed_data = data[metric][-1]
    return parsed_data


def get_yesterdays_date():
    today = datetime.date.today()
    yesterdays_date = str(today - datetime.timedelta(days=1))
    return yesterdays_date


def validate_date(data, metric):
    target_date = get_yesterdays_date()

    for key in data[metric]:
        date = key["timestamp"][:10]
        if date == target_date:
            return key
        else:
            # TODO: Abstact this to make more general
            data = {"timestamp": target_date, "count": 0, "uniques": 0}
    return data


def post_query(query):
    response = requests.post(
        api_url_base + v4, headers=headers, json={"query": query}
    )
    if response.status_code == 200:
        return json.loads(response.content.decode("utf-8"))
    else:
        logging.info(response.status_code)


def build_output(date, repo, clones, views, query):
    output_doc = {}
    output_doc = {
        "id": str(uuid.uuid4()),
        "category": "GitHub Repo",
        "date": date,
        "repo": repo,
        "metrics": {
            "views": {
                "count": "{}".format(views["count"]),
                "unique": "{}".format(views["uniques"]),
            },
            "clones": {
                "count": "{}".format(clones["count"]),
                "unique": "{}".format(clones["uniques"]),
            },
            "forks": "{}".format(query["forks"]["totalCount"]),
            "watchers": "{}".format(query["watchers"]["totalCount"]),
            "stars": "{}".format(query["stargazers"]["totalCount"]),
            "pull requests": "{}".format(query["pullRequests"]["totalCount"])
        },
    }

    return output_doc


def main(mytimer: func.TimerRequest):
    utc_timestamp = (
        datetime.datetime.utcnow()
        .replace(tzinfo=datetime.timezone.utc)
        .isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function ran at %s", utc_timestamp)
    cosmos_db = Database(
                os.getenv("CosmosDBConnectionString"),
                "<YOUR-DATABASE-NAME>",
                "<YOUR-CONTAINER-NAME>"
                )
    for name, url in repos.items():
        raw_views = get_data(url, traffic_views)
        views = validate_date(raw_views, "views")

        raw_clones = get_data(url, clones)
        total_clones = validate_date(raw_clones, "clones")

        graphql_query = post_query(build_query(url))["data"]["repository"]

        output = build_output(
            get_yesterdays_date(), name, total_clones, views, graphql_query
        )

        cosmos_db.upload(output)
