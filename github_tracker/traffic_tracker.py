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


class Repo:
    _api_url_base = "https://api.github.com"
    _v4 = "/graphql"

    def __init__(self, owner, name, url, api_token):
        self._owner = owner
        self._name = name
        self._url = url
        self._headers = {"Authorization": "Bearer {}".format(api_token)}
        self._v3 = "/repos/{}/".format(owner)

    def metrics(self):
        metrics = self._build_output(get_yesterdays_date(),
                                     self._name,
                                     self._clones(),
                                     self._views(),
                                     self._query()
                                     )
        return metrics

    def _get_data(self, url, metric):
        response = requests.get(self._api_url_base + self._v3 + url + metric,
                                headers=self._headers)
        if response.status_code == 200:
            return json.loads(response.content.decode("utf-8"))

        else:
            logging.info(response.status_code)

    def _validate_date(self, data, metric):
        target_date = get_yesterdays_date()

        for key in data[metric]:
            date = key["timestamp"][:10]
            if date == target_date:
                return key
            else:
                data = {"timestamp": target_date, "count": 0, "uniques": 0}
        return data

    def _views(self):
        _raw_views = self._get_data(self._url, '/traffic/views')
        _views = self._validate_date(_raw_views, 'views')
        return _views

    def _clones(self):
        _raw_clones = self._get_data(self._url, '/traffic/clones')
        _clones = self._validate_date(_raw_clones, 'clones')
        return _clones

    def _v4_query(self):
        repository = '{repository(name: "' + self._url + '", owner: "' + self._owner + '")'
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

    def _query(self):
        response = requests.post(
            self._api_url_base + self._v4,
            headers=self._headers,
            json={"query": self._v4_query()}
        )
        if response.status_code == 200:
            return json.loads(response.content.decode("utf-8"))
        else:
            logging.info(response.status_code)

    def _build_output(self, date, repo, clones, views, query):
        output_doc = {}
        filtered_query = query["data"]["repository"]
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
                "forks": "{}".format(filtered_query["forks"]
                                                   ["totalCount"]),
                "watchers": "{}".format(filtered_query["watchers"]
                                                      ["totalCount"]),
                "stars": "{}".format(filtered_query["stargazers"]
                                                   ["totalCount"]),
                "pull requests": "{}".format(filtered_query["pullRequests"]
                                                           ["totalCount"])
            },
        }

        return output_doc


# Repos to collect metrics from.
# The first variable is a human readabl name, the second needs to match the url
# for the repo exactly.
repos = {
    "<REPO-NAME>": "<REPO-URL-EXTENSION>",
}


def get_yesterdays_date():
    today = datetime.date.today()
    yesterdays_date = str(today - datetime.timedelta(days=1))
    return yesterdays_date


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
        repo = Repo("<USER-OR-ORG-OWNER>", name, url, os.getenv("GithubApiKey"))
        output = repo.metrics()
        cosmos_db.upload(output)
        del repo
