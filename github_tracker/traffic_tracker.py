# Copyright (c) Microsoft Corporation. All Rights Reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the Software), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE

import datetime
import logging
import json
import requests
import uuid

import azure.functions as func
from azure.cosmos import CosmosClient

_API_URL_BASE = "https://api.github.com"

class Database:
    ''' Class used to handle Cosmos DB
    ...

    Attributes
    ----------
    connection_string : str
        connection string from Azure to authenticate connection to CosmosDB.
        This should be set as an environment variable.
    database_name : str
        database name in Azure
    container_name : str
        name of container within the database

    Methods
    -------
    upload(data)
        Writes data to CosmosDB database
    '''

    def __init__(self, connection_string, database_name, container_name):
        self._client = CosmosClient.from_connection_string(connection_string)
        self._database = self._client.get_database_client(database_name)
        self._container_client = self._database.get_container_client(
                                 container_name
                                 )

    def upload(self, data):
        ''' Write data to CosmosDB database

        Parameters
        ----------
        data : dict
            JSON formated data

        '''
        self._container_client.upsert_item(data)

class UserOrOrg:
    '''This call represents a GitHub org
    '''
    def __init__(self, owner, api_token):
        self._owner = owner
        self._headers = {"Authorization": "Bearer {}".format(api_token)}
        self._userrepos = "/users/{}/repos".format(owner)

    def repos(self):
        '''Returns a list of all repos for an org
        '''
        results = dict()
        page = 1
        per_page = 100
        while True:
            params = { "per_page" : per_page, "page" : page}
            response = requests.get(_API_URL_BASE + self._userrepos, headers=self._headers, params=params)
            if response.status_code == 200:
                repos = json.loads(response.content.decode("utf-8"))
                for r in repos:
                    results.update({r["name"]:r["name"]})

                if len(repos) < per_page:
                    break

                page += 1

            elif response.status_code == 404 or 403 or 400:
                logging.info("GitHub failed to connect, check that the owner and repo url are set correctly")
                logging.warning("GitHub connection error " +
                                str(response.status_code))
                
                return dict()

        return results

class Repo:
    ''' This class represents the repo/metrics
    ...

    Attributes
    ----------
    owner : str
        owner of the repository, can be either a user or an organization
    name : str
        name of the repository, this doesn't have to match GitHub
    url : str
        the GitHub URL extension for the repository
    api_token:
        OAuth token for GitHub
    metrics : dict
        returns a dict of the metrics of the repo.

    Methods
    -------
    metrics()
        Returns a dict of the views, clones, watchers, stars, forks, and pull
        requests for the repository
    '''
    _V4 = "/graphql"

    def __init__(self, owner, name, url, api_token):
        self._owner = owner
        self._name = name
        self._url = url
        self._headers = {"Authorization": "Bearer {}".format(api_token)}
        self._v3 = "/repos/{}/".format(owner)

    def metrics(self):
        ''' Returns a dict of metrics from repository

        Returns
        -------
        dict
            a dict of the following metric from GitHub repository:
            - UUID
            - date
            - repo name
            - views
            - clones
            - watchers
            - stars
            - forks
            - pull requests
        '''
        metrics = self._build_output(get_yesterdays_date(),
                                     self._name,
                                     self._clones(),
                                     self._views(),
                                     self._query()
                                     )
        return metrics

    def _get_data(self, url, metric):
        # returns json from the GitHub API
        response = requests.get(_API_URL_BASE + self._v3 + url + metric,
                                headers=self._headers)
        if response.status_code == 200:
            return json.loads(response.content.decode("utf-8"))

        elif response.status_code == 404 or 403 or 400:
            logging.info("GitHub failed to connect, check that the owner and repo url are set correctly")
            logging.warning("GitHub connection error " +
                            str(response.status_code))

    def _validate_date(self, data, metric):
        # returns data entry for target date only
        # if no data is available for target date returns 0 in all fields
        target_date = get_yesterdays_date()

        for key in data[metric]:
            date = key["timestamp"][:10]
            if date == target_date:
                return key
            else:
                data = {"timestamp": target_date, "count": 0, "uniques": 0}
        return data

    def _views(self):
        # returns total views and unique views for target date
        raw_views = self._get_data(self._url, '/traffic/views')
        views = self._validate_date(raw_views, 'views')
        return views

    def _clones(self):
        # returns total clones and unique clones for target date
        raw_clones = self._get_data(self._url, '/traffic/clones')
        clones = self._validate_date(raw_clones, 'clones')
        return clones

    def _v4_query(self):
        # returns formed GraphQL query
        repository = '{repository(name: "' + self._url + \
                     '", owner: "' + self._owner + '")'
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
        # returns dict with total forks, watchers, stars, and pull requests
        response = requests.post(
            _API_URL_BASE + self._V4,
            headers=self._headers,
            json={"query": self._v4_query()}
        )
        if response.status_code == 200:
            return json.loads(response.content.decode("utf-8"))
        else:
            logging.info(response.status_code)

    def _build_output(self, date, repo, clones, views, query):
        # returns formed output dict
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


def get_yesterdays_date():
    ''' Method to get yesterday's date

    Returns
    -------
    str
        yesterday's date DD/MM/YYYY
    '''
    today = datetime.date.today()
    yesterdays_date = str(today - datetime.timedelta(days=1))
    return yesterdays_date
