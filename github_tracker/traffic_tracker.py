import datetime
import logging
import json
import requests
import uuid

import azure.functions as func
from azure.cosmos import CosmosClient


class Database:
    ''' Classs used to handle Cosmos DB
    ...

    Attributes
    ----------
    connection_string : str
        connection string from Azure to authenticate connection to CosmosDB.
        This should be set as an enviroment variable.
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


class Repo:
    ''' This class represnted the repo/metrics
    ...

    Attributes
    ----------
    owner : str
        owner of the repository, can be either a user or an organization
    name : str
        name of the reposistory, this doesn't have to match GitHub
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
    __API_URL_BASE = "https://api.github.com"
    __V4 = "/graphql"

    def __init__(self, owner, name, url, api_token):
        self.__owner = owner
        self.__name = name
        self.__url = url
        self.__headers = {"Authorization": "Bearer {}".format(api_token)}
        self.__v3 = "/repos/{}/".format(owner)

    def metrics(self):
        ''' Returns a dict of metrics from repository

        Returns
        -------
        dict
            a dict of the follwoing metric from GitHub repository:
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
        metrics = self.__build_output(get_yesterdays_date(),
                                      self.__name,
                                      self.__clones(),
                                      self.__views(),
                                      self.__query()
                                      )
        return metrics

    def __get_data(self, url, metric):
        # returns json from the GitHub API
        response = requests.get(self.__API_URL_BASE + self.__v3 + url + metric,
                                headers=self.__headers)
        if response.status_code == 200:
            return json.loads(response.content.decode("utf-8"))

        else:
            logging.info("GitHub failed to connect, check that the owner and repo url are set correctly")
            logging.error("GitHub connection error " +
                          str(response.status_code))

    def __validate_date(self, data, metric):
        # returns data entry for target date only
        # if no data is avialble for taget date returns 0 in all fields
        target_date = get_yesterdays_date()

        for key in data[metric]:
            date = key["timestamp"][:10]
            if date == target_date:
                return key
            else:
                data = {"timestamp": target_date, "count": 0, "uniques": 0}
        return data

    def __views(self):
        # returns total views and unique views for target date
        raw_views = self.__get_data(self.__url, '/traffic/views')
        __views = self.__validate_date(raw_views, 'views')
        return __views

    def __clones(self):
        # returns total clones and unique clones for target date
        raw_clones = self.__get_data(self.__url, '/traffic/clones')
        __clones = self.__validate_date(raw_clones, 'clones')
        return __clones

    def __v4_query(self):
        # returns formed GraphQL query
        repository = '{repository(name: "' + self.__url + \
                     '", owner: "' + self.__owner + '")'
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

    def __query(self):
        # returns dict with total forks, watchers, stars, and pull requests
        response = requests.post(
            self.__API_URL_BASE + self.__V4,
            headers=self.__headers,
            json={"query": self.__v4_query()}
        )
        if response.status_code == 200:
            return json.loads(response.content.decode("utf-8"))
        else:
            logging.info(response.status_code)

    def __build_output(self, date, repo, clones, views, query):
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
