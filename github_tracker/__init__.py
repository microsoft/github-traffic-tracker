import logging
import datetime
import os
import azure.functions as func

from .traffic_tracker import Database, Repo

# Repos to collect metrics from.
# The first variable is a human readable name, the second needs to match the
# url extension for the repo exactly.
repos = {
    "<REPO-NAME>": "<REPO-URL-EXTENSION>",
}


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
        repo = Repo("<REPO-OWNER>", name, url, os.getenv("GithubApiKey"))
        output = repo.metrics()
        cosmos_db.upload(output)
