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

import logging
import datetime
import os
import azure.functions as func

from .traffic_tracker import Database, Repo, UserOrOrg

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

    # Uncomment this to load the repos from GitHub each time
    # user_or_org = UserOrOrg("<USER-OR-ORG>", os.getenv("GithubApiKey"))
    # repos = user_or_org.repos()
    
    for name, url in repos.items():
        repo = Repo("<REPO-OWNER>", name, url, os.getenv("GithubApiKey"))
        output = repo.metrics()
        cosmos_db.upload(output)
