import asyncio
import json
import logging
import os
from functools import partial
from textwrap import dedent
from urllib.parse import quote

from packaging.version import Version as V
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.httputil import url_concat
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.log import LogFormatter
from traitlets import Bool, Int, Unicode, default
from traitlets.config import Application

import subprocess
import requests
import pathlib
import re


__version__ = "0.0.1.dev"

STATE_FILTER_MIN_VERSION = V("1.3.0")

secret_keys = ["grouper_user", "grouper_pass"]


def boolean_string(b):
    return {True: "T", False: "F"}[b]


def auth(user, password):
    return requests.auth.HTTPBasicAuth(user, password)


def add_members(base_uri, auth, group, replace_existing, members):
    """Replace the members of the grouper group {group} with {users}."""
    # https://github.com/Internet2/grouper/blob/master/grouper-ws/grouper-ws/doc/samples/addMember/WsSampleAddMemberRest_json.txt
    print(f"Adding members to {group}")
    data = {
        "WsRestAddMemberRequest": {
            "replaceAllExisting": boolean_string(replace_existing),
            "subjectLookups": [],
        }
    }
    for member in members:
        if type(member) == int or member.isalpha():
            # UUID
            member_key = "subjectId"
        else:
            # e.g. group path id
            member_key = "subjectIdentifier"
        data["WsRestAddMemberRequest"]["subjectLookups"].append({member_key: member})
    r = requests.put(
        f"{base_uri}/groups/{group}/members",
        data=json.dumps(data),
        auth=auth,
        headers={"Content-type": "text/x-json"},
    )
    out = r.json()
    problem_key = "WsRestResultProblem"
    try:
        if problem_key in out:
            print(f"{problem_key} in output")
            meta = out[problem_key]["resultMetadata"]
            raise Exception(meta)
        results_key = "WsAddMemberResults"
    except Exception as e:
        print(f" error: {e}")
    return out


async def sync_users_to_calgroups(
    namespace,
    url,
    api_token,
    grouper_user,
    grouper_pass,
    calgroup_base_url,
    logger,
    concurrency=10,
    api_page_size=0,
):

    defaults = {
        # GET /users may be slow if there are thousands of users and we
        # don't do any server side filtering so default request timeouts
        # to 60 seconds rather than tornado's 20 second default.
        "request_timeout": int(os.environ.get("JUPYTERHUB_REQUEST_TIMEOUT") or 60)
    }

    AsyncHTTPClient.configure(None, defaults=defaults)
    client = AsyncHTTPClient()

    if concurrency:
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch(req):
            """client.fetch wrapped in a semaphore to limit concurrency"""
            await semaphore.acquire()
            try:
                return await client.fetch(req)
            finally:
                semaphore.release()

    else:
        fetch = client.fetch

    async def fetch_paginated(req):
        """Make a paginated API request

        async generator, yields all items from a list endpoint
        """
        req.headers["Accept"] = "application/jupyterhub-pagination+json"
        url = req.url
        resp_future = asyncio.ensure_future(fetch(req))
        page_no = 1
        item_count = 0
        while resp_future is not None:
            response = await resp_future
            resp_future = None
            resp_model = json.loads(response.body.decode("utf8", "replace"))

            if isinstance(resp_model, list):
                # handle pre-2.0 response, no pagination
                items = resp_model
            else:
                # paginated response
                items = resp_model["items"]

                next_info = resp_model["_pagination"]["next"]
                if next_info:
                    page_no += 1
                    logger.info(f"Fetching page {page_no} {next_info['url']}")
                    # submit next request
                    req.url = next_info["url"]
                    resp_future = asyncio.ensure_future(fetch(req))

            for item in items:
                item_count += 1
                yield item

        logger.debug(f"Fetched {item_count} items from {url} in {page_no} pages")

    # Starting with jupyterhub 1.3.0 the users can be filtered in the server
    # using the `state` filter parameter. "ready" means all users who have any
    # ready servers (running, not pending).
    auth_header = {"Authorization": f"token {api_token}"}


    async def handle_user(users_to_process):
        """
        JUPYTERHUB_API_URL

        """

        if "staging" not in namespace:
            group_base = "edu:berkeley:app:datahub:"
            if namespace == "datahub":
                group_name = group_base + "datahub-users"
            else:
                group_name = group_base + "datahub-" + namespace + "-users"

            members = []
            for user in users_to_process:
                user_is_admin = user["admin"]
                if not user_is_admin:
                    user_name = user.get("name", "")
                    if "@berkeley.edu" not in user_name:
                        if "@" in user_name: ## @*.com @*.edu but not @berkeley.edu
                            print(f"Non-Berkeley Email: {user_name}")
                            continue
                        user_name = user_name + "@berkeley.edu"
                    members.append(user_name)

            try:
                grouper_auth = auth(
                    grouper_user, grouper_pass
                )
                logger.info(f"Found {len(members)} members to add in namespace {namespace}. ")
                logger.info(f"Group name: {group_name}")
                logger.info(f"Members: {members}")

                add_members(calgroup_base_url, grouper_auth, group_name, True, members)
                logger.info(f"Done adding members in namespace {namespace}. ")
            except subprocess.CalledProcessError as e:
                logger.debug(f"An error occurred while running the command: {e}")

    params = {}
    if api_page_size:
        params["limit"] = str(api_page_size)

    users_url = f"{url}/users"
    req = HTTPRequest(
        url=url_concat(users_url, params),
        headers=auth_header,
    )

    users_to_process = []
    async for user in fetch_paginated(req):
        users_to_process.append(user)

    await handle_user(users_to_process)


class CalgroupBuilder(Application):

    api_page_size = Int(
        0,
        help=dedent(
            """
            Number of users to request per page,
            when using JupyterHub 2.0's paginated user list API.
            Default: user the server-side default configured page size.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    concurrency = Int(
        10,
        help=dedent(
            """
            Limit the number of concurrent requests made to the Hub.

            Deleting a lot of users at the same time can slow down the Hub,
            so limit the number of API requests we have outstanding at any given time.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    sync_every = Int(
        0,
        help=dedent(
            """
            The interval (in seconds) for checking for idle servers to cull.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    @default("sync_every")
    def _default_sync_every(self):
        return self.timeout * 6

    _log_formatter_cls = LogFormatter

    @default("log_level")
    def _log_level_default(self):
        return logging.INFO

    @default("log_datefmt")
    def _log_datefmt_default(self):
        """Exclude date from default date format"""
        return "%Y-%m-%d %H:%M:%S"

    @default("log_format")
    def _log_format_default(self):
        """override default log format to include time"""
        return "%(color)s[%(levelname)1.1s %(asctime)s.%(msecs).03d %(name)s %(module)s:%(lineno)d]%(end_color)s %(message)s"

    timeout = Int(
        600,
        help=dedent(
            """
            The idle timeout (in seconds).
            """
        ).strip(),
    ).tag(
        config=True,
    )

    namespace = Unicode(
        os.environ.get("namespace"),
        allow_none=False,
        help=dedent(
            """
            The namespace of the deployment.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    url = Unicode(
        os.environ.get("JUPYTERHUB_API_URL"),
        allow_none=True,
        help=dedent(
            """
            The JupyterHub API URL.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    calgroup_base_url = Unicode(
        os.environ.get("Calgroup_Base_URL"),
        allow_none=False,
        help=dedent(
            """
            The Calgroup Base URL.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    grouper_user = Unicode(
        os.environ.get("grouper_user"),
        allow_none=False,
        help=dedent(
            """
            The grouper user.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    grouper_pass = Unicode(
        os.environ.get("grouper_pass"),
        allow_none=False,
        help=dedent(
            """
            The grouper password.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    aliases = {
        "api-page-size": "CalgroupBuilder.api_page_size",
        "concurrency": "CalgroupBuilder.concurrency",
        "cull-every": "CalgroupBuilder.cull_every",
        "timeout": "CalgroupBuilder.timeout",
        "namespace": "CalgroupBuilder.namespace",
        "url": "CalgroupBuilder.url",
        "calgroup_base_url": "CalgroupBuilder.calgroup_base_url",
        "grouper_user": "CalgroupBuilder.grouper_user",
        "grouper_pass": "CalgroupBuilder.grouper_pass",
    }

    def start(self):
        api_token = os.environ["JUPYTERHUB_API_TOKEN"]

        try:
            AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        except ImportError as e:
            self.log.warning(
                f"Could not load pycurl: {e}\n"
                "pycurl is recommended if you have a large number of users."
            )

        loop = IOLoop.current()
        sync_calgroups = partial(
            sync_users_to_calgroups,
            namespace=self.namespace,
            url=self.url,
            api_token=api_token,
            grouper_user=self.grouper_user,
            grouper_pass=self.grouper_pass,
            calgroup_base_url=self.calgroup_base_url,
            logger=self.log,
            concurrency=self.concurrency,
            api_page_size=self.api_page_size,
        )
        # schedule first sync immediately
        # because PeriodicCallback doesn't start until the end of the first interval
        loop.add_callback(sync_calgroups)
        # schedule periodic sync
        pc = PeriodicCallback(sync_calgroups, 1e3 * self.sync_every)
        pc.start()
        try:
            loop.start()
        except KeyboardInterrupt:
            pass


def main():
    CalgroupBuilder.launch_instance()


if __name__ == "__main__":
    main()
