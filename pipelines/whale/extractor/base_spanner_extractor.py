import json
import logging
import os
import re
from collections import namedtuple

import google.oauth2.service_account
import google_auth_httplib2
from googleapiclient.discovery import build
import httplib2
from pyhocon import ConfigTree  # noqa: F401
from typing import List, Any  # noqa: F401

from databuilder.extractor.base_extractor import Extractor


LOGGER = logging.getLogger(__name__)


class BaseSpannerExtractor(Extractor):
    PROJECT_ID_KEY = "project_id"
    KEY_PATH_KEY = "key_path"
    # sometimes we don't have a key path, but only have an variable
    CRED_KEY = "project_cred"
    PAGE_SIZE_KEY = "page_size"
    FILTER_KEY = "filter"
    _DEFAULT_SCOPES = [
        "https://www.googleapis.com/auth/spanner.admin",
    ]
    DEFAULT_PAGE_SIZE = 300
    NUM_RETRIES = 3

    def init(self, conf: ConfigTree) -> None:
        # should use key_path, or cred_key if the former doesn't exist
        key_path = conf.get_string(BaseSpannerExtractor.KEY_PATH_KEY, None)
        try:
            self.key_path = os.path.expanduser(key_path)
        except:
            self.key_path = key_path
        self.cred_key = conf.get_string(BaseSpannerExtractor.CRED_KEY, None)
        self.project_id = conf.get_string(BaseSpannerExtractor.PROJECT_ID_KEY)
        self.pagesize = conf.get_int(
            BaseSpannerExtractor.PAGE_SIZE_KEY, BaseSpannerExtractor.DEFAULT_PAGE_SIZE
        )

        if self.key_path:
            credentials = (
                google.oauth2.service_account.Credentials.from_service_account_file(
                    self.key_path, scopes=self._DEFAULT_SCOPES
                )
            )
        else:
            if self.cred_key:
                service_account_info = json.loads(self.cred_key)
                credentials = (
                    google.oauth2.service_account.Credentials.from_service_account_info(
                        service_account_info, scopes=self._DEFAULT_SCOPES
                    )
                )
            else:
                credentials, _ = google.auth.default(scopes=self._DEFAULT_SCOPES)

        http = httplib2.Http()
        authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)
        self.spanner_service = build(
            "spanner", "v1", http=authed_http, cache_discovery=False
        )
        self.iter = iter(self._iterate_over_databases())

    def extract(self) -> Any:
        try:
            return next(self.iter)
        except StopIteration:
            return None

    def _iterate_over_databases(self):
        for instance in self._retrieve_instances():
            for entry in self._retrieve_databases(instance):
                yield (entry)

    def _retrieve_instances(self):
        instances = []
        for page in self._page_instance_list_results():
            if "instances" not in page:
                continue

            for instance in page["instances"]:
                instance_ref = instance["name"]
                instances.append(instance_ref)

        return instances

    def _page_instance_list_results(self):
        # type: () -> Any

        response = (
            self.spanner_service.projects()
            .instances()
            .list(
                pageSize=self.pagesize,
                parent="projects/{}".format(self.project_id),
            )
            .execute()
        )

        while response:
            yield response

            if "nextPageToken" in response:

                response = (
                    self.spanner_service.projects()
                    .instances()
                    .list(
                        parent="projects/{}".format(self.project_id),
                        pageToken=response["nextPageToken"],
                    )
                    .execute(num_retries=BaseSpannerExtractor.NUM_RETRIES)
                )
            else:
                response = None

    def _page_database_list_results(self, instance: str):

        response = (
            self.spanner_service.projects()
            .instances()
            .databases()
            .list(
                pageSize=self.pagesize,
                parent=instance,
            )
            .execute(num_retries=BaseSpannerExtractor.NUM_RETRIES)
        )

        while response:
            yield response

            if "nextPageToken" in response:
                response = (
                    self.spanner_service.projects()
                    .instances()
                    .databases()
                    .list(
                        parent=instance,
                        pageToken=response["nextPageToken"],
                    )
                    .execute(num_retries=BaseSpannerExtractor.NUM_RETRIES)
                )
            else:
                response = None

    def get_scope(self) -> str:
        return "extractor.spanner_table_metadata"
