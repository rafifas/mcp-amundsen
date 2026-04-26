import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

from mcp_amundsen.errors import AmundsenApiError, TableNotFoundError

logger = logging.getLogger(__name__)


class AmundsenApiClient:
    """Base class for Amundsen API interactions."""

    BASE_URL = os.environ.get("AMUNDSEN_API_URL", "http://localhost:5000")

    def make_request(self, url: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Makes an HTTP request to the specified URL.

        Args:
            url (str): The URL to make the request to
            data (dict, optional): The payload for POST requests. Defaults to None.

        Returns:
            dict: The parsed JSON response

        Raises:
            AmundsenApiError: If the API returns an error
        """
        logger.debug(f"Requesting: {url}")

        try:
            headers = {}
            if data:
                headers["Content-Type"] = "application/json"

            req = urllib.request.Request(url, headers=headers)

            if data:
                req.data = json.dumps(data).encode("utf-8")

            timeout = int(os.environ.get("AMUNDSEN_REQUEST_TIMEOUT_SECONDS", "30"))
            with urllib.request.urlopen(req, timeout=timeout) as response:
                response_data = response.read().decode("utf-8")
                return json.loads(response_data)
        except urllib.error.HTTPError as e:
            raise AmundsenApiError(f"HTTP Error: {e.code} {e.reason}", status_code=e.code)
        except urllib.error.URLError as e:
            raise AmundsenApiError(f"URL Error: {e.reason}")


class SearchApiClient(AmundsenApiClient):
    """Client for interacting with Amundsen Search API endpoints."""

    BASE_URL = os.environ.get("AMUNDSEN_SEARCH_API_URL", "http://localhost:5001")

    @staticmethod
    def _add_wildcard_to_filter(filter_input: list[str]) -> list[str]:
        """Adds wildcards to each filter parameter."""
        return [f"*{param}*" for param in filter_input]

    def search(
        self,
        query_term: str,
        page_index: int = 0,
        results_per_page: int = 10,
        filters: list | None = None,
    ) -> dict[str, Any]:
        """
        Performs a search query against the Amundsen search API.
        """
        url = f"{self.BASE_URL}/v2/search"
        payload = {
            "query_term": query_term,
            "page_index": page_index,
            "results_per_page": results_per_page,
            "resource_types": ["table"],
            "filters": filters or [],
        }
        return self.make_request(url, data=payload)

    def get_search_data(
        self,
        query_term: str,
        databases: list[str] = [],
        schemas: list[str] = [],
        tables: list[str] = [],
        columns: list[str] = [],
        column_filter_operator: str = "OR",
        tags: list[str] = [],
        exclusions: dict[str, list[str]] = {},
        limit: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Performs a paginated search query and returns all table results.
        """
        filters = [
            {
                "name": "database",
                "values": self._add_wildcard_to_filter(databases),
                "operation": "OR",
            },
            {
                "name": "schema",
                "values": self._add_wildcard_to_filter(schemas),
                "operation": "OR",
            },
            {
                "name": "table",
                "values": self._add_wildcard_to_filter(tables),
                "operation": "OR",
            },
            {
                "name": "column",
                "values": self._add_wildcard_to_filter(columns),
                "operation": column_filter_operator,
            },
            {
                "name": "tag",
                "values": self._add_wildcard_to_filter(tags),
                "operation": "OR",
            },
        ]

        def filter_by_exclusions(data: list[dict[str, Any]], exclusions: dict[str, list[str]]):
            filtered_data = []
            for d in data:
                if not any(d.get(key) in values for key, values in exclusions.items()):
                    filtered_data.append(d)
            return filtered_data

        page_index = 0
        all_results = []

        while True:
            search_result = self.search(query_term, page_index=page_index, results_per_page=10, filters=filters)
            table_results = search_result.get("results", {}).get("table", {})
            result_table = table_results.get("results")

            if not table_results or not result_table:
                break

            if exclusions:
                result_table = filter_by_exclusions(result_table, exclusions)

            if limit > 0:
                if len(all_results) == limit:
                    break

                required_result = limit - len(all_results)

                if len(result_table) >= required_result:
                    result_table = result_table[:required_result]

            all_results.extend(result_table)

            if len(all_results) >= table_results.get("total_results", 0):
                break

            page_index += 1

        return all_results


class TableApiClient(AmundsenApiClient):
    """Client for interacting with Amundsen Table API endpoints."""

    def get_table_url(self, database: str, schema_name: str, table_name: str) -> str:
        """
        Constructs the base table URL.
        """
        # 'gold' is the standard Amundsen cluster name: {database}://{cluster}.{schema}/{table}
        # Override with AMUNDSEN_CLUSTER_NAME env var if your deployment uses a different cluster name.
        cluster = os.environ.get("AMUNDSEN_CLUSTER_NAME", "gold")
        return f"{self.BASE_URL}/table/{database}://{cluster}.{schema_name}/{table_name}"

    def get_metadata(self, database: str, schema_name: str, table_name: str) -> dict[str, Any]:
        """
        Fetches metadata for a specific table.
        """
        url = self.get_table_url(database, schema_name, table_name)
        try:
            return self.make_request(url)
        except AmundsenApiError as e:
            if e.status_code == 404:
                raise TableNotFoundError(table_name)
            raise e

    def get_lineage(
        self,
        database: str,
        schema_name: str,
        table_name: str,
        depth: int = 1,
        direction: str = "both",
    ) -> dict[str, Any]:
        """
        Fetches lineage information for a specific table.
        """
        if direction not in ["both", "upstream", "downstream"]:
            raise ValueError("Invalid direction. Must be one of: both, upstream, downstream")

        base_url = self.get_table_url(database, schema_name, table_name)
        url = f"{base_url}/lineage?depth={depth}&direction={direction}"
        return self.make_request(url)

    def get_dashboards(self, database: str, schema_name: str, table_name: str) -> dict[str, Any]:
        """
        Fetches dashboards for a specific table.
        """
        base_url = self.get_table_url(database, schema_name, table_name)
        url = f"{base_url}/dashboard/"
        return self.make_request(url)

    def dashboard_detail(self, dashboard_uri: str):
        url = f"{self.BASE_URL}/dashboard/{dashboard_uri}"
        return self.make_request(url)

    def questions(
        self,
        database: str,
        schema: str,
        table: str,
        dashboard_name: str,
        limit: int = 0,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        resp = self.get_dashboards(database, schema, table)

        if "error" in resp:
            return {"error": resp["error"]}

        dashboards = resp.get("dashboards", [])

        try:
            dashboard = next(d for d in dashboards if d["name"] == dashboard_name)
        except Exception as error:
            return {"error": f"dashboard name: {dashboard_name} not found"}

        dashboard_uri = dashboard.get("uri")
        if dashboard_uri is None:
            return {"error": "dashboard uri not defined"}

        dashboard_detail = self.dashboard_detail(dashboard_uri)
        queries = dashboard_detail.get("queries", [])

        if limit > 0:
            queries = queries[:limit]

        return queries

    def question_detail(
        self,
        database: str,
        schema: str,
        table: str,
        dashboard_name: str,
        question_name: str,
    ) -> dict[str, Any]:
        questions = self.questions(database, schema, table, dashboard_name)
        try:
            question = next(q for q in questions if q["name"] == question_name)
        except Exception as error:
            return {"error": f"question name: {question_name} {error}"}

        return question
