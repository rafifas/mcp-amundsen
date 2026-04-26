import re
from typing import Any


class MetadataProcessor:
    @staticmethod
    def extract_columns(
        table_data: dict[str, Any],
    ) -> dict[str, Any]:
        if "error" in table_data:
            return {"error": table_data["error"]}

        column_metadata = {"columns": [], "partition_keys": []}

        for column in table_data.get("columns", []):
            column_info = {
                "name": column.get("name"),
                "type": column.get("col_type"),
                "description": column.get("description"),
            }

            column_metadata["columns"].append(column_info)

            for badge in column.get("badges", []):
                if badge.get("badge_name") == "partition column":
                    column_metadata["partition_keys"].append(column_info)

        return column_metadata

    @staticmethod
    def extract_date_range(table_data: dict[str, Any]) -> dict[str, Any]:
        if "error" in table_data:
            return {"error": table_data["error"]}

        date_range = {"from": None, "to": None}

        for watermark in table_data.get("watermarks", []):
            watermark_type = watermark.get("watermark_type")
            range_value = {
                "key": watermark.get("partition_key"),
                "value": watermark.get("partition_value"),
            }

            if watermark_type == "low_watermark":
                date_range["from"] = range_value

            elif watermark_type == "high_watermark":
                date_range["to"] = range_value

        return date_range

    @staticmethod
    def extract_owners(table_data: dict[str, Any]) -> list[str] | dict[str, Any]:
        if "error" in table_data:
            return {"error": table_data["error"]}

        owners = []

        for owner in table_data.get("owners", []):
            user_id = owner.get("user_id")
            owners.append(user_id)

        return owners

    @staticmethod
    def extract_dashboards(
        dashboard_data: dict[str, Any],
    ) -> list[dict[str, Any]] | dict[str, Any]:
        if "error" in dashboard_data:
            return {"error": dashboard_data["error"]}

        raw_dashboards = dashboard_data.get("dashboards", [])
        dashboards = []

        for raw_dashboard in raw_dashboards:
            dashboard_url = raw_dashboard.get("url")
            dashboard_name = raw_dashboard.get("name")
            collection_name = raw_dashboard.get("group_name")
            dashboards.append(
                {
                    "url": dashboard_url,
                    "dashboard_name": dashboard_name,
                    "collection_name": collection_name,
                }
            )

        return dashboards

    @staticmethod
    def extract_code_location(table_data: dict[str, Any]) -> str:
        gitlab_url = table_data.get("source", {}).get("source")
        return gitlab_url

    @staticmethod
    def extract_airflow_url(table_data: dict[str, Any]) -> str:
        airflow_url = table_data.get("table_writer", {}).get("application_url")
        return airflow_url

    @staticmethod
    def parse_programmatic_descriptions(table_data: dict[str, Any]) -> dict[str, Any]:
        # regex to capture section name and content
        try:
            programmatic_descriptions = table_data.get("programmatic_descriptions", [])[0]
        except Exception as error:
            return {"error": f"programmatic_descriptions not found {error}"}

        programmatic_descriptions = programmatic_descriptions.get("text", "")
        pattern = r"\*\*\*(.*?)\*\*\*:\s*\n*([^*]+)?"
        matches = re.findall(pattern, programmatic_descriptions, re.DOTALL)

        result = {"schema": table_data.get("schema"), "table": table_data.get("name")}

        for key, value in matches:
            # strip whitespace and normalize
            cleaned_value = value.strip() if value else ""
            result[key.strip()] = cleaned_value

        return result

    @staticmethod
    def extract_table_schedule(parsed_programmatic_descriptions: dict[str, Any]) -> str:
        return parsed_programmatic_descriptions.get("Schedule", "")

    @staticmethod
    def extract_table_storage_location(
        parsed_programmatic_descriptions: dict[str, Any],
    ) -> str:
        table_gcs_location = parsed_programmatic_descriptions.get("GCS Path", "")

        return table_gcs_location


class SearchProcessor:
    @staticmethod
    def _extract_table_info(table: dict[str, Any]) -> dict[str, Any | None]:
        """Extracts relevant information from a single table dictionary."""
        return {
            "database": table.get("database"),
            "schema": table.get("schema"),
            "table": table.get("table"),
            "description": table.get("description"),
        }

    def extract_search_data(self, search_results: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Extracts and processes search results for tables.
        """
        if not search_results:
            return {"total_results": 0, "table_results": []}

        table_results = [self._extract_table_info(table) for table in search_results]

        return {
            "total_results": len(table_results),
            "table_results": table_results,
        }
