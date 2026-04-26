import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from mcp_amundsen.clients import SearchApiClient, TableApiClient
from mcp_amundsen.processors import MetadataProcessor, SearchProcessor

# Initialize MCP server
host = os.environ.get("FASTMCP_HOST", "127.0.0.1")
port = int(os.environ.get("FASTMCP_PORT", "8000"))
mcp = FastMCP("Amundsen", host=host, port=port)

# Initialize clients and processors
table_client = TableApiClient()
search_client = SearchApiClient()
metadata_processor = MetadataProcessor()
search_processor = SearchProcessor()


@mcp.tool()
def get_table_columns(database: str, schema_name: str, table_name: str) -> dict[str, Any]:
    """
    Fetches table columns for a specific table and extracts simplified information.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :return: A dictionary containing simplified column information.
    """
    table_data = table_client.get_metadata(database, schema_name, table_name)
    return metadata_processor.extract_columns(table_data)


@mcp.tool()
def get_table_date_range(database: str, schema_name: str, table_name: str) -> dict[str, Any]:
    """
    Fetches the available date range for a specific table.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :return: A dictionary containing the date range.
    """
    table_data = table_client.get_metadata(database, schema_name, table_name)
    return metadata_processor.extract_date_range(table_data)


@mcp.tool()
def get_table_code_url(database: str, schema_name: str, table_name: str) -> str:
    """
    Fetches the source code URL for the pipeline that generates this table.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :return: url of code that generate the table
    """
    table_data = table_client.get_metadata(database, schema_name, table_name)
    return metadata_processor.extract_code_location(table_data)


@mcp.tool()
def get_table_airflow_url(database: str, schema_name: str, table_name: str) -> str:
    """
    Fetches the airflow url of a specific table.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :return: url of airflow that generate the table
    """
    table_data = table_client.get_metadata(database, schema_name, table_name)
    return metadata_processor.extract_airflow_url(table_data)


@mcp.tool()
def get_table_schedule(database: str, schema_name: str, table_name: str) -> str:
    """
    Fetches the schedule update of a specific table.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :return: Schedule of the table update
    """
    table_data = table_client.get_metadata(database, schema_name, table_name)
    parsed_programmatic_descriptions = metadata_processor.parse_programmatic_descriptions(table_data)
    return metadata_processor.extract_table_schedule(parsed_programmatic_descriptions)


@mcp.tool()
def get_table_storage_location(database: str, schema_name: str, table_name: str) -> str:
    """
    Fetches the storage location update of a specific table.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :return: storage location of the table update
    """
    table_data = table_client.get_metadata(database, schema_name, table_name)
    parsed_programmatic_descriptions = metadata_processor.parse_programmatic_descriptions(table_data)
    return metadata_processor.extract_table_storage_location(parsed_programmatic_descriptions)


@mcp.tool()
def get_table_owners(database: str, schema_name: str, table_name: str) -> list[str] | dict[str, Any]:
    """
    Fetches the owners of a specific table.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :return: A list containing the table owners.
    """
    table_data = table_client.get_metadata(database, schema_name, table_name)
    return metadata_processor.extract_owners(table_data)


@mcp.tool()
def get_table_lineage_info(
    database: str,
    schema_name: str,
    table_name: str,
    depth: int = 10,
    direction: str = "both",
) -> dict[str, Any]:
    """
    Fetches lineage information for a specific table.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :param depth: The depth of the lineage to fetch.
    :param direction: The direction of the lineage to fetch (upstream, downstream, or both).
    :return: A dictionary containing the lineage information.
    """
    return table_client.get_lineage(database, schema_name, table_name, depth, direction)


@mcp.tool()
def get_table_dashboard_info(database: str, schema_name: str, table_name: str) -> list[dict[str, Any]] | dict[str, Any]:
    """
    Fetches dashboards created from a specific table.

    :param database: The name of the database.
    :param schema_name: The name of the schema.
    :param table_name: The name of the table.
    :return: A list of dictionaries containing dashboard information.
    """
    dashboard_data = table_client.get_dashboards(database, schema_name, table_name)
    return metadata_processor.extract_dashboards(dashboard_data)


@mcp.tool()
def get_table_dashboard_questions(
    database: str, schema: str, table: str, dashboard_name: str, limit: int = 10
) -> list[dict[str, Any]] | dict[str, Any]:
    """
    Fetches dashboards questions.

    :param database: The name of the database.
    :param schema: The name of the schema.
    :param table: The name of the table.
    :param dashboard_name: The name of the dashboard.
    :return: A list of questions struct
    """
    questions = table_client.questions(database, schema, table, dashboard_name, limit)
    response = []
    for question in questions:
        data = {"name": question["name"], "url": question["url"]}
        response.append(data)

    return response


@mcp.tool()
def get_table_dashboard_question_query(
    database: str, schema: str, table: str, dashboard_name: str, question_name: str
) -> dict[str, Any]:
    """
    Fetches specific query from dashboard question.

    :param database: The name of the database.
    :param schema: The name of the schema.
    :param table: The name of the table.
    :param dashboard_name: The name of the dashboard.
    :param question_name: The name of the question.
    :return: query text of specific question
    """
    question = table_client.question_detail(database, schema, table, dashboard_name, question_name)

    return {"result": {"query": question.get("query_text", "")}}


@mcp.tool()
def search_tables(
    query_term: str,
    schemas: list[str] = [],
    tables: list[str] = [],
    columns: list[str] = [],
    column_filter_operator: str = "OR",
    tags: list[str] = [],
    exclusions: dict[str, list[str]] = {},
    limit: int = 10,
) -> dict[str, Any]:
    """
    Performs a search query against the Amundsen search API.

    :param query_term: The search term to use. To search all keywords, pass an empty string ("") instead of "*".
    :param schemas: A list of schemas to filter by.
    :param tables: A list of tables to filter by.
    :param columns: A list of columns to filter by.
    :param column_filter_operator: The operator to use when filtering by columns (OR or AND).
    :param tags: A list of tags to filter by.
    :return: A dictionary containing the search results.
    """
    search_result = search_client.get_search_data(
        query_term,
        ["hive"],
        schemas,
        tables,
        columns,
        column_filter_operator,
        tags,
        exclusions,
        limit,
    )
    return search_processor.extract_search_data(search_result)
