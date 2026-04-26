from unittest.mock import MagicMock, patch

import pytest

from mcp_amundsen.main import (
    get_table_airflow_url,
    get_table_code_url,
    get_table_columns,
    get_table_dashboard_info,
    get_table_dashboard_question_query,
    get_table_dashboard_questions,
    get_table_date_range,
    get_table_lineage_info,
    get_table_owners,
    get_table_schedule,
    get_table_storage_location,
    search_tables,
)

# Sample data for mocking
MOCK_TABLE_DATA = {"description": "A sample table."}
MOCK_PARSED_DESCRIPTIONS = {"schedule": "daily"}
MOCK_DASHBOARD_DATA = {"dashboards": []}
MOCK_SEARCH_DATA = {"results": []}
MOCK_QUESTIONS_DATA = [{"name": "Question 1", "url": "http://local.test"}]
MOCK_QUESTION_DETAIL = {"query_text": "SELECT * FROM table"}


@patch("mcp_amundsen.main.metadata_processor")
@patch("mcp_amundsen.main.table_client")
def test_get_table_columns(mock_table_client, mock_metadata_processor):
    mock_table_client.get_metadata.return_value = MOCK_TABLE_DATA
    mock_metadata_processor.extract_columns.return_value = {"columns": ["col1"]}
    result = get_table_columns("db", "schema", "table")
    mock_table_client.get_metadata.assert_called_once_with("db", "schema", "table")
    mock_metadata_processor.extract_columns.assert_called_once_with(MOCK_TABLE_DATA)
    assert result == {"columns": ["col1"]}


@patch("mcp_amundsen.main.metadata_processor")
@patch("mcp_amundsen.main.table_client")
def test_get_table_date_range(mock_table_client, mock_metadata_processor):
    mock_table_client.get_metadata.return_value = MOCK_TABLE_DATA
    mock_metadata_processor.extract_date_range.return_value = {"start": "2023-01-01"}
    result = get_table_date_range("db", "schema", "table")
    mock_table_client.get_metadata.assert_called_once_with("db", "schema", "table")
    mock_metadata_processor.extract_date_range.assert_called_once_with(MOCK_TABLE_DATA)
    assert result == {"start": "2023-01-01"}


@patch("mcp_amundsen.main.metadata_processor")
@patch("mcp_amundsen.main.table_client")
def test_get_table_code_url(mock_table_client, mock_metadata_processor):
    mock_table_client.get_metadata.return_value = MOCK_TABLE_DATA
    mock_metadata_processor.extract_code_location.return_value = "http://code.url"
    result = get_table_code_url("db", "schema", "table")
    mock_table_client.get_metadata.assert_called_once_with("db", "schema", "table")
    mock_metadata_processor.extract_code_location.assert_called_once_with(MOCK_TABLE_DATA)
    assert result == "http://code.url"


@patch("mcp_amundsen.main.metadata_processor")
@patch("mcp_amundsen.main.table_client")
def test_get_table_airflow_url(mock_table_client, mock_metadata_processor):
    mock_table_client.get_metadata.return_value = MOCK_TABLE_DATA
    mock_metadata_processor.extract_airflow_url.return_value = "http://airflow.url"
    result = get_table_airflow_url("db", "schema", "table")
    mock_table_client.get_metadata.assert_called_once_with("db", "schema", "table")
    mock_metadata_processor.extract_airflow_url.assert_called_once_with(MOCK_TABLE_DATA)
    assert result == "http://airflow.url"


@patch("mcp_amundsen.main.metadata_processor")
@patch("mcp_amundsen.main.table_client")
def test_get_table_schedule(mock_table_client, mock_metadata_processor):
    mock_table_client.get_metadata.return_value = MOCK_TABLE_DATA
    mock_metadata_processor.parse_programmatic_descriptions.return_value = MOCK_PARSED_DESCRIPTIONS
    mock_metadata_processor.extract_table_schedule.return_value = "daily"
    result = get_table_schedule("db", "schema", "table")
    mock_table_client.get_metadata.assert_called_once_with("db", "schema", "table")
    mock_metadata_processor.parse_programmatic_descriptions.assert_called_once_with(MOCK_TABLE_DATA)
    mock_metadata_processor.extract_table_schedule.assert_called_once_with(MOCK_PARSED_DESCRIPTIONS)
    assert result == "daily"


@patch("mcp_amundsen.main.metadata_processor")
@patch("mcp_amundsen.main.table_client")
def test_get_table_storage_location(mock_table_client, mock_metadata_processor):
    mock_table_client.get_metadata.return_value = MOCK_TABLE_DATA
    mock_metadata_processor.parse_programmatic_descriptions.return_value = MOCK_PARSED_DESCRIPTIONS
    mock_metadata_processor.extract_table_storage_location.return_value = "/path/to/storage"
    result = get_table_storage_location("db", "schema", "table")
    mock_table_client.get_metadata.assert_called_once_with("db", "schema", "table")
    mock_metadata_processor.parse_programmatic_descriptions.assert_called_once_with(MOCK_TABLE_DATA)
    mock_metadata_processor.extract_table_storage_location.assert_called_once_with(MOCK_PARSED_DESCRIPTIONS)
    assert result == "/path/to/storage"


@patch("mcp_amundsen.main.metadata_processor")
@patch("mcp_amundsen.main.table_client")
def test_get_table_owners(mock_table_client, mock_metadata_processor):
    mock_table_client.get_metadata.return_value = MOCK_TABLE_DATA
    mock_metadata_processor.extract_owners.return_value = ["owner1"]
    result = get_table_owners("db", "schema", "table")
    mock_table_client.get_metadata.assert_called_once_with("db", "schema", "table")
    mock_metadata_processor.extract_owners.assert_called_once_with(MOCK_TABLE_DATA)
    assert result == ["owner1"]


@patch("mcp_amundsen.main.table_client")
def test_get_table_lineage_info(mock_table_client):
    mock_table_client.get_lineage.return_value = {"lineage": []}
    result = get_table_lineage_info("db", "schema", "table")
    mock_table_client.get_lineage.assert_called_once_with("db", "schema", "table", 10, "both")
    assert result == {"lineage": []}


@patch("mcp_amundsen.main.metadata_processor")
@patch("mcp_amundsen.main.table_client")
def test_get_table_dashboard_info(mock_table_client, mock_metadata_processor):
    mock_table_client.get_dashboards.return_value = MOCK_DASHBOARD_DATA
    mock_metadata_processor.extract_dashboards.return_value = [{"name": "dash"}]
    result = get_table_dashboard_info("db", "schema", "table")
    mock_table_client.get_dashboards.assert_called_once_with("db", "schema", "table")
    mock_metadata_processor.extract_dashboards.assert_called_once_with(MOCK_DASHBOARD_DATA)
    assert result == [{"name": "dash"}]


@patch("mcp_amundsen.main.table_client")
def test_get_table_dashboard_questions(mock_table_client):
    mock_table_client.questions.return_value = MOCK_QUESTIONS_DATA
    result = get_table_dashboard_questions("db", "schema", "table", "dash")
    mock_table_client.questions.assert_called_once_with("db", "schema", "table", "dash", 10)
    assert result == MOCK_QUESTIONS_DATA


@patch("mcp_amundsen.main.table_client")
def test_get_table_dashboard_question_query(mock_table_client):
    mock_table_client.question_detail.return_value = MOCK_QUESTION_DETAIL
    result = get_table_dashboard_question_query("db", "schema", "table", "dash", "question")
    mock_table_client.question_detail.assert_called_once_with("db", "schema", "table", "dash", "question")
    assert result == {"result": {"query": "SELECT * FROM table"}}


@patch("mcp_amundsen.main.search_processor")
@patch("mcp_amundsen.main.search_client")
def test_search_tables(mock_search_client, mock_search_processor):
    mock_search_client.get_search_data.return_value = MOCK_SEARCH_DATA
    mock_search_processor.extract_search_data.return_value = {"tables": []}
    result = search_tables("term")
    mock_search_client.get_search_data.assert_called_once_with("term", ["hive"], [], [], [], "OR", [], {}, 10)
    mock_search_processor.extract_search_data.assert_called_once_with(MOCK_SEARCH_DATA)
    assert result == {"tables": []}
