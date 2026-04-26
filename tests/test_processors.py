import pytest
from mcp_amundsen.processors import MetadataProcessor, SearchProcessor

# Mock data for testing
mock_table_data = {
    "columns": [
        {"name": "col1", "col_type": "string", "description": "description1", "badges": []},
        {"name": "col2", "col_type": "int", "description": "description2", "badges": [{"badge_name": "partition column"}]},
    ],
    "watermarks": [
        {"watermark_type": "low_watermark", "partition_key": "ds", "partition_value": "2023-01-01"},
        {"watermark_type": "high_watermark", "partition_key": "ds", "partition_value": "2023-01-31"},
    ],
    "owners": [
        {"user_id": "user1"},
        {"user_id": "user2"},
    ],
}

mock_dashboard_data = {
    "dashboards": [
        {"url": "http://dashboard1.com", "name": "Dashboard 1", "group_name": "Group 1"},
        {"url": "http://dashboard2.com", "name": "Dashboard 2", "group_name": "Group 2"},
    ]
}

def test_extract_columns():
    processor = MetadataProcessor()
    result = processor.extract_columns(mock_table_data)
    assert len(result["columns"]) == 2
    assert result["columns"][0]["name"] == "col1"
    assert len(result["partition_keys"]) == 1
    assert result["partition_keys"][0]["name"] == "col2"

def test_extract_date_range():
    processor = MetadataProcessor()
    result = processor.extract_date_range(mock_table_data)
    assert result["from"]["value"] == "2023-01-01"
    assert result["to"]["value"] == "2023-01-31"

def test_extract_owners():
    processor = MetadataProcessor()
    result = processor.extract_owners(mock_table_data)
    assert len(result) == 2
    assert "user1" in result

def test_extract_dashboards():
    processor = MetadataProcessor()
    result = processor.extract_dashboards(mock_dashboard_data)
    assert len(result) == 2
    assert result[0]["dashboard_name"] == "Dashboard 1"

def test_extract_columns_error():
    processor = MetadataProcessor()
    result = processor.extract_columns({"error": "Not found"})
    assert result["error"] == "Not found"

def test_extract_date_range_no_range():
    processor = MetadataProcessor()
    result = processor.extract_date_range({})
    assert result == {'from': None, 'to': None}

def test_extract_owners_no_owners():
    processor = MetadataProcessor()
    result = processor.extract_owners({})
    assert result == []

def test_extract_dashboards_no_dashboards():
    processor = MetadataProcessor()
    result = processor.extract_dashboards({"dashboards": []})
    assert result == []

# Mock data for search tests
mock_search_data = [
    {
        "database": "db1",
        "schema": "schema1",
        "table": "table1",
        "description": "description1",
    },
    {
        "database": "db2",
        "schema": "schema2",
        "table": "table2",
        "description": "description2",
    },
]

def test_extract_search_data():
    processor = SearchProcessor()
    result = processor.extract_search_data(mock_search_data)
    assert result["total_results"] == 2
    assert len(result["table_results"]) == 2
    assert result["table_results"][0]["table"] == "table1"

def test_extract_search_data_empty():
    processor = SearchProcessor()
    result = processor.extract_search_data([])
    assert result["total_results"] == 0
    assert len(result["table_results"]) == 0

def test_extract_code_location():
    processor = MetadataProcessor()
    mock_data = {"source": {"source": "http://gitlab.com/repo"}}
    result = processor.extract_code_location(mock_data)
    assert result == "http://gitlab.com/repo"

def test_extract_airflow_url():
    processor = MetadataProcessor()
    mock_data = {"table_writer": {"application_url": "http://airflow.com/dag"}}
    result = processor.extract_airflow_url(mock_data)
    assert result == "http://airflow.com/dag"

def test_parse_programmatic_descriptions():
    processor = MetadataProcessor()
    mock_data = {
        "programmatic_descriptions": [{"text": "***Schedule***: Daily\n***GCS Path***: gs://bucket/path"}],
        "schema": "test_schema",
        "name": "test_table"
    }
    result = processor.parse_programmatic_descriptions(mock_data)
    assert result["Schedule"] == "Daily"
    assert result["GCS Path"] == "gs://bucket/path"
    assert result["schema"] == "test_schema"
    assert result["table"] == "test_table"

def test_extract_table_schedule():
    processor = MetadataProcessor()
    mock_data = {"Schedule": "Daily", "schema": "other"}
    result = processor.extract_table_schedule(mock_data)
    assert result == "Daily"

def test_extract_table_schedule_empty():
    processor = MetadataProcessor()
    mock_data = {"Schedule": "", "schema": "dayshift"}
    result = processor.extract_table_schedule(mock_data)
    assert result == ""

def test_extract_table_storage_location():
    processor = MetadataProcessor()
    mock_data = {"GCS Path": "gs://bucket/path"}
    result = processor.extract_table_storage_location(mock_data)
    assert result == "gs://bucket/path"
