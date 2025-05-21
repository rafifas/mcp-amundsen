import urllib.request
import urllib.error
import json
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from mcp.server.fastmcp import FastMCP

# Initialize MCP server
mcp = FastMCP("Amundsen")

# Base API client class
class AmundsenApiClient:
    """Base class for Amundsen API interactions."""
    
    BASE_URL = os.environ.get("AMUNDSEN_API_URL", "http://localhost:5000")
    
    @staticmethod
    def make_request(url: str) -> Dict[str, Any]:
        """
        Makes an HTTP request to the specified URL.
        
        Args:
            url (str): The URL to make the request to
            
        Returns:
            dict: The parsed JSON response
        """
        print(f"Requesting: {url}")
        
        try:
            with urllib.request.urlopen(url) as response:
                response_data = response.read().decode('utf-8')
                return json.loads(response_data)
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            return {"error": str(e)}


# Table API client
class TableApiClient(AmundsenApiClient):
    """Client for interacting with Amundsen Table API endpoints."""
    
    def get_table_url(self, database: str, schema_name: str, table_name: str) -> str:
        """
        Constructs the base table URL.
        
        Args:
            database (str): The database name
            schema_name (str): The schema name
            table_name (str): The table name
            
        Returns:
            str: The base table URL
        """
        return f"{self.BASE_URL}/table/{database}://gold.{schema_name}/{table_name}"
    
    def get_metadata(self, database: str, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Fetches metadata for a specific table.
        
        Args:
            database (str): The database name
            schema_name (str): The schema name
            table_name (str): The table name
            
        Returns:
            dict: The table metadata
        """
        url = self.get_table_url(database, schema_name, table_name)
        return self.make_request(url)
    
    def get_lineage(self, database: str, schema_name: str, table_name: str, 
                   depth: int = 1, direction: str = "both") -> Dict[str, Any]:
        """
        Fetches lineage information for a specific table.
        
        Args:
            database (str): The database name
            schema_name (str): The schema name
            table_name (str): The table name
            depth (int): The number of levels to traverse
            direction (str): The direction of lineage to fetch
            
        Returns:
            dict: The table lineage information
        """
        # Validate direction parameter
        if direction not in ["both", "upstream", "downstream"]:
            return {"error": "Invalid direction. Must be one of: both, upstream, downstream"}
        
        # Construct the API URL with query parameters
        base_url = self.get_table_url(database, schema_name, table_name)
        url = f"{base_url}/lineage?depth={depth}&direction={direction}"
        response = self.make_request(url)
        upstream = response.get("upstream_entities")
        downstream = response.get("downstream_entities")

        lineage = {"upstream": upstream, "downstream": downstream}

        
        if upstream or downstream:
            return lineage
        else:
            return "Table has no lineage"

    def get_tables_dashboard(self, database: str, schema_name:str, table_name:str):
        """
        Fetches list of dashboard created from a specific table.
        
        Args:
            database (str): The database name
            schema_name (str): The schema name
            table_name (str): The table name
            
        Returns:
            list: List of dashboard generated from table
        """
        base_url = self.get_table_url(database, schema_name, table_name)
        url = f"{base_url}/dashboard/"
        # print(url)
        response = self.make_request(url)
        raw_dashboards = response.get("dashboards")
        dashboards = []
        

        for raw_dashboard in raw_dashboards:
            dashboard_url = raw_dashboard.get("url")
            dashboard_name = raw_dashboard.get("name")
            collection_name = raw_dashboard.get("group_name")
            dashboards.append({"url": dashboard_url, "dashboard_name": dashboard_name, "collection_name": collection_name})
        
        if len(dashboards) > 0:
            return dashboards
        else:
            return "table has no dashboard"

# Metadata processor
class MetadataProcessor:
    @staticmethod
    def extract_columns(table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "error" in table_data:
            return {"error": table_data["error"]}
        
        # Initialize the simplified metadata structure
        column_metadata = {
            "columns": [],
            "partition_keys": []
        }
        
        # Extract column information
        for column in table_data.get("columns", []):
            column_info = {
                "name": column.get("name"),
                "type": column.get("col_type"),
                "description": column.get("description")
            }
            
            # Add to columns list
            column_metadata["columns"].append(column_info)
            
            # Check if this is a partition column
            for badge in column.get("badges", []):
                if badge.get("badge_name") == "partition column":
                    column_metadata["partition_keys"].append(column_info)
        
        return column_metadata

    @staticmethod
    def extract_date_range(table_data: Dict[str, Any]) -> Dict[str, Any]:
        # Check if there was an error
        if "error" in table_data:
            return {"error": table_data["error"]}
        
        date_range = {"from": None, "to": None}
        
        # Extract watermark information
        for watermark in table_data.get("watermarks", []):
            if watermark.get("watermark_type") == "low_watermark":
                date_range["from"] = {
                    "key": watermark.get("partition_key"),
                    "value": watermark.get("partition_value")
                }
            elif watermark.get("watermark_type") == "high_watermark":
                date_range["to"] = {
                    "key": watermark.get("partition_key"),
                    "value": watermark.get("partition_value")
                }
            
        if date_range["from"] or date_range["to"]:      
            return date_range
        else:
            return "table has no date range"
    
    @staticmethod
    def extract_owners(table_data: Dict[str, Any]) -> Dict[str, Any]:
        # Check if there was an error
        if "error" in table_data:
            return {"error": table_data["error"]}
        
        owners = []
        
        # Extract watermark information
        for owner in table_data.get("owners", []):
            user_id = owner.get("user_id")
            owners.append(user_id)
        
        if len(owners) > 0:
            return owners
        else:
            return "the table has no owners"

# Initialize clients
table_client = TableApiClient()
metadata_processor = MetadataProcessor()


@mcp.tool()
def get_table_columns(database: str, schema_name: str, table_name: str) -> Dict[str, Any]:
    """
    Fetches table columns for a specific table and extracts simplified information.
    
    Args:
        database (str): The database name
        schema_name (str): The schema name
        table_name (str): The table name
    
    Returns:
        dict: A dictionary containing simplified metadata information:
             - column names, types and desc
             - partition keys
    """
    # Get the full metadata
    table_data = table_client.get_metadata(database, schema_name, table_name)
    
    # Process and return the simplified metadata
    return metadata_processor.extract_columns(table_data)


@mcp.tool()
def get_table_date_range(database: str, schema_name: str, table_name: str) -> Dict[str, Any]:
    """
    Fetches table date range availables.
    
    Args:
        database (str): The database name
        schema_name (str): The schema name
        table_name (str): The table name
    
    Returns:
        dict: A dictionary containing date range:
             - date of earliest data and its date column name
             - date of latest data and its date column name
    """
    # Get the full metadata
    table_data = table_client.get_metadata(database, schema_name, table_name)
    
    # Process and return the simplified metadata
    return metadata_processor.extract_date_range(table_data)

@mcp.tool()
def get_table_owners(database: str, schema_name: str, table_name: str) -> Dict[str, Any]:
    """
    Fetches table owners.
    
    Args:
        database (str): The database name
        schema_name (str): The schema name
        table_name (str): The table name
    
    Returns:
        list: list of owners of the table
    """
    # Get the full metadata
    table_data = table_client.get_metadata(database, schema_name, table_name)
    
    # Process and return the simplified metadata
    return metadata_processor.extract_owners(table_data)

@mcp.tool()
def get_table_lineage_info(database: str, schema_name: str, table_name: str, 
                          depth: int = 1, direction: str = "both") -> Dict[str, Any]:
    """
    Fetches lineage information for a specific table.
    
    Args:
        database (str): The database name
        schema_name (str): The schema name
        table_name (str): The table name
        depth (int): The number of levels to traverse (default: 1)
        direction (str): The direction of lineage to fetch (options: "both", "upstream", "downstream", default: "both")
    
    Returns:
        dict: A dictionary containing table lineage information:
             - upstream tables (sources)
             - downstream tables (targets)
             - lineage graph structure
    """
    # Get the lineage data
    return table_client.get_lineage(database, schema_name, table_name, depth, direction)

@mcp.tool()
def get_table_dasboard_info(database: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
    """
    Fetches dashboard created from specific table.
    
    Args:
        database (str): The database name
        schema_name (str): The schema name
        table_name (str): The table name
    
    Returns:
        list: list of dictionary containing dashboards created from table:
             - url
             - dashboard_name
             - collection_name
    """
    
    return table_client.get_tables_dashboard(database, schema_name, table_name)

if __name__ == '__main__':
    mcp.run(transport='stdio')
