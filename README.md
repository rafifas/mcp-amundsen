# amundsen-mcp

An [MCP](https://modelcontextprotocol.io/) server that exposes [Amundsen](https://www.amundsen.io/) data discovery capabilities as LLM-callable tools. Connect your AI assistant (Claude, Cursor, Cline, etc.) to your Amundsen instance and let it query table metadata, lineage, dashboards, and search results directly.

## Prerequisites

- Python 3.10+
- A running [Amundsen](https://github.com/amundsen-io/amundsen) instance (metadata service + search service)
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Quick Start

### Local (without Docker)

1. Clone the repository:
   ```bash
   git clone https://github.com/rafifas/mcp-amundsen.git
   cd amundsen-mcp
   ```

2. Install dependencies:
   ```bash
   uv pip install -r requirements.txt
   ```

3. Configure environment variables (copy `.env.example` and edit):
   ```bash
   cp .env.example .env
   # Edit .env with your Amundsen URLs
   ```

4. Start the server:
   ```bash
   uvicorn run:app --host 0.0.0.0 --port 8000
   ```

The server will be accessible at `http://localhost:8000/mcp`.

### Docker

1. Build the image:
   ```bash
   docker build -t mcp-amundsen .
   ```

2. Run with Docker Compose (edit `docker-compose.yml` with your Amundsen URLs first):
   ```bash
   docker-compose up
   ```

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `AMUNDSEN_API_URL` | `http://localhost:5000` | Amundsen metadata service URL |
| `AMUNDSEN_SEARCH_API_URL` | `http://localhost:5001` | Amundsen search service URL |
| `FASTMCP_HOST` | `127.0.0.1` | MCP server bind host (use `0.0.0.0` for Docker) |
| `FASTMCP_PORT` | `8000` | MCP server port |

## MCP Client Setup

After the server is running, add it to your MCP client configuration:

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "amundsen": {
      "type": "streamableHttp",
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

**Cline** (`cline_mcp_settings.json`):
```json
{
  "mcpServers": {
    "amundsen-mcp": {
      "disabled": false,
      "type": "streamableHttp",
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

**Cursor**: Add via `Settings > MCP > Add Server` with URL `http://localhost:8000/mcp`.

## Available Tools

| Tool | Description |
|---|---|
| `get_table_columns` | Fetch column names, types, descriptions, and partition keys for a table |
| `get_table_date_range` | Get the available date range (watermarks) for a table |
| `get_table_code_url` | Get the source code URL for the pipeline that generates the table |
| `get_table_airflow_url` | Get the Airflow DAG URL for the table's pipeline |
| `get_table_schedule` | Get the table's update schedule |
| `get_table_storage_location` | Get the storage location (e.g. GCS path) of a table |
| `get_table_owners` | Get the list of table owners |
| `get_table_lineage_info` | Fetch upstream/downstream lineage for a table |
| `get_table_dashboard_info` | List dashboards that use data from a table |
| `get_table_dashboard_questions` | List questions/charts in a dashboard |
| `get_table_dashboard_question_query` | Get the SQL query behind a specific dashboard question |
| `search_tables` | Search for tables by keyword, schema, columns, or tags |

## Development

Install dev dependencies:
```bash
uv pip install -e ".[dev]"
```

Run tests:
```bash
pytest
```

Run linter:
```bash
ruff check .
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

MIT — see [LICENSE](LICENSE).
