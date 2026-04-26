from dotenv import load_dotenv

load_dotenv()

from mcp_amundsen.main import mcp

app = mcp.streamable_http_app()
