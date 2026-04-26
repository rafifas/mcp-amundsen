class AmundsenError(Exception):
    """Base exception for Amundsen client errors."""

    pass


class AmundsenApiError(AmundsenError):
    """Raised for API-related errors."""

    def __init__(self, message: str, status_code: int = None):
        self.status_code = status_code
        super().__init__(f"API Error: {message}")


class TableNotFoundError(AmundsenError):
    """Raised when a table is not found."""

    def __init__(self, table_name: str):
        super().__init__(f"Table '{table_name}' not found.")
