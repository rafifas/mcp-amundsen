# Contributing

## Development Setup

1. Clone the repo and install dev dependencies:
   ```bash
   git clone https://github.com/rafifas/mcp-amundsen.git
   cd amundsen-mcp
   uv pip install -e ".[dev]"
   ```

2. Copy `.env.example` to `.env` and set your Amundsen URLs:
   ```bash
   cp .env.example .env
   ```

## Running Tests

```bash
pytest
```

## Linting

```bash
ruff check .
```

## Making Changes

- Branch off `master`: `git checkout -b your-feature`
- Write tests for new behavior
- Ensure `pytest` and `ruff check .` both pass before opening a PR
- Open a pull request against `master`

## Amundsen API Reference

This project wraps the [Amundsen metadata service](https://github.com/amundsen-io/amundsen/tree/main/metadata) and [search service](https://github.com/amundsen-io/amundsen/tree/main/search) APIs. Understanding those APIs is useful when adding new tools.
