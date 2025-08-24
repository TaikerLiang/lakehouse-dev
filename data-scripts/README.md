# Lakehouse Data Scripts

Python scripts and utilities for managing data ingestion and processing in the lakehouse environment.

## Overview

This package provides tools for:
- Data ingestion from various sources (CSV, JSON, Parquet)
- Table creation and management in Trino/Iceberg
- Connection management for multiple services (Trino, PostgreSQL, Redis, MinIO)
- Data validation and quality checks
- Pipeline automation and monitoring

## Installation

### Prerequisites
- Python 3.9+
- Poetry for dependency management
- Running lakehouse services (Trino, PostgreSQL, MinIO, Redis)

### Setup

1. **Install Poetry** (if not already installed):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Install dependencies**:
   ```bash
   poetry install
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your service configurations
   ```

4. **Activate the virtual environment**:
   ```bash
   poetry shell
   ```

## Configuration

All configuration is managed through environment variables defined in `.env`:

- **Trino**: Connection to the query engine
- **PostgreSQL**: Metadata storage and logging
- **Redis**: Caching and temporary data
- **MinIO**: Object storage
- **Feature Flags**: Toggle functionality like email alerts, validation
- **Processing**: Batch sizes, timeouts, retry logic

See `.env.example` for all available options.

## Usage

### Basic Client Usage

```python
from utils.clients import clients
from settings import settings

# Execute Trino queries
results = clients.trino.execute_query("SHOW TABLES")

# Upload files to MinIO
clients.minio.upload_file("data.csv", "raw/data.csv")

# Cache data in Redis
clients.redis.set("pipeline_status", "running", expire=300)

# Log to PostgreSQL
clients.postgres.execute_insert(
    "INSERT INTO logs (message, timestamp) VALUES (%s, NOW())",
    ("Pipeline completed",)
)

# Health check all services
health = clients.health_check()
print(health)  # {'trino': True, 'postgres': True, 'redis': True, 'minio': True}
```

### Using Context Manager for Auto-Cleanup

```python
from utils.clients import ClientContext

with ClientContext() as clients:
    # Use clients normally
    results = clients.trino.execute_query("SELECT * FROM my_table")
    # All connections automatically closed when exiting
```

## Development

### Code Quality Tools

This project uses several tools to maintain code quality:

- **Black**: Code formatting
- **Ruff**: Linting and import sorting  
- **MyPy**: Type checking
- **Pytest**: Testing framework

Run all quality checks:
```bash
# Format code
poetry run black .

# Lint code
poetry run ruff check .

# Type check
poetry run mypy .

# Run tests
poetry run pytest
```

### Project Structure

```
data-scripts/
├── pyproject.toml          # Poetry configuration
├── settings.py             # Application configuration
├── .env.example           # Environment variables template
├── utils/                 # Utility modules
│   ├── __init__.py
│   └── clients.py         # Database/service clients
├── examples/              # Example scripts (coming soon)
├── ingestion/            # Data ingestion modules (coming soon)
└── tests/                # Test files (coming soon)
```

## Features

### Settings Management
- Type-safe configuration with Pydantic
- Environment-based configuration
- Feature flags for toggling functionality
- Helper methods for common operations

### Client Management
- Unified interface for all external services
- Lazy initialization and connection pooling
- Comprehensive error handling and logging
- Health check functionality
- Automatic connection cleanup

### Supported Services
- **Trino**: SQL query execution, table management
- **PostgreSQL**: Metadata storage, logging
- **Redis**: Caching, temporary storage
- **MinIO**: Object storage operations

## CLI Commands

The package includes CLI commands for common operations:

```bash
# Check service health
poetry run lakehouse-health

# Run data ingestion (coming soon)
poetry run lakehouse-ingest --source=csv --target=my_table
```

## Contributing

1. Follow the existing code style (Black formatting)
2. Add type hints for all functions
3. Write tests for new functionality
4. Update documentation as needed

## License

This project is licensed under the MIT License - see the main project LICENSE file for details.