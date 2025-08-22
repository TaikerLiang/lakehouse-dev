# Data Lakehouse

A modern data lakehouse setup using Docker Compose with MinIO, Hive Metastore, and Trino for analytics.

## Architecture

This project provides a complete data lakehouse infrastructure with:

- **MinIO**: S3-compatible object storage for data lake storage
- **PostgreSQL**: Metadata database for Hive Metastore
- **Hive Metastore**: Metadata catalog service for table management
- **Trino**: Distributed SQL query engine for analytics
- **Apache Iceberg**: Table format for large analytic datasets with ACID transactions

## Quick Start

### Prerequisites

- Docker
- Docker Compose

### Starting the Services

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f [service-name]
```

### Stopping the Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## Service Access

### MinIO Console
- **URL**: http://localhost:9001
- **Username**: `minio`
- **Password**: `minio123`

Use the MinIO Console to:
- Browse and manage S3 buckets
- Upload/download files
- Monitor storage usage

### Trino Web UI
- **URL**: http://localhost:8080
- **Authentication**: None required

The Trino Web UI provides:
- Query execution interface
- Performance monitoring
- Cluster status information

### MinIO S3 API
- **Endpoint**: http://localhost:9000
- **Access Key**: `minio`
- **Secret Key**: `minio123`

## Running SQL Queries

You can connect to Trino using any SQL client that supports JDBC/ODBC:

- **JDBC URL**: `jdbc:trino://localhost:8080`
- **Catalog**: `iceberg`
- **Schema**: Available schemas in your Hive Metastore

Example using Trino CLI:
```bash
docker exec -it trino trino --server localhost:8080 --catalog iceberg
```

## Data Storage

- **Object Storage**: MinIO at `s3://warehouse/`
- **Metadata**: PostgreSQL database
- **Table Format**: Apache Iceberg for ACID transactions and schema evolution

## Development

See [CLAUDE.md](./CLAUDE.md) for detailed development guidance when using Claude Code.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.