# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

This is a modern data lakehouse setup using Docker Compose with the following components:

- **MinIO**: S3-compatible object storage for data lake storage (ports 9000, 9001)
- **PostgreSQL**: Database backend for Hive Metastore (port 5433)
- **Hive Metastore**: Metadata catalog service using Apache Hive with PostgreSQL backend (port 9083)  
- **Trino**: Distributed SQL query engine for analytics (port 8080)
- **CloudBeaver**: Web-based database administration interface (port 8978)
- **Iceberg**: Table format for large analytic datasets, configured to use Hive catalog and MinIO storage

The stack follows the lakehouse pattern where:
1. MinIO provides the storage layer (replaces HDFS/S3)
2. PostgreSQL stores Hive Metastore metadata persistently
3. Hive Metastore manages table metadata 
4. Trino provides the compute layer for SQL analytics
5. CloudBeaver offers web-based database administration and query interface
6. Iceberg handles table format and ACID transactions

## Environment Setup

### Initial Configuration
1. **Copy environment template:**
   ```bash
   cp .env.example .env
   ```

2. **Update environment variables:**
   - Edit `.env` with your secrets and configuration
   - Single file contains all infrastructure and application settings

### Environment File Structure
The project uses a **single `.env` file** approach for simplicity:

- **Root `.env`**: Contains ALL environment variables
  - Infrastructure secrets (PostgreSQL, MinIO, CloudBeaver credentials)
  - Application settings (feature flags, processing settings)
  - Port configurations and service endpoints
  - Email/notification settings
  - Both docker-compose and Python scripts read from this file

### Security Best Practices
- **Never commit `.env` files** - they contain secrets and credentials
- **Use strong passwords** - replace default passwords in production
- **Rotate credentials regularly** - especially for production environments
- **Use different credentials** for different environments (dev/staging/prod)
- **Review `.env.example` file** - it should never contain real secrets
- **Single source of truth** - all services read from the same .env file, eliminating sync issues

## Common Commands

### Starting the Environment
```bash
docker-compose up -d
```

### Stopping the Environment
```bash
docker-compose down
```

### Viewing Logs
```bash
docker-compose logs -f [service-name]
```

### Accessing Services
- MinIO Console: http://localhost:9001 (credentials in `.env`)
- Trino Web UI: http://localhost:8080
- CloudBeaver: http://localhost:8978 (credentials in `.env`)
- MinIO API: http://localhost:9000
- PostgreSQL: localhost:5433 (credentials in `.env`)

### MinIO Client Operations
The MinIO client is automatically configured via the init script. The warehouse bucket is created automatically on startup.

## Configuration Files

- `docker-compose.yml`: Main orchestration file defining all services and their dependencies
- `init/mc.sh`: MinIO initialization script that creates the warehouse bucket
- `trino/catalog/iceberg.properties`: Trino catalog configuration for Iceberg tables with MinIO backend

## Service Dependencies

Services start in this order due to health checks:
1. PostgreSQL - database backend for Hive Metastore
2. MinIO (with health check) - object storage
3. MinIO Client (mc) - creates warehouse bucket  
4. Hive Metastore - metadata catalog service
5. Trino - SQL query engine
6. CloudBeaver - web administration interface

## CloudBeaver Configuration

CloudBeaver provides a web-based interface for database administration and reduces operational costs by:
- Eliminating need for multiple database clients
- Providing visual SQL editor and data browser
- Supporting collaborative query development
- Offering built-in query performance analysis

### Initial Setup
1. Access CloudBeaver at http://localhost:8978
2. Login with credentials from `.env` file
3. Configure data source connections:
   - **Trino**: `jdbc:trino://trino:8080/iceberg` (for Iceberg tables)
   - **PostgreSQL**: `jdbc:postgresql://postgres:5432/metastore` (for metadata)

## Data Storage

- MinIO data: `./minio-data` local directory mounted to `/data` (allows direct file observation)
- PostgreSQL data: `postgres-data` volume for Hive Metastore backend
- Hive Metastore: `hive-warehouse` volume for temporary data
- CloudBeaver data: `cloudbeaver-data` volume for workspace and configurations
- Warehouse data: Stored in MinIO at `s3://warehouse/` (accessible at `./minio-data/warehouse/`)