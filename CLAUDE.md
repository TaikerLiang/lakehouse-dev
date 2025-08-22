# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

This is a modern data lakehouse setup using Docker Compose with the following components:

- **MinIO**: S3-compatible object storage for data lake storage (ports 9000, 9001)
- **Hive Metastore**: Metadata catalog service using Apache Hive with Derby database (port 9083)  
- **Trino**: Distributed SQL query engine for analytics (port 8080)
- **Iceberg**: Table format for large analytic datasets, configured to use Hive catalog and MinIO storage

The stack follows the lakehouse pattern where:
1. MinIO provides the storage layer (replaces HDFS/S3)
2. Hive Metastore manages table metadata 
3. Trino provides the compute layer for SQL analytics
4. Iceberg handles table format and ACID transactions

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
- MinIO Console: http://localhost:9001 (minio/minio123)
- Trino Web UI: http://localhost:8080
- MinIO API: http://localhost:9000

### MinIO Client Operations
The MinIO client is automatically configured via the init script. The warehouse bucket is created automatically on startup.

## Configuration Files

- `docker-compose.yml`: Main orchestration file defining all services and their dependencies
- `init/mc.sh`: MinIO initialization script that creates the warehouse bucket
- `trino/catalog/iceberg.properties`: Trino catalog configuration for Iceberg tables with MinIO backend

## Service Dependencies

Services start in this order due to health checks:
1. MinIO (with health check)
2. MinIO Client (mc) - creates warehouse bucket  
3. Hive Metastore (with health check)
4. Trino

## Data Storage

- MinIO data: `minio-data` volume mapped to `/data`
- Hive Metastore: `hive-metastore-data` volume for Derby database
- Warehouse data: Stored in MinIO at `s3://warehouse/`