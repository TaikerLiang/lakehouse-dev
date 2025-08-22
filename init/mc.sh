#!/bin/sh
set -e

echo "⏳ Waiting for MinIO to be ready..."

# Configure MinIO client
mc alias set local http://minio:9000 minio minio123

# Create warehouse bucket if it doesn't exist
if ! mc ls local/warehouse >/dev/null 2>&1; then
  echo "📦 Creating bucket 'warehouse'..."
  mc mb -p local/warehouse
else
  echo "✅ Bucket 'warehouse' already exists"
fi
