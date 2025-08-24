"""
Client utilities for connecting to external services
All clients are managed through a single ClientManager for easy access
"""
import logging
from typing import Optional, List, Dict, Any, Union
import sys
import os

# Add the parent directory to the path to import settings
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from settings import settings

# Import external libraries with proper error handling
try:
    import trino
except ImportError:
    trino = None
    logging.warning("Trino library not installed. Trino functionality will be unavailable.")

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None
    logging.warning("psycopg2 library not installed. PostgreSQL functionality will be unavailable.")

try:
    import redis
except ImportError:
    redis = None
    logging.warning("redis library not installed. Redis functionality will be unavailable.")

try:
    from minio import Minio
except ImportError:
    Minio = None
    logging.warning("minio library not installed. MinIO functionality will be unavailable.")

logger = logging.getLogger(__name__)

class TrinoClient:
    """Trino database client for executing SQL queries on the lakehouse"""
    
    def __init__(self):
        if trino is None:
            raise ImportError("Trino library not installed. Run: pip install trino")
        
        self._conn: Optional[trino.dbapi.Connection] = None
        logger.info(f"Initializing Trino client for {settings.trino_host}:{settings.trino_port}")
    
    @property
    def conn(self):
        """Lazy connection to Trino"""
        if self._conn is None:
            try:
                self._conn = trino.dbapi.connect(
                    host=settings.trino_host,
                    port=settings.trino_port,
                    user=settings.trino_user or 'default',
                    catalog=settings.trino_catalog,
                    schema=settings.trino_schema
                )
                logger.info("Successfully connected to Trino")
            except Exception as e:
                logger.error(f"Failed to connect to Trino: {e}")
                raise
        return self._conn
    
    def execute_query(self, query: str) -> List[tuple]:
        """Execute query and return results as list of tuples"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            logger.debug(f"Executed query successfully, returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def execute_with_description(self, query: str) -> Dict[str, Any]:
        """Execute query and return results with column descriptions"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            logger.debug(f"Executed query with description, returned {len(rows)} rows")
            return {"columns": columns, "data": rows}
        except Exception as e:
            logger.error(f"Query execution with description failed: {e}")
            raise
    
    def create_iceberg_table(self, table_name: str, columns: str, location: Optional[str] = None) -> None:
        """Create Iceberg table with specified schema"""
        location_clause = f", external_location = '{location}'" if location else ""
        query = f"""
        CREATE TABLE IF NOT EXISTS {settings.trino_catalog}.{settings.trino_schema}.{table_name} (
            {columns}
        ) WITH (
            format = 'PARQUET'
            {location_clause}
        )
        """
        self.execute_query(query)
        logger.info(f"Created Iceberg table: {table_name}")
    
    def show_tables(self) -> List[str]:
        """Get list of tables in current schema"""
        results = self.execute_query("SHOW TABLES")
        return [row[0] for row in results]
    
    def describe_table(self, table_name: str) -> List[Dict[str, str]]:
        """Describe table structure"""
        result = self.execute_with_description(f"DESCRIBE {table_name}")
        columns = result["columns"]
        data = result["data"]
        return [dict(zip(columns, row)) for row in data]
    
    def close(self):
        """Close Trino connection"""
        if self._conn:
            try:
                self._conn.close()
                self._conn = None
                logger.info("Trino connection closed")
            except Exception as e:
                logger.warning(f"Error closing Trino connection: {e}")

class PostgresClient:
    """PostgreSQL client for metadata operations and logging"""
    
    def __init__(self):
        if psycopg2 is None:
            raise ImportError("psycopg2 library not installed. Run: pip install psycopg2-binary")
        
        self._conn: Optional[psycopg2.extensions.connection] = None
        logger.info(f"Initializing PostgreSQL client for {settings.postgres_host}:{settings.postgres_port}")
    
    @property
    def conn(self):
        """Lazy connection to PostgreSQL"""
        if self._conn is None:
            try:
                self._conn = psycopg2.connect(
                    host=settings.postgres_host,
                    port=settings.postgres_port,
                    database=settings.postgres_db,
                    user=settings.postgres_user,
                    password=settings.postgres_password
                )
                self._conn.autocommit = False
                logger.info("Successfully connected to PostgreSQL")
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}")
                raise
        return self._conn
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """Execute SELECT query and return results"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            logger.debug(f"Executed PostgreSQL query, returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"PostgreSQL query execution failed: {e}")
            self.conn.rollback()
            raise
    
    def execute_insert(self, query: str, params: Optional[Union[tuple, List[tuple]]] = None) -> None:
        """Execute INSERT/UPDATE/DELETE query"""
        try:
            cursor = self.conn.cursor()
            if isinstance(params, list):
                # Batch insert
                cursor.executemany(query, params)
                logger.debug(f"Executed batch insert with {len(params)} rows")
            else:
                cursor.execute(query, params)
                logger.debug("Executed single insert/update/delete")
            
            self.conn.commit()
        except Exception as e:
            logger.error(f"PostgreSQL insert execution failed: {e}")
            self.conn.rollback()
            raise
    
    def close(self):
        """Close PostgreSQL connection"""
        if self._conn:
            try:
                self._conn.close()
                self._conn = None
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                logger.warning(f"Error closing PostgreSQL connection: {e}")

class RedisClient:
    """Redis client for caching and temporary data storage"""
    
    def __init__(self):
        if redis is None:
            raise ImportError("redis library not installed. Run: pip install redis")
        
        self._client: Optional[redis.Redis] = None
        logger.info(f"Initializing Redis client for {settings.redis_host}:{settings.redis_port}")
    
    @property
    def client(self):
        """Lazy connection to Redis"""
        if self._client is None:
            try:
                self._client = redis.Redis(
                    host=settings.redis_host,
                    port=settings.redis_port,
                    db=settings.redis_db,
                    password=settings.redis_password or None,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                # Test connection
                self._client.ping()
                logger.info("Successfully connected to Redis")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                raise
        return self._client
    
    def set(self, key: str, value: str, expire: Optional[int] = None) -> bool:
        """Set key-value with optional expiration in seconds"""
        try:
            result = self.client.set(key, value, ex=expire)
            logger.debug(f"Set Redis key: {key}, expire: {expire}")
            return result
        except Exception as e:
            logger.error(f"Failed to set Redis key {key}: {e}")
            raise
    
    def get(self, key: str) -> Optional[str]:
        """Get value by key"""
        try:
            value = self.client.get(key)
            logger.debug(f"Got Redis key: {key}, found: {value is not None}")
            return value
        except Exception as e:
            logger.error(f"Failed to get Redis key {key}: {e}")
            raise
    
    def delete(self, key: str) -> bool:
        """Delete key"""
        try:
            result = bool(self.client.delete(key))
            logger.debug(f"Deleted Redis key: {key}, success: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to delete Redis key {key}: {e}")
            raise
    
    def exists(self, key: str) -> bool:
        """Check if key exists"""
        try:
            result = bool(self.client.exists(key))
            logger.debug(f"Redis key exists: {key}, result: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to check Redis key {key}: {e}")
            raise

class MinIOClient:
    """MinIO/S3 client for object storage operations"""
    
    def __init__(self):
        if Minio is None:
            raise ImportError("minio library not installed. Run: pip install minio")
        
        self._client: Optional[Minio] = None
        logger.info(f"Initializing MinIO client for {settings.minio_endpoint}")
    
    @property 
    def client(self):
        """Lazy connection to MinIO"""
        if self._client is None:
            try:
                self._client = Minio(
                    settings.minio_endpoint,
                    access_key=settings.minio_access_key,
                    secret_key=settings.minio_secret_key,
                    secure=settings.minio_secure
                )
                # Test connection by checking if bucket exists
                bucket_exists = self._client.bucket_exists(settings.minio_bucket)
                logger.info(f"Successfully connected to MinIO. Bucket '{settings.minio_bucket}' exists: {bucket_exists}")
            except Exception as e:
                logger.error(f"Failed to connect to MinIO: {e}")
                raise
        return self._client
    
    def upload_file(self, file_path: str, object_name: str, bucket: Optional[str] = None) -> None:
        """Upload file to MinIO bucket"""
        bucket = bucket or settings.minio_bucket
        try:
            self.client.fput_object(bucket, object_name, file_path)
            logger.info(f"Uploaded file {file_path} to {bucket}/{object_name}")
        except Exception as e:
            logger.error(f"Failed to upload file {file_path}: {e}")
            raise
    
    def download_file(self, object_name: str, file_path: str, bucket: Optional[str] = None) -> None:
        """Download file from MinIO bucket"""
        bucket = bucket or settings.minio_bucket
        try:
            self.client.fget_object(bucket, object_name, file_path)
            logger.info(f"Downloaded {bucket}/{object_name} to {file_path}")
        except Exception as e:
            logger.error(f"Failed to download object {object_name}: {e}")
            raise
    
    def list_objects(self, prefix: str = "", bucket: Optional[str] = None) -> List[str]:
        """List objects in bucket with optional prefix"""
        bucket = bucket or settings.minio_bucket
        try:
            objects = self.client.list_objects(bucket, prefix=prefix)
            object_names = [obj.object_name for obj in objects]
            logger.debug(f"Listed {len(object_names)} objects in {bucket} with prefix '{prefix}'")
            return object_names
        except Exception as e:
            logger.error(f"Failed to list objects in bucket {bucket}: {e}")
            raise
    
    def delete_object(self, object_name: str, bucket: Optional[str] = None) -> None:
        """Delete object from MinIO bucket"""
        bucket = bucket or settings.minio_bucket
        try:
            self.client.remove_object(bucket, object_name)
            logger.info(f"Deleted object {bucket}/{object_name}")
        except Exception as e:
            logger.error(f"Failed to delete object {object_name}: {e}")
            raise
    
    def object_exists(self, object_name: str, bucket: Optional[str] = None) -> bool:
        """Check if object exists in bucket"""
        bucket = bucket or settings.minio_bucket
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except Exception:
            return False

# ============ CLIENT MANAGER ============
class ClientManager:
    """Manages all clients with lazy initialization and connection pooling"""
    
    def __init__(self):
        self._trino: Optional[TrinoClient] = None
        self._postgres: Optional[PostgresClient] = None
        self._redis: Optional[RedisClient] = None
        self._minio: Optional[MinIOClient] = None
        logger.info("ClientManager initialized")
    
    @property
    def trino(self) -> TrinoClient:
        """Get Trino client instance"""
        if self._trino is None:
            self._trino = TrinoClient()
        return self._trino
    
    @property
    def postgres(self) -> PostgresClient:
        """Get PostgreSQL client instance"""
        if self._postgres is None:
            self._postgres = PostgresClient()
        return self._postgres
    
    @property 
    def redis(self) -> RedisClient:
        """Get Redis client instance"""
        if self._redis is None:
            self._redis = RedisClient()
        return self._redis
    
    @property
    def minio(self) -> MinIOClient:
        """Get MinIO client instance"""
        if self._minio is None:
            self._minio = MinIOClient()
        return self._minio
    
    def health_check(self) -> Dict[str, bool]:
        """Check health of all available services"""
        health = {}
        
        # Check Trino
        try:
            self.trino.execute_query("SELECT 1")
            health['trino'] = True
        except Exception as e:
            logger.warning(f"Trino health check failed: {e}")
            health['trino'] = False
        
        # Check PostgreSQL
        try:
            self.postgres.execute_query("SELECT 1")
            health['postgres'] = True
        except Exception as e:
            logger.warning(f"PostgreSQL health check failed: {e}")
            health['postgres'] = False
        
        # Check Redis
        try:
            self.redis.client.ping()
            health['redis'] = True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            health['redis'] = False
        
        # Check MinIO
        try:
            self.minio.client.bucket_exists(settings.minio_bucket)
            health['minio'] = True
        except Exception as e:
            logger.warning(f"MinIO health check failed: {e}")
            health['minio'] = False
        
        logger.info(f"Health check results: {health}")
        return health
    
    def close_all(self):
        """Close all client connections"""
        logger.info("Closing all client connections")
        
        if self._trino:
            self._trino.close()
            self._trino = None
        
        if self._postgres:
            self._postgres.close()
            self._postgres = None
        
        # Redis and MinIO clients don't need explicit closing
        if self._redis:
            self._redis = None
        
        if self._minio:
            self._minio = None
        
        logger.info("All client connections closed")

# Global client manager instance
clients = ClientManager()

# Convenience function for health checks
def check_services_health() -> Dict[str, bool]:
    """Check health of all services"""
    return clients.health_check()

# Context manager for automatic cleanup
class ClientContext:
    """Context manager for automatic client cleanup"""
    
    def __enter__(self) -> ClientManager:
        return clients
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        clients.close_all()