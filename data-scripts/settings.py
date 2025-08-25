"""
Settings module - Single source of truth for ALL configuration
Reads from .env file and provides typed settings for the entire application
"""
import os
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv

# Define root path of the project (parent of data-scripts directory)
ROOT_PATH = Path(__file__).parent.parent
ENV_FILE_PATH = ROOT_PATH / ".env"

# Load environment variables from root .env file
load_dotenv(dotenv_path=ENV_FILE_PATH)

class Settings(BaseSettings):
    """All application settings in one place"""
    
    # ============ APP SETTINGS ============
    app_name: str = Field(default="Data Lakehouse Pipeline", env='APP_NAME')
    environment: str = Field(default="development", env='ENVIRONMENT')
    debug: bool = Field(default=True, env='DEBUG')
    log_level: str = Field(default="INFO", env='LOG_LEVEL')
    
    # ============ TRINO SETTINGS ============
    trino_host: str = Field(default='localhost', env='TRINO_HOST')
    trino_port: int = Field(default=8080, env='TRINO_PORT')
    trino_catalog: str = Field(default='iceberg', env='TRINO_CATALOG')
    trino_schema: str = Field(default='default', env='TRINO_SCHEMA')
    trino_user: str = Field(default='', env='TRINO_USER')
    
    # ============ POSTGRES SETTINGS ============
    postgres_host: str = Field(default='localhost', env='POSTGRES_HOST')
    postgres_port: int = Field(default=5433, env='POSTGRES_PORT')
    postgres_db: str = Field(default='metastore', env='POSTGRES_DB')
    postgres_user: str = Field(default='hive', env='POSTGRES_USER')
    postgres_password: str = Field(default='hive', env='POSTGRES_PASSWORD')
    
    # ============ REDIS SETTINGS ============
    redis_host: str = Field(default='localhost', env='REDIS_HOST')
    redis_port: int = Field(default=6379, env='REDIS_PORT')
    redis_db: int = Field(default=0, env='REDIS_DB')
    redis_password: str = Field(default='', env='REDIS_PASSWORD')
    
    # ============ MINIO SETTINGS ============
    minio_endpoint: str = Field(default='localhost:9000', env='MINIO_ENDPOINT')
    minio_access_key: str = Field(default='minio', env='MINIO_ACCESS_KEY')
    minio_secret_key: str = Field(default='minio123', env='MINIO_SECRET_KEY')
    minio_bucket: str = Field(default='warehouse', env='MINIO_BUCKET')
    minio_secure: bool = Field(default=False, env='MINIO_SECURE')
    
    # Infrastructure-specific MinIO settings
    minio_root_user: str = Field(default='minio', env='MINIO_ROOT_USER')
    minio_root_password: str = Field(default='minio123', env='MINIO_ROOT_PASSWORD')
    minio_port: int = Field(default=9000, env='MINIO_PORT')
    minio_console_port: int = Field(default=9001, env='MINIO_CONSOLE_PORT')
    
    # ============ FEATURE FLAGS ============
    send_email_alerts: bool = Field(default=False, env='SEND_EMAIL_ALERTS')
    enable_data_validation: bool = Field(default=True, env='ENABLE_DATA_VALIDATION')
    dry_run_mode: bool = Field(default=False, env='DRY_RUN_MODE')
    auto_create_tables: bool = Field(default=True, env='AUTO_CREATE_TABLES')
    
    # ============ PROCESSING SETTINGS ============
    batch_size: int = Field(default=1000, env='BATCH_SIZE')
    max_retries: int = Field(default=3, env='MAX_RETRIES')
    timeout_seconds: int = Field(default=300, env='TIMEOUT_SECONDS')
    parallel_workers: int = Field(default=4, env='PARALLEL_WORKERS')
    
    # ============ EMAIL SETTINGS ============
    email_smtp_host: str = Field(default='smtp.gmail.com', env='EMAIL_SMTP_HOST')
    email_smtp_port: int = Field(default=587, env='EMAIL_SMTP_PORT')
    email_username: str = Field(default='', env='EMAIL_USERNAME')
    email_password: str = Field(default='', env='EMAIL_PASSWORD')
    email_from: str = Field(default='noreply@company.com', env='EMAIL_FROM')
    email_recipients: str = Field(default='', env='EMAIL_RECIPIENTS')  # comma-separated
    
    # ============ INFRASTRUCTURE SETTINGS ============
    # CloudBeaver settings
    cb_server_name: str = Field(default='Lakehouse CloudBeaver', env='CB_SERVER_NAME')
    cb_server_url: str = Field(default='http://localhost:8978', env='CB_SERVER_URL')
    cb_admin_name: str = Field(default='admin', env='CB_ADMIN_NAME')
    cb_admin_password: str = Field(default='admin123', env='CB_ADMIN_PASSWORD')
    cloudbeaver_port: int = Field(default=8978, env='CLOUDBEAVER_PORT')
    
    # Hive Metastore settings
    hive_metastore_port: int = Field(default=9083, env='HIVE_METASTORE_PORT')
    
    class Config:
        env_file = str(ENV_FILE_PATH)
        case_sensitive = False
    
    # ============ HELPER METHODS ============
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.environment.lower() == "development"
    
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.environment.lower() == "production"
    
    def get_email_recipients_list(self) -> list[str]:
        """Get email recipients as a list"""
        if not self.email_recipients:
            return []
        return [email.strip() for email in self.email_recipients.split(',') if email.strip()]
    
    def get_postgres_connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

# Global settings instance
settings = Settings()

# Export commonly used settings for convenience
DEBUG = settings.debug
LOG_LEVEL = settings.log_level
ENVIRONMENT = settings.environment