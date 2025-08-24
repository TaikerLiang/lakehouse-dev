"""
Tests for settings module
"""
import pytest
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from settings import settings

def test_settings_defaults():
    """Test that settings have reasonable defaults"""
    assert settings.app_name == "Data Lakehouse Pipeline"
    assert settings.environment == "development"
    assert settings.debug == True
    assert settings.trino_host == "localhost"
    assert settings.trino_port == 8080
    assert settings.batch_size == 1000

def test_settings_helper_methods():
    """Test helper methods work correctly"""
    # Test development check (default environment is 'development')
    assert settings.is_development() == True
    assert settings.is_production() == False
    
    # Test email recipients parsing
    recipients = settings.get_email_recipients_list()
    assert isinstance(recipients, list)
    
    # Test postgres connection string
    conn_str = settings.get_postgres_connection_string()
    assert "postgresql://" in conn_str
    assert settings.postgres_user in conn_str
    assert settings.postgres_host in conn_str

def test_environment_variable_override():
    """Test that environment variables can override defaults"""
    # Set a test environment variable
    os.environ['BATCH_SIZE'] = '2000'
    
    # Re-import settings to pick up the change
    from settings import Settings
    test_settings = Settings()
    
    assert test_settings.batch_size == 2000
    
    # Clean up
    del os.environ['BATCH_SIZE']