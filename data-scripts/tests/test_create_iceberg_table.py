"""
Unit tests for create_iceberg_table.py module.
Tests data generation, table creation, data insertion, and verification functions.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os
from pathlib import Path

# Add the parent directory to the path to import the module
sys.path.insert(0, str(Path(__file__).parent.parent))

from create_iceberg_table import (
    generate_sample_data,
    create_iceberg_table,
    insert_data_batch,
    verify_data,
    main
)


class TestGenerateSampleData:
    """Test cases for the generate_sample_data function."""
    
    def test_generate_sample_data_default_size(self):
        """Test generating sample data with default size."""
        df = generate_sample_data()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1000  # default size
        assert list(df.columns) == [
            "order_id", "product_name", "category", "quantity", 
            "unit_price", "region", "order_date", "customer_id", "total_amount"
        ]
    
    def test_generate_sample_data_custom_size(self):
        """Test generating sample data with custom size."""
        custom_size = 50
        df = generate_sample_data(custom_size)
        
        assert len(df) == custom_size
        assert isinstance(df, pd.DataFrame)
    
    def test_generate_sample_data_structure_and_types(self):
        """Test the structure and data types of generated data."""
        df = generate_sample_data(10)
        
        # Check data types
        assert df["order_id"].dtype == "object"  # string
        assert df["product_name"].dtype == "object"
        assert df["category"].dtype == "object"
        assert df["quantity"].dtype == "int64"
        assert df["unit_price"].dtype == "float64"
        assert df["total_amount"].dtype == "float64"
        assert df["region"].dtype == "object"
        assert df["customer_id"].dtype == "object"
        
        # Check order_id format
        assert all(df["order_id"].str.startswith("ORD-"))
        assert all(df["order_id"].str.len() == 10)  # "ORD-" + 6 digits
        
        # Check customer_id format
        assert all(df["customer_id"].str.startswith("CUST-"))
        assert all(df["customer_id"].str.len() == 9)  # "CUST-" + 4 digits
        
        # Check value ranges
        assert all(df["quantity"] >= 1) and all(df["quantity"] <= 10)
        assert all(df["unit_price"] >= 10.0) and all(df["unit_price"] <= 1000.0)
        # Check total_amount calculation (use approximation due to floating-point precision)
        calculated_total = df["quantity"] * df["unit_price"]
        assert all(abs(df["total_amount"] - calculated_total) < 0.01)
    
    def test_generate_sample_data_expected_values(self):
        """Test that generated data contains expected values."""
        df = generate_sample_data(100)
        
        expected_products = {
            "Laptop", "Smartphone", "Tablet", "Headphones", "Keyboard", 
            "Mouse", "Monitor", "Speaker", "Camera", "Watch"
        }
        expected_categories = {
            "Electronics", "Computers", "Accessories", "Audio", "Photography"
        }
        expected_regions = {"US", "EU", "ASIA", "LATAM", "AFRICA"}
        
        assert set(df["product_name"].unique()).issubset(expected_products)
        assert set(df["category"].unique()).issubset(expected_categories)
        assert set(df["region"].unique()).issubset(expected_regions)
    
    def test_generate_sample_data_date_range(self):
        """Test that generated dates are within expected range."""
        df = generate_sample_data(50)
        
        # Dates should be within the last 365 days
        today = datetime.now()
        min_date = today - timedelta(days=365)
        
        for order_date in df["order_date"]:
            assert order_date >= min_date
            assert order_date <= today
    
    def test_generate_sample_data_empty(self):
        """Test generating empty dataset."""
        df = generate_sample_data(0)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        # Empty DataFrame from pd.DataFrame([]) has no columns, which is expected behavior


class TestCreateIcebergTable:
    """Test cases for the create_iceberg_table function."""
    
    def test_create_iceberg_table_success(self):
        """Test successful table creation."""
        mock_client = Mock()
        
        create_iceberg_table(mock_client, "test_table", "test_schema")
        
        # Verify execute was called once
        mock_client.execute.assert_called_once()
        
        # Check the SQL contains expected elements
        sql_call = mock_client.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS iceberg.test_schema.test_table" in sql_call
        assert "format = 'PARQUET'" in sql_call
        assert "s3://warehouse/test_schema/test_table/" in sql_call
    
    def test_create_iceberg_table_default_schema(self):
        """Test table creation with default schema."""
        mock_client = Mock()
        
        create_iceberg_table(mock_client, "test_table")
        
        sql_call = mock_client.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS iceberg.default.test_table" in sql_call
        assert "s3://warehouse/default/test_table/" in sql_call
    
    def test_create_iceberg_table_exception(self):
        """Test table creation with exception."""
        mock_client = Mock()
        mock_client.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            create_iceberg_table(mock_client, "test_table")


class TestInsertDataBatch:
    """Test cases for the insert_data_batch function."""
    
    def test_insert_data_batch_single_batch(self):
        """Test inserting data in a single batch."""
        mock_client = Mock()
        
        # Create small sample data
        df = pd.DataFrame({
            "order_id": ["ORD-000001", "ORD-000002"],
            "product_name": ["Laptop", "Mouse"],
            "category": ["Electronics", "Accessories"],
            "quantity": [1, 2],
            "unit_price": [999.99, 25.50],
            "total_amount": [999.99, 51.00],
            "region": ["US", "EU"],
            "order_date": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "customer_id": ["CUST-0001", "CUST-0002"]
        })
        
        insert_data_batch(mock_client, df, "test_table", "test_schema", batch_size=10)
        
        # Should be called once since data fits in one batch
        mock_client.execute.assert_called_once()
        
        # Check SQL structure
        sql_call = mock_client.execute.call_args[0][0]
        assert "INSERT INTO iceberg.test_schema.test_table" in sql_call
        assert "VALUES" in sql_call
    
    def test_insert_data_batch_multiple_batches(self):
        """Test inserting data in multiple batches."""
        mock_client = Mock()
        
        # Create data that requires multiple batches
        df = pd.DataFrame({
            "order_id": [f"ORD-{i:06d}" for i in range(1, 6)],
            "product_name": ["Laptop"] * 5,
            "category": ["Electronics"] * 5,
            "quantity": [1] * 5,
            "unit_price": [100.0] * 5,
            "total_amount": [100.0] * 5,
            "region": ["US"] * 5,
            "order_date": [datetime(2024, 1, 1)] * 5,
            "customer_id": [f"CUST-{i:04d}" for i in range(1, 6)]
        })
        
        insert_data_batch(mock_client, df, "test_table", batch_size=2)
        
        # Should be called 3 times (5 records / 2 batch_size = 3 batches)
        assert mock_client.execute.call_count == 3
    
    def test_insert_data_batch_exception(self):
        """Test batch insertion with exception."""
        mock_client = Mock()
        mock_client.execute.side_effect = Exception("Insert failed")
        
        df = pd.DataFrame({
            "order_id": ["ORD-000001"],
            "product_name": ["Laptop"],
            "category": ["Electronics"],
            "quantity": [1],
            "unit_price": [999.99],
            "total_amount": [999.99],
            "region": ["US"],
            "order_date": [datetime(2024, 1, 1)],
            "customer_id": ["CUST-0001"]
        })
        
        with pytest.raises(Exception, match="Insert failed"):
            insert_data_batch(mock_client, df, "test_table")
    
    def test_insert_data_batch_empty_dataframe(self):
        """Test inserting empty dataframe."""
        mock_client = Mock()
        
        df = pd.DataFrame(columns=[
            "order_id", "product_name", "category", "quantity", 
            "unit_price", "total_amount", "region", "order_date", "customer_id"
        ])
        
        insert_data_batch(mock_client, df, "test_table")
        
        # Should not call execute for empty dataframe since len(df) == 0
        mock_client.execute.assert_not_called()


class TestVerifyData:
    """Test cases for the verify_data function."""
    
    def test_verify_data_success(self):
        """Test successful data verification."""
        mock_client = Mock()
        
        # Mock return values for different queries
        mock_client.fetchone.return_value = {"row_count": 1000}
        mock_client.fetchall.side_effect = [
            [  # Sample data query result
                {"order_id": "ORD-000001", "product_name": "Laptop", "total_amount": 999.99},
                {"order_id": "ORD-000002", "product_name": "Mouse", "total_amount": 25.50}
            ],
            [  # Aggregation query result
                {"category": "Electronics", "order_count": 500, "total_revenue": 50000.0, "avg_order_value": 100.0},
                {"category": "Accessories", "order_count": 300, "total_revenue": 15000.0, "avg_order_value": 50.0}
            ]
        ]
        
        verify_data(mock_client, "test_table", "test_schema")
        
        # Should call fetchone once and fetchall twice
        mock_client.fetchone.assert_called_once()
        assert mock_client.fetchall.call_count == 2
    
    def test_verify_data_no_data(self):
        """Test verification with no data in table."""
        mock_client = Mock()
        mock_client.fetchone.return_value = {"row_count": 0}
        mock_client.fetchall.side_effect = [[], []]  # Empty results
        
        verify_data(mock_client, "test_table")
        
        mock_client.fetchone.assert_called_once()
        assert mock_client.fetchall.call_count == 2
    
    def test_verify_data_exception(self):
        """Test verification with exception."""
        mock_client = Mock()
        mock_client.fetchone.side_effect = Exception("Query failed")
        
        with pytest.raises(Exception, match="Query failed"):
            verify_data(mock_client, "test_table")


class TestMain:
    """Test cases for the main function."""
    
    @patch('create_iceberg_table.TrinoClient')
    @patch('create_iceberg_table.generate_sample_data')
    @patch('create_iceberg_table.create_iceberg_table')
    @patch('create_iceberg_table.insert_data_batch')
    @patch('create_iceberg_table.verify_data')
    def test_main_success(self, mock_verify, mock_insert, mock_create, mock_generate, mock_trino_client):
        """Test successful execution of main function."""
        # Setup mocks
        mock_client_instance = Mock()
        mock_trino_client.return_value = mock_client_instance
        
        mock_df = Mock()
        mock_generate.return_value = mock_df
        
        # Execute main
        main()
        
        # Verify all functions were called in correct order
        mock_trino_client.assert_called_once()
        mock_client_instance.execute.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS iceberg.sales")
        mock_generate.assert_called_once_with(2000)
        mock_create.assert_called_once_with(mock_client_instance, "ecommerce_orders", "sales")
        mock_insert.assert_called_once_with(mock_client_instance, mock_df, "ecommerce_orders", "sales", 200)
        mock_verify.assert_called_once_with(mock_client_instance, "ecommerce_orders", "sales")
        mock_client_instance.close.assert_called_once()
    
    @patch('create_iceberg_table.TrinoClient')
    @patch('create_iceberg_table.generate_sample_data')
    def test_main_exception_handling(self, mock_generate, mock_trino_client):
        """Test main function exception handling."""
        # Setup mock to raise exception
        mock_generate.side_effect = Exception("Test error")
        mock_client_instance = Mock()
        mock_trino_client.return_value = mock_client_instance
        
        # Execute main and expect SystemExit
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        # Verify exit code is 1
        assert exc_info.value.code == 1
        
        # Verify client is still closed even on exception
        mock_client_instance.close.assert_called_once()
    
    @patch('create_iceberg_table.TrinoClient')
    def test_main_client_initialization_error(self, mock_trino_client):
        """Test main function when client initialization fails."""
        mock_trino_client.side_effect = Exception("Connection failed")
        
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 1


class TestIntegration:
    """Integration tests combining multiple functions."""
    
    def test_data_generation_and_insertion_compatibility(self):
        """Test that generated data is compatible with insertion function."""
        mock_client = Mock()
        
        # Generate sample data
        df = generate_sample_data(10)
        
        # This should not raise any exceptions
        insert_data_batch(mock_client, df, "test_table", batch_size=5)
        
        # Verify SQL was executed twice (2 batches)
        assert mock_client.execute.call_count == 2
        
        # Verify SQL contains all expected columns
        for call_args in mock_client.execute.call_args_list:
            sql = call_args[0][0]
            assert "INSERT INTO" in sql
            assert "VALUES" in sql


# Test fixtures and utilities
@pytest.fixture
def sample_dataframe():
    """Fixture providing a small sample DataFrame for testing."""
    return pd.DataFrame({
        "order_id": ["ORD-000001", "ORD-000002"],
        "product_name": ["Laptop", "Mouse"],
        "category": ["Electronics", "Accessories"],
        "quantity": [1, 2],
        "unit_price": [999.99, 25.50],
        "total_amount": [999.99, 51.00],
        "region": ["US", "EU"],
        "order_date": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
        "customer_id": ["CUST-0001", "CUST-0002"]
    })


@pytest.fixture
def mock_trino_client():
    """Fixture providing a mock TrinoClient."""
    return Mock()


# Parameterized tests for edge cases
@pytest.mark.parametrize("num_records,expected_batches", [
    (0, 0),
    (1, 1),
    (50, 1),
    (100, 1),
    (150, 2),
    (250, 3),
])
def test_batch_calculation(num_records, expected_batches, mock_trino_client):
    """Test that batch calculation works correctly for different data sizes."""
    if num_records == 0:
        df = pd.DataFrame(columns=[
            "order_id", "product_name", "category", "quantity", 
            "unit_price", "total_amount", "region", "order_date", "customer_id"
        ])
    else:
        df = generate_sample_data(num_records)
    
    insert_data_batch(mock_trino_client, df, "test_table", batch_size=100)
    
    assert mock_trino_client.execute.call_count == expected_batches