#!/usr/bin/env python3
"""
Script to create Iceberg tables and insert sample data through Trino.
This demonstrates the lakehouse pattern with MinIO storage and Iceberg tables.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
from utils.clients import TrinoClient
from loguru import logger
import sys


def generate_sample_data(num_records: int = 1000) -> pd.DataFrame:
    """Generate sample e-commerce data for demonstration."""
    logger.info(f"Generating {num_records} sample records")
    
    # Sample data for e-commerce scenario
    products = [
        "Laptop", "Smartphone", "Tablet", "Headphones", "Keyboard", 
        "Mouse", "Monitor", "Speaker", "Camera", "Watch"
    ]
    
    categories = [
        "Electronics", "Computers", "Accessories", "Audio", "Photography"
    ]
    
    regions = ["US", "EU", "ASIA", "LATAM", "AFRICA"]
    
    data = []
    base_date = datetime.now() - timedelta(days=365)
    
    for i in range(num_records):
        record = {
            "order_id": f"ORD-{i+1:06d}",
            "product_name": random.choice(products),
            "category": random.choice(categories),
            "quantity": random.randint(1, 10),
            "unit_price": round(random.uniform(10.0, 1000.0), 2),
            "region": random.choice(regions),
            "order_date": base_date + timedelta(days=random.randint(0, 365)),
            "customer_id": f"CUST-{random.randint(1, 1000):04d}",
        }
        
        # Calculate total amount
        record["total_amount"] = round(record["quantity"] * record["unit_price"], 2)
        
        data.append(record)
    
    df = pd.DataFrame(data)
    logger.info(f"Generated DataFrame with shape: {df.shape}")
    return df


def create_iceberg_table(trino_client: TrinoClient, table_name: str, schema: str = "default") -> None:
    """Create an Iceberg table with the appropriate schema."""
    logger.info(f"Creating Iceberg table: {schema}.{table_name}")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS iceberg.{schema}.{table_name} (
        order_id VARCHAR,
        product_name VARCHAR,
        category VARCHAR,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        region VARCHAR,
        order_date DATE,
        customer_id VARCHAR
    ) WITH (
        format = 'PARQUET',
        location = 's3://warehouse/{schema}/{table_name}/'
    )
    """
    
    try:
        trino_client.execute(create_table_sql)
        logger.success(f"Successfully created table: {schema}.{table_name}")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise


def insert_data_batch(trino_client: TrinoClient, df: pd.DataFrame, table_name: str, 
                     schema: str = "default", batch_size: int = 100) -> None:
    """Insert data into Iceberg table in batches."""
    logger.info(f"Inserting {len(df)} records into {schema}.{table_name} in batches of {batch_size}")
    
    total_batches = (len(df) + batch_size - 1) // batch_size
    
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches}")
        
        # Convert DataFrame rows to VALUES clause
        values_list = []
        for _, row in batch_df.iterrows():
            values = (
                f"('{row['order_id']}', '{row['product_name']}', '{row['category']}', "
                f"{row['quantity']}, {row['unit_price']}, {row['total_amount']}, "
                f"'{row['region']}', DATE '{row['order_date'].strftime('%Y-%m-%d')}', "
                f"'{row['customer_id']}')"
            )
            values_list.append(values)
        
        insert_sql = f"""
        INSERT INTO iceberg.{schema}.{table_name}
        VALUES {', '.join(values_list)}
        """
        
        try:
            trino_client.execute(insert_sql)
            logger.info(f"Successfully inserted batch {batch_num}")
        except Exception as e:
            logger.error(f"Failed to insert batch {batch_num}: {e}")
            raise


def verify_data(trino_client: TrinoClient, table_name: str, schema: str = "default") -> None:
    """Verify the data was inserted correctly."""
    logger.info(f"Verifying data in {schema}.{table_name}")
    
    # Get row count
    count_sql = f"SELECT COUNT(*) as row_count FROM iceberg.{schema}.{table_name}"
    result = trino_client.fetchone(count_sql)
    logger.info(f"Table contains {result['row_count']} rows")
    
    # Get sample data
    sample_sql = f"SELECT * FROM iceberg.{schema}.{table_name} LIMIT 5"
    sample_data = trino_client.fetchall(sample_sql)
    
    logger.info("Sample data:")
    for row in sample_data:
        logger.info(f"  {dict(row)}")
    
    # Get aggregation example
    agg_sql = f"""
    SELECT 
        category,
        COUNT(*) as order_count,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value
    FROM iceberg.{schema}.{table_name}
    GROUP BY category
    ORDER BY total_revenue DESC
    """
    
    agg_results = trino_client.fetchall(agg_sql)
    logger.info("Revenue by category:")
    for row in agg_results:
        logger.info(f"  {dict(row)}")


def main():
    """Main execution function."""
    logger.info("Starting Iceberg table creation and data insertion")
    
    # Configuration
    table_name = "ecommerce_orders"
    schema = "sales"
    num_records = 2000
    batch_size = 200
    
    try:
        # Initialize Trino client
        trino_client = TrinoClient()
        
        # Create schema if it doesn't exist
        logger.info(f"Creating schema: {schema}")
        trino_client.execute(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema}")
        
        # Generate sample data
        df = generate_sample_data(num_records)
        
        # Create Iceberg table
        create_iceberg_table(trino_client, table_name, schema)
        
        # Insert data in batches
        insert_data_batch(trino_client, df, table_name, schema, batch_size)
        
        # Verify the data
        verify_data(trino_client, table_name, schema)
        
        logger.success("Successfully completed Iceberg table creation and data insertion!")
        
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        sys.exit(1)
    finally:
        if 'trino_client' in locals():
            trino_client.close()


if __name__ == "__main__":
    main()