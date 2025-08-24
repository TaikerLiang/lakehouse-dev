#!/usr/bin/env python3
"""
Script to analyze data from the Iceberg table using Trino.
Demonstrates various SQL operations on lakehouse data.
"""

from utils.clients import TrinoClient
from loguru import logger
import sys
from typing import List, Dict, Any


def run_analysis_queries(trino_client: TrinoClient, table_name: str, schema: str = "sales") -> None:
    """Run various analytical queries on the data."""
    
    full_table_name = f"iceberg.{schema}.{table_name}"
    logger.info(f"Running analysis on {full_table_name}")
    
    # Query 1: Basic table stats
    logger.info("=== Basic Table Statistics ===")
    stats_query = f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(DISTINCT product_name) as unique_products,
        MIN(order_date) as earliest_order,
        MAX(order_date) as latest_order,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value
    FROM {full_table_name}
    """
    
    try:
        stats = trino_client.fetchone(stats_query)
        if stats:
            logger.info("Table Statistics:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
    except Exception as e:
        logger.error(f"Failed to get basic stats: {e}")
    
    # Query 2: Revenue by category
    logger.info("\n=== Revenue by Category ===")
    category_query = f"""
    SELECT 
        category,
        COUNT(*) as order_count,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value,
        SUM(quantity) as total_quantity
    FROM {full_table_name}
    GROUP BY category
    ORDER BY total_revenue DESC
    """
    
    try:
        category_results = trino_client.fetchall(category_query)
        logger.info("Category Performance:")
        for row in category_results:
            logger.info(f"  {row['category']}: ${row['total_revenue']:.2f} "
                       f"({row['order_count']} orders, avg: ${row['avg_order_value']:.2f})")
    except Exception as e:
        logger.error(f"Failed to get category analysis: {e}")
    
    # Query 3: Regional performance
    logger.info("\n=== Regional Performance ===")
    regional_query = f"""
    SELECT 
        region,
        COUNT(*) as order_count,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM {full_table_name}
    GROUP BY region
    ORDER BY total_revenue DESC
    """
    
    try:
        regional_results = trino_client.fetchall(regional_query)
        logger.info("Regional Performance:")
        for row in regional_results:
            logger.info(f"  {row['region']}: ${row['total_revenue']:.2f} "
                       f"({row['unique_customers']} customers, {row['order_count']} orders)")
    except Exception as e:
        logger.error(f"Failed to get regional analysis: {e}")
    
    # Query 4: Monthly trend
    logger.info("\n=== Monthly Trend Analysis ===")
    monthly_query = f"""
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m') as month,
        COUNT(*) as order_count,
        SUM(total_amount) as monthly_revenue,
        AVG(total_amount) as avg_order_value
    FROM {full_table_name}
    GROUP BY DATE_FORMAT(order_date, '%Y-%m')
    ORDER BY month
    """
    
    try:
        monthly_results = trino_client.fetchall(monthly_query)
        logger.info("Monthly Trends (last 10 months):")
        for row in monthly_results[-10:]:  # Show last 10 months
            logger.info(f"  {row['month']}: ${row['monthly_revenue']:.2f} "
                       f"({row['order_count']} orders)")
    except Exception as e:
        logger.error(f"Failed to get monthly analysis: {e}")
    
    # Query 5: Top products
    logger.info("\n=== Top Products ===")
    product_query = f"""
    SELECT 
        product_name,
        category,
        COUNT(*) as order_count,
        SUM(quantity) as total_quantity,
        SUM(total_amount) as total_revenue,
        AVG(unit_price) as avg_price
    FROM {full_table_name}
    GROUP BY product_name, category
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    
    try:
        product_results = trino_client.fetchall(product_query)
        logger.info("Top 10 Products by Revenue:")
        for i, row in enumerate(product_results, 1):
            logger.info(f"  {i}. {row['product_name']} ({row['category']}): "
                       f"${row['total_revenue']:.2f} ({row['order_count']} orders)")
    except Exception as e:
        logger.error(f"Failed to get product analysis: {e}")
    
    # Query 6: Customer analysis
    logger.info("\n=== Top Customers ===")
    customer_query = f"""
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total_amount) as total_spent,
        AVG(total_amount) as avg_order_value,
        SUM(quantity) as total_items
    FROM {full_table_name}
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 10
    """
    
    try:
        customer_results = trino_client.fetchall(customer_query)
        logger.info("Top 10 Customers by Total Spent:")
        for i, row in enumerate(customer_results, 1):
            logger.info(f"  {i}. {row['customer_id']}: ${row['total_spent']:.2f} "
                       f"({row['order_count']} orders, avg: ${row['avg_order_value']:.2f})")
    except Exception as e:
        logger.error(f"Failed to get customer analysis: {e}")


def check_table_exists(trino_client: TrinoClient, table_name: str, schema: str = "sales") -> bool:
    """Check if the table exists."""
    try:
        tables_query = f"SHOW TABLES FROM iceberg.{schema}"
        tables = trino_client.fetchall(tables_query)
        table_names = [table['Table'] for table in tables]
        exists = table_name in table_names
        logger.info(f"Table {schema}.{table_name} exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Failed to check if table exists: {e}")
        return False


def main():
    """Main execution function."""
    logger.info("Starting data analysis")
    
    # Configuration
    table_name = "ecommerce_orders"
    schema = "sales"
    
    try:
        # Initialize Trino client
        trino_client = TrinoClient()
        
        # Check if table exists
        if not check_table_exists(trino_client, table_name, schema):
            logger.error(f"Table {schema}.{table_name} does not exist. "
                        "Please run create_iceberg_table.py first.")
            sys.exit(1)
        
        # Run analysis
        run_analysis_queries(trino_client, table_name, schema)
        
        logger.success("Data analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Analysis failed with error: {e}")
        sys.exit(1)
    finally:
        if 'trino_client' in locals():
            trino_client.close()


if __name__ == "__main__":
    main()