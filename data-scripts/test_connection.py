#!/usr/bin/env python3
"""
Test script to verify all connections are working.
"""

from utils.clients import ClientManager
from loguru import logger
import sys


def test_connections():
    """Test all service connections."""
    logger.info("Testing service connections...")
    
    client_manager = ClientManager()
    
    try:
        # Test health check
        health_results = client_manager.health_check()
        
        logger.info("=== Connection Test Results ===")
        all_healthy = True
        for service, is_healthy in health_results.items():
            status = "✓ HEALTHY" if is_healthy else "✗ FAILED"
            logger.info(f"  {service.upper()}: {status}")
            if not is_healthy:
                all_healthy = False
        
        if all_healthy:
            logger.success("All services are healthy!")
            return True
        else:
            logger.error("Some services are not healthy")
            return False
            
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False
    finally:
        client_manager.close_all()


def main():
    """Main execution function."""
    logger.info("Starting connection test")
    
    success = test_connections()
    
    if success:
        logger.success("Connection test completed successfully!")
        sys.exit(0)
    else:
        logger.error("Connection test failed")
        sys.exit(1)


if __name__ == "__main__":
    main()