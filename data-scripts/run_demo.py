#!/usr/bin/env python3
"""
Quick demo script to run the Iceberg table creation and data insertion.
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from create_iceberg_table import main
from loguru import logger

if __name__ == "__main__":
    logger.info("Starting Iceberg demo")
    try:
        main()
        logger.success("Demo completed successfully!")
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        sys.exit(1)