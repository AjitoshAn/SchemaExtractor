#!/usr/bin/env python3
"""
Test script to demonstrate the file locking solution.

This script simulates the scenario where a file is open in another application
and shows how the robust file writing functions handle this situation.
"""

import time
import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# File operation constants
MAX_FILE_WRITE_RETRIES = 3
FILE_WRITE_RETRY_DELAY = 2  # seconds

def safe_write_csv_with_retry(
    df: pd.DataFrame, 
    target_path: Path, 
    temp_path: Path,
    max_retries: int = MAX_FILE_WRITE_RETRIES,
    retry_delay: int = FILE_WRITE_RETRY_DELAY
) -> bool:
    """
    Safely writes a DataFrame to CSV with retry logic for file locking issues.
    """
    for attempt in range(max_retries + 1):
        try:
            # Step 1: Write to temp file
            logger.info(f"Writing to temp file (attempt {attempt + 1}): {temp_path}")
            df.to_csv(temp_path, index=False, encoding="utf-8-sig")
            
            # Step 2: Check if target file is locked
            if target_path.exists():
                try:
                    # Try to open the file in write mode to check if it's locked
                    with open(target_path, 'r+b') as test_file:
                        pass  # If we can open it, it's not locked
                except (PermissionError, OSError) as lock_error:
                    if attempt < max_retries:
                        logger.warning(
                            f"Target file {target_path.name} appears to be locked (attempt {attempt + 1}): {lock_error}"
                        )
                        logger.info(f"Waiting {retry_delay} seconds before retry...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.error(f"Target file {target_path.name} is locked after {max_retries} attempts. Cannot overwrite.")
                        if temp_path.exists():
                            temp_path.unlink(missing_ok=True)
                        return False
            
            # Step 3: Move temp to target (atomic operation)
            logger.info(f"Moving temp file to target: {temp_path.name} -> {target_path.name}")
            import shutil
            shutil.move(str(temp_path), str(target_path))
            logger.info(f"Successfully wrote file: {target_path.name}")
            return True
            
        except (OSError, shutil.Error) as move_error:
            if attempt < max_retries:
                logger.warning(
                    f"Failed to move temp file to target (attempt {attempt + 1}): {move_error}"
                )
                logger.info(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
                # Clean up temp file if it exists
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
                continue
            else:
                logger.error(f"Failed to write file after {max_retries} attempts: {move_error}")
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
                return False
                
        except Exception as e:
            logger.exception(f"Unexpected error writing file (attempt {attempt + 1}): {e}")
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            if attempt < max_retries:
                logger.info(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
                continue
            else:
                return False
    
    return False

def test_file_locking_scenario():
    """Test the file locking solution with a sample scenario."""
    
    # Create test data
    test_data = {
        'TABLE_SCHEMA': ['dbo', 'dbo', 'dbo'],
        'TABLE_NAME': ['Users', 'Products', 'Orders'],
        'COLUMN_NAME': ['id', 'name', 'price'],
        'DATA_TYPE': ['int', 'varchar', 'decimal']
    }
    df = pd.DataFrame(test_data)
    
    # Test file paths
    test_dir = Path("test_output")
    test_dir.mkdir(exist_ok=True)
    
    target_file = test_dir / "test_schema_v001.csv"
    temp_file = test_dir / "test_schema_v001.csv.tmp"
    
    # Clean up any existing files
    if target_file.exists():
        target_file.unlink()
    if temp_file.exists():
        temp_file.unlink()
    
    logger.info("=== Testing File Locking Solution ===")
    logger.info(f"Target file: {target_file}")
    logger.info(f"Temp file: {temp_file}")
    
    # Test 1: Normal write (should succeed)
    logger.info("\n--- Test 1: Normal write (file not locked) ---")
    success = safe_write_csv_with_retry(df, target_file, temp_file)
    if success:
        logger.info("✅ Test 1 PASSED: Normal write succeeded")
    else:
        logger.error("❌ Test 1 FAILED: Normal write failed")
    
    # Test 2: Write to existing file (should succeed)
    logger.info("\n--- Test 2: Write to existing file ---")
    success = safe_write_csv_with_retry(df, target_file, temp_file)
    if success:
        logger.info("✅ Test 2 PASSED: Overwrite succeeded")
    else:
        logger.error("❌ Test 2 FAILED: Overwrite failed")
    
    # Test 3: Simulate locked file scenario
    logger.info("\n--- Test 3: Simulating locked file scenario ---")
    logger.info("To test this scenario:")
    logger.info("1. Open the file in Excel or another application")
    logger.info("2. Run this script again")
    logger.info("3. The script will retry and eventually fail gracefully")
    
    # Check if file is currently locked
    try:
        with open(target_file, 'r+b') as test_file:
            logger.info("File is currently NOT locked")
    except (PermissionError, OSError) as e:
        logger.warning(f"File appears to be locked: {e}")
        logger.info("This is expected if you have the file open in another application")
    
    logger.info("\n=== Test Summary ===")
    logger.info("The robust file writing solution provides:")
    logger.info("✅ Retry logic for temporary file system issues")
    logger.info("✅ File locking detection")
    logger.info("✅ Graceful failure with proper cleanup")
    logger.info("✅ Atomic write operations (temp file → target file)")
    logger.info("✅ Detailed logging for troubleshooting")

if __name__ == "__main__":
    test_file_locking_scenario() 