# File Locking Solution for Data Model Extractor

## Problem Description

When running the data model extractor, if the target CSV files are open in other applications (like Excel, text editors, etc.), the script fails to write the updated data because the files are locked by the other applications.

## Solution Implemented

I've implemented a robust file writing solution that handles file locking issues with the following features:

### Key Features

1. **Retry Logic**: Automatically retries failed write operations
2. **File Lock Detection**: Checks if target files are locked before attempting to overwrite
3. **Atomic Operations**: Uses temporary files for safe atomic writes
4. **Graceful Failure**: Provides clear error messages and proper cleanup
5. **Configurable Retry Parameters**: Adjustable retry count and delay

### Implementation Details

#### Constants Added
```python
MAX_FILE_WRITE_RETRIES = 3
FILE_WRITE_RETRY_DELAY = 2  # seconds
FILE_LOCK_CHECK_DELAY = 1   # seconds
```

#### New Functions

1. **`safe_write_csv_with_retry()`**: Main function that handles robust CSV writing
2. **`check_file_locked()`**: Utility function to check if a file is locked

### How It Works

1. **Write to Temp File**: First writes data to a temporary file
2. **Check File Lock**: Attempts to open the target file to check if it's locked
3. **Retry on Lock**: If locked, waits and retries up to the configured limit
4. **Atomic Move**: Once unlocked, moves the temp file to the target location
5. **Cleanup**: Properly cleans up temporary files on failure

### Usage

The solution is automatically integrated into both main scripts:

- `DataModelAutoExtractor.py`
- `TestDataModelMetaDataExtractAll.py`

No changes to your workflow are needed - the scripts will now handle file locking automatically.

## Testing the Solution

Run the test script to see how it works:

```bash
python test_file_locking_solution.py
```

### Manual Testing Steps

1. **Normal Operation**: Run the extractor normally - should work as before
2. **File Locked Scenario**: 
   - Open a CSV file in Excel or another application
   - Run the extractor
   - The script will retry and eventually fail gracefully with clear error messages
3. **File Unlocked**: Close the file in the other application and run again - should succeed

## Error Handling

### When Files Are Locked

The script will:
1. Detect the locked file
2. Log a warning message
3. Wait 2 seconds
4. Retry up to 3 times
5. If still locked after all retries, log an error and continue with other files

### Log Messages

You'll see messages like:
```
WARNING - Target file schema_v001.csv appears to be locked (attempt 1): [Errno 13] Permission denied
INFO - Waiting 2 seconds before retry...
ERROR - Target file schema_v001.csv is locked after 3 attempts. Cannot overwrite.
```

## Configuration

You can adjust the retry behavior by modifying these constants in both scripts:

```python
MAX_FILE_WRITE_RETRIES = 3      # Number of retry attempts
FILE_WRITE_RETRY_DELAY = 2      # Seconds to wait between retries
```

## Benefits

1. **No Data Loss**: Uses atomic operations to prevent partial writes
2. **Clear Feedback**: Detailed logging shows exactly what's happening
3. **Graceful Degradation**: Continues processing other files even if some fail
4. **Automatic Recovery**: Retries automatically when files become available
5. **Clean State**: Properly cleans up temporary files

## Troubleshooting

### Common Issues

1. **Files Still Not Updating**: 
   - Check if files are open in other applications
   - Look for error messages in the logs
   - Verify file permissions

2. **Script Hanging**:
   - Check if files are locked by other processes
   - Review the retry configuration
   - Monitor the log output

3. **Permission Errors**:
   - Ensure the script has write permissions to the output directory
   - Check if antivirus software is blocking file operations

### Debug Mode

To see more detailed information, you can increase the logging level in the scripts:

```python
logger.setLevel(logging.DEBUG)
```

## Files Modified

1. **DataModelAutoExtractor.py**: Added robust file writing functions and updated export logic
2. **TestDataModelMetaDataExtractAll.py**: Added the same robust file writing functions
3. **test_file_locking_solution.py**: Test script to demonstrate the solution
4. **FILE_LOCKING_SOLUTION.md**: This documentation file

## Next Steps

1. Test the solution with your actual data
2. Monitor the logs to ensure it's working as expected
3. Adjust retry parameters if needed for your environment
4. Consider adding file locking detection to other file operations if needed

The solution should resolve the file locking issues you experienced while maintaining the same functionality and user experience. 