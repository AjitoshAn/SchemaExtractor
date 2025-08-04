# Global Business Data Model Schema Extract Automation

This project provides automated extraction of database schema information and exports it to CSV format with versioning and history tracking.

## Features

- **Robust File Writing**: Implements atomic writes with retry logic to handle file locking issues
- **Version Control**: Automatic versioning of exported schema files
- **Archive Management**: Automatic archiving of previous versions with cleanup
- **User Input Processing**: Support for user-provided CSV updates
- **Comprehensive Logging**: Detailed logging for troubleshooting and monitoring

## Security Features

- **Pandas Security**: Follows best practices to prevent SQL injection and other vulnerabilities
- **Parameterized Queries**: All database queries use parameterized statements
- **Secret Management**: Configuration template provided to prevent accidental secret exposure

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configuration**:
   - Copy `config.template.ini` to `config.ini`
   - Update the configuration with your actual values:
     - Azure AD service principal credentials
     - SQL Server connection details
     - SharePoint paths and URLs
     - Notification trigger URLs

3. **Database Access**:
   - Ensure you have access to the target SQL Server instances
   - Configure Azure AD authentication if using service principal

## Usage

### Main Script
```bash
python DataModelAutoExtractor.py
```

### Test Script
```bash
python TestDataModelMetaDataExtractAll.py
```

### File Locking Test
```bash
python test_file_locking_solution.py
```

## File Structure

- `DataModelAutoExtractor.py` - Main extraction script
- `TestDataModelMetaDataExtractAll.py` - Test version of the main script
- `test_file_locking_solution.py` - Standalone test for file locking handling
- `config.template.ini` - Configuration template (copy to `config.ini`)
- `requirements.txt` - Python dependencies
- `FILE_LOCKING_SOLUTION.md` - Documentation for the file locking solution

## Key Features

### File Locking Solution
The script includes robust file writing capabilities that handle cases where output files are open in other applications:

- **Atomic Writes**: Writes to temporary files first, then moves to final location
- **Retry Logic**: Automatically retries failed writes with configurable delays
- **Lock Detection**: Checks if files are locked before attempting writes
- **Error Handling**: Comprehensive error handling and logging

### Security Considerations
- No use of dangerous Pandas methods (`query()`, `eval()`, `read_pickle()`, `to_html()`)
- All SQL queries are properly parameterized
- Input validation for user-provided CSV files
- Configuration secrets are excluded from version control

## Logging

Logs are written to the `logs/` directory with timestamps. The logging level can be configured in the scripts.

## Troubleshooting

### File Locking Issues
If you encounter file locking issues:
1. Ensure the target file is not open in Excel or other applications
2. Check the logs for detailed error messages
3. The script will automatically retry with delays

### Configuration Issues
1. Verify your `config.ini` file is properly configured
2. Check that all required Azure AD credentials are correct
3. Ensure network access to SQL Server instances

## Contributing

When contributing to this project:
1. Never commit actual secrets or credentials
2. Use the template configuration file
3. Test file locking scenarios before submitting changes
4. Follow the existing code structure and logging patterns 