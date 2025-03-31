# Location Metric

This application processes location data from Excel files and stores it in a SQL Server database. It monitors a directory for new Excel files, processes them, and archives them after successful processing.

## Features

- Monitors a directory for new Excel files
- Processes location data and metrics from Excel files
- Stores data in a SQL Server database
- Logs processing information and errors
- Can run as a Windows service using NSSM

## Configuration

The application uses various configuration settings defined in `src/configurations/config.py`:

### Database Configuration

The application supports multiple database configurations through the `DB_CONFIGS` dictionary:

```python
DB_CONFIGS = {
    'primary': {
        'driver': 'SQL Server',
        'server': os.getenv('PRIMARY_DB_SERVER'),
        'database': os.getenv('PRIMARY_DB_NAME'),
        'username': os.getenv('PRIMARY_DB_USERNAME'),
        'password': os.getenv('PRIMARY_DB_PASSWORD'),
        'main_table': 'rmertDLMOutscraperLocation',
        'metric_table': 'rmertDLMOutscraperLocation',
        'type_table': 'rmertDLMOutscraperLocation',
    },
    'secondary': {
        'driver': 'SQL Server',
        'server': os.getenv('SECONDARY_DB_SERVER'),
        'database': os.getenv('SECONDARY_DB_NAME'),
        'username': os.getenv('SECONDARY_DB_USERNAME'),
        'password': os.getenv('SECONDARY_DB_PASSWORD'),
        'main_table': 'rmertDLMOutscraperLocation',
        'metric_table': 'rmertDLMOutscraperLocationMetric',
        'type_table': 'rmertDLMOutscraperLocationTypes',
    }
}
```

You can switch between configurations by changing:

```python
DB_CONFIG = DB_CONFIGS['secondary']  # Change to 'primary' for production
```

### Excel Configuration

```python
EXCEL_CONFIG = {
    'watch_folder': 'C:/WORKRUNNER/ExcelData',  # Directory to monitor for Excel files
    'archive_folder': 'C:/WORKRUNNER/ExcelData/Archive',  # Directory to move processed files
}
```

Make sure these directories exist on your system or the application will create them.

### Logging Configuration
```python
LOG_CONFIG = {
    'log_folder': './logs',
    'log_level': 'INFO',
}
```

## Installation

1. Clone the repository
2. Install the required packages:

```bash
pip install -r requirements.txt
```

3. Update the configuration in `src/configurations/config.py` to match your environment
4. Run the application:

```bash
python main.py
```

## Setting up as a Windows Service using NSSM

NSSM (Non-Sucking Service Manager) allows you to run Python applications as Windows services.

1. Download NSSM from [nssm.cc](https://nssm.cc/download)
2. Extract the archive and open a command prompt as administrator
3. Navigate to the NSSM directory and install the service:

```bash
nssm install LocationMetricService
```

4. In the dialog that appears:
   - Path: Select your Python executable (e.g., `C:\Python312\python.exe`)
   - Startup directory: Select your project directory (e.g., `C:/python-repo/location-metric`)
   - Arguments: `main.py`

5. Configure additional settings:
   - Go to the "Details" tab and set a description
   - Go to the "Log on" tab and configure appropriate credentials
   - Go to the "Startup" tab and set "Startup type" to "Automatic" to have the service start at system boot

6. Click "Install service"

The service will now run automatically when Windows starts.

To manage the service:
- Start: `nssm start LocationMetricService`
- Stop: `nssm stop LocationMetricService`
- Status: `nssm status LocationMetricService`
- Restart: `nssm restart LocationMetricService`
- Remove: `nssm remove LocationMetricService`

## Data Structure

The application processes Excel files that should contain the following columns (defined in `TARGET_COLUMNS`):

- name
- type
- phone
- full_address
- postal_code
- state
- latitude
- longitude
- rating
- reviews
- reviews_per_score_1 through reviews_per_score_5
- photos_count
- verified
- location_link
- place_id
- google_id
- cid
- country
- country_code
- time_zone

## Development Notes

When developing locally:

1. Update the database configuration to match your environment
2. Make sure the watch and archive directories exist
3. Test database connectivity with `src/database/test_mssql_connection.py`

## Troubleshooting

Check the application log files in the `logs` directory for error messages and information about the application's operation.

Common issues:
- Database connection failures: Check your database credentials and server availability
- Permission issues: Ensure the application has write access to the watch and archive directories
- Excel file format issues: Verify the input files have the expected columns

