import logging
import sys
import time
from src.excel.processor import ExcelProcessor
from src.database.test_mssql_connection import test_mssql_connection
from src.monitoring.file_watcher import start_file_monitoring
from config import EXCEL_CONFIG

if __name__ == '__main__':
    processor = ExcelProcessor()
    logging.info("Starting Program")

    # Test MSSQL Connection
    if not test_mssql_connection():
        logging.error("MSSQL Connection Failed")
        sys.exit(1)
    
    watch_folder = EXCEL_CONFIG['watch_folder'] 
    logging.info(f"Starting to monitor folder: {watch_folder}")

    processed_count = processor.watch_folder()

    if processed_count > 0:
        logging.info(f"Initial processing completed. {processed_count} Excel files processed.")
    else:
        logging.info("No Excel files found. Monitoring for new files...")

    observer = start_file_monitoring(watch_folder, processor.process_file)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Program interrupted. Shutting down.")
        observer.stop()
    
    observer.join()
    logging.info("Program completed.")