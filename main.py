import logging
import sys
from src.excel.processor import ExcelProcessor
from src.database.test_mssql_connection import test_mssql_connection

if __name__ == "__main__":
    processor = ExcelProcessor(batch_size=50)

    logging.info("Program starting...")

    if not test_mssql_connection():
        logging.error("Database connection failed! Exiting program.")
        sys.exit(1)

    logging.info("Starting to process Excel files...")
    processed_count = processor.watch_folder()

    if processed_count > 0:
        logging.info(f"Processing completed. Total {processed_count} Excel files processed.")
    else:
        logging.info("No Excel files found to process.")

    logging.info("Program completed.")