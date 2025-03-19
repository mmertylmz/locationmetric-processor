import logging
import sys
from src.excel.processor import ExcelProcessor
#from src.database import test_mssql_connection
from config import TARGET_COLUMNS

if __name__ == "__main__":
    processor = ExcelProcessor(batch_size=100)

    logging.info("Program starting...")

    #if not test_mssql_connection():
     #   logging.error("Database connection failed! Exiting program.")
      #  sys.exit(1)

    logging.info("Starting to process Excel files...")
    processed_count = processor.watch_folder()

    if processed_count > 0:
        logging.info(f"Processing completed. Total {processed_count} Excel files processed.")
    else:
        logging.info("No Excel files found to process.")

    logging.info("Program completed.")