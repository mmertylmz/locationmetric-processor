import logging
import sys
import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.excel.processor import ExcelProcessor
from src.database.test_mssql_connection import test_mssql_connection
from config import EXCEL_CONFIG

class ExcelFileHandler(FileSystemEventHandler):
    def __init__(self, processor):
        self.processor = processor
        self.processing_files = set()

    def on_created(self, event):
        if not event.is_directory and self.is_excel_file(event.src_path):
            self._process_file(event.src_path)

    def on_moved(self, event):
        if not event.is_directory and self.is_excel_file(event.dest_path):
            self._process_file(event.dest_path)

    def is_excel_file(self, file_path):
        return file_path.endswith(('.xlsx', '.xls'))

    def _process_file(self, file_path):
        if file_path in self.processing_files:
            return

        time.sleep(1)

        try:
            if not os.path.exists(file_path):
                return

            try:
                with open(file_path, 'rb') as _:
                    pass
            except (IOError, PermissionError):
                logging.warning(f"File {file_path} is locked. Will try again later.")
                return

            self.processing_files.add(file_path)
            logging.info(f"New Excel File detected: {file_path}")

            try:
                self.processor.process_file(file_path)
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")
            finally:
                self.processing_files.remove(file_path)
        except Exception as e:
            logging.error(f"Error in file processing flow: {e}")
            if file_path in self.processing_files:
                self.processing_files.remove(file_path)

if __name__ == "__main__":
    processor = ExcelProcessor(batch_size=50)
    logging.info("Programming starting...")

    if not test_mssql_connection():
        logging.error("Database connection failed! Existing program.")
        sys.exit(1)

    watch_folder = EXCEL_CONFIG["watch_folder"]
    logging.info(f"Starting to monitor folder: {watch_folder}")

    processed_count = processor.watch_folder()

    if processed_count > 0:
        logging.info(f"Initial processing completed. {processed_count} Excel files processed.")
    else:
        logging.info("No Excel files found for initial processing.")

    event_handler = ExcelFileHandler(processor)
    observer = Observer()
    observer.schedule(event_handler, watch_folder, recursive=False)
    observer.start()

    logging.info(f"Watching folder {watch_folder} for new Excel files.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Program interrupted by user. Shutting down...")
        observer.stop()

    observer.join()
    logging.info("Program completed.")










