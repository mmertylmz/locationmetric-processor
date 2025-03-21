import logging
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ExcelFileHandler(FileSystemEventHandler):
    def __init__(self, process_function):
        self.process_function = process_function
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
                self.process_function(file_path)
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")
            finally:
                self.processing_files.remove(file_path)
        except Exception as e:
            logging.error(f"Error in file processing flow: {e}")
            if file_path in self.processing_files:
                self.processing_files.remove(file_path)


def start_file_monitoring(watch_folder, process_function):
    event_handler = ExcelFileHandler(process_function)
    observer = Observer()
    observer.schedule(event_handler, watch_folder, recursive=False)
    observer.start()
    
    logging.info(f"Watching folder {watch_folder} for new Excel files.")
    
    return observer