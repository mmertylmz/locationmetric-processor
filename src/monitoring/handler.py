import os
import time
from watchdog.events import FileSystemEventHandler
from ..excel.processor import ExcelProcessor

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, target_columns):
        self.processor = ExcelProcessor(target_columns)
        self.processed_files = set()

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            # Wait a moment to ensure the file is completely written
            time.sleep(1)

            print(f"\nNew Excel file detected: {file_path}")

            try:
                if os.path.exists(file_path) and os.access(file_path, os.R_OK):
                    if file_path not in self.processed_files:
                        df = self.processor.process_file(file_path)
                        if df is not None:
                            self.processed_files.add(file_path)
                    else:
                        print(f"File already processed: {file_path}")
                else:
                    print(f"File is not accessible yet: {file_path}")
            except Exception as e:
                print(f"Error handling newly created file: {e}")

    def process_existing_file(self, file_path):
        if file_path not in self.processed_files:
            df = self.processor.process_file(file_path)
            if df is not None:
                self.processed_files.add(file_path)
                return True
        return False