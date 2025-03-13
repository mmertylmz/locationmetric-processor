import os
import time
from watchdog.observers import Observer
from .handler import ExcelHandler

class FolderMonitor:
    def __init__(self, folder_path, target_columns):
        self.folder_path = folder_path
        self.target_columns = target_columns
        self.event_handler = ExcelHandler(target_columns)
        self.observer = None

    def start(self):
        """Start monitoring the folder."""
        print(f"Starting monitoring of folder: {self.folder_path}")
        print("Using watchdog to detect new Excel files.")
        print("Press Ctrl+C to stop monitoring.")

        # Create and start the observer
        self.observer = Observer()
        self.observer.schedule(self.event_handler, self.folder_path, recursive=False)
        self.observer.start()

        try:
            # Check for existing Excel files
            print("Checking for existing Excel files...")
            self._process_existing_files()

            # Keep the thread alive
            print("\nMonitoring for new files... Press Ctrl+C to stop.")
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\nStopping monitoring. Exiting...")
            self.stop()

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()

    def _process_existing_files(self):
        try:
            for file in os.listdir(self.folder_path):
                if file.endswith('.xlsx') or file.endswith('.xls'):
                    file_path = os.path.join(self.folder_path, file)
                    print(f"Found existing Excel file: {file}")
                    self.event_handler.process_existing_file(file_path)
        except Exception as e:
            print(f"Error processing existing files: {e}")