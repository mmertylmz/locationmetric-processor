import pandas as pd
import os
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

def read_selected_excel_columns(file_path, columns_to_select=None):
    try:
        df = pd.read_excel(file_path)

        if columns_to_select is not None:
            existing_columns = [col for col in columns_to_select if col in df.columns]
            missing_columns = [col for col in columns_to_select if col not in df.columns]

            if missing_columns:
                print(f"Warming: These columns did not found: {', '.join(missing_columns)}")

            df = df[existing_columns]


        print("\nFirst Five Rows")
        print(df.head())

        print(f"\nTotal Records: {len(df)}")
        print("\nColumns Data Types:")
        for col in df.columns:
            print(f" {col}: {df[col].dtype}")

        return df

    except FileNotFoundError:
        print(f"Error: '{file_path}' file could not found.")
    except Exception as e:
        print(f"An error occurred: '{e}'")

    return None

def process_excel_file(file_path, target_columns):
    print(f"Processing:  {file_path}")
    print(f"Process Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    df = read_selected_excel_columns(file_path, target_columns)
    if df is not None and not df.empty:
        print("Data successfully processed.")
        return df
    else:
        print("An error occurred.")
        return None

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, target_columns):
        self.target_columns = target_columns
        self.processed_files = set()

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            time.sleep(1)

            print(f"\nNew excel file detected: {file_path}")

            try:
                if os.path.exists(file_path) and os.access(file_path, os.R_OK):
                    if file_path not in self.processed_files:
                        df = process_excel_file(file_path, self.target_columns)
                        if df is not None:
                            self.processed_files.add(file_path)
                    else:
                        print(f"File already processed: {file_path}")
                else:
                    print(f"File is not accessible yet: {file_path}")
            except Exception as e:
                print(f"Error handling newly created a file: {e}")

def start_monitoring(folder_path, target_columns):
    print(f"Starting monitoring of folder: {folder_path}")
    print(f"Using watchdog to detect new Excel files.")
    print("Press Ctrl+C to stop monitoring.")

    event_handler = ExcelHandler(target_columns)
    observer = Observer()
    observer.schedule(event_handler, folder_path, recursive=False)
    observer.start()

    try:
        print("Checking for existing Excel Files...")
        for file in os.listdir(folder_path):
            if file.endswith('.xlsx') or file.endswith('.xls'):
                file_path = os.path.join(folder_path, file)
                if file_path not in event_handler.processed_files:
                    print(f"Found existing Excel File: {file}")
                    df = process_excel_file(file_path, target_columns)

        print("\nMonitoring for new files... Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping monitoring. Exiting...")
        observer.stop()

    observer.join()

if __name__ == "__main__":
    excel_folder = "Excel Files"

    target_columns = [
        "type",
        "full_address",
        "postal_code",
        "state",
        "latitude",
        "longitude",
        "rating",
        "reviews",
        "reviews_per_score_1",
        "reviews_per_score_2",
        "reviews_per_score_3",
        "reviews_per_score_4",
        "reviews_per_score_5",
        "photos_count",
        "verified",
        "location_link",
        "place_id",
        "google_id",
        "cid"
    ]

    #file_path = os.path.join(excel_folder, "Outscraper.xlsx")
    #df = process_excel_file(file_path, target_columns)

    start_monitoring(excel_folder, target_columns)
