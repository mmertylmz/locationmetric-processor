import os
from config import EXCEL_FOLDER, TARGET_COLUMNS
from src.monitoring.monitor import FolderMonitor
from src.utils.helpers import ensure_directory_exists


def main():
    if not ensure_directory_exists(EXCEL_FOLDER):
        print(f"Error: Could not access or create the Excel folder: {EXCEL_FOLDER}")
        return

    monitor = FolderMonitor(EXCEL_FOLDER, TARGET_COLUMNS)
    try:
        monitor.start()
    except Exception as e:
        print(f"Error starting the monitor: {e}")
    finally:
        monitor.stop()


if __name__ == "__main__":
    main()