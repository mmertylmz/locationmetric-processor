import os
from datetime import datetime

def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
            print(f"Created directory: {directory_path}")
            return True
        except Exception as e:
            print(f"Error creating directory {directory_path}: {e}")
            return False
    return True


def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def is_valid_excel_file(file_path):
    return (file_path.endswith('.xlsx') or file_path.endswith('.xls')) and \
        os.path.exists(file_path) and \
        os.access(file_path, os.R_OK)