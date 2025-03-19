import os
from datetime import datetime

def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

