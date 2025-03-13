from datetime import datetime
from ..excel.reader import ExcelReader

class ExcelProcessor:
    def __init__(self, target_columns):
        self.target_columns = target_columns

    def process_file(self, file_path):
        print(f"Processing: {file_path}")
        print(f"Process Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Use the ExcelReader to read the file
        df = ExcelReader.read_selected_columns(file_path, self.target_columns)

        if df is not None and not df.empty:
            print("Data successfully processed.")
            return df
        else:
            print("An error occurred during processing.")
            return None