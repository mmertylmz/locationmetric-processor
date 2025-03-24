import pandas as pd
from ..configurations.config import TARGET_COLUMNS

class ExcelReader:
    @staticmethod
    def read_selected_columns(file_path, columns_to_select=None):
        try:
            df = pd.read_excel(file_path)

            if columns_to_select is not None:
                existing_columns = [col for col in columns_to_select if col in df.columns]
                missing_columns = [col for col in columns_to_select if col not in df.columns]

                if missing_columns:
                    print(f"Warning: These columns were not found: {', '.join(missing_columns)}")

                df = df[existing_columns]

            print("\nFirst Five Rows")
            print(df.head())

            print(f"\nTotal Records: {len(df)}")
            print("\nColumns Data Types:")
            for col in df.columns:
                print(f" {col}: {df[col].dtype}")

            return df

        except FileNotFoundError:
            print(f"Error: '{file_path}' file could not be found.")
        except Exception as e:
            print(f"An error occurred: '{e}'")

        return None


if __name__ == '__main__':
    excelReader = ExcelReader()
    excelReader.read_selected_columns("../../excel_files/Outscraper.xlsx", TARGET_COLUMNS)