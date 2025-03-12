import pandas as pd
import os
#import time
from datetime import datetime

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

def monitor_folder(folder_path, target_columns, check_interval=60):
    processed_files = set()
    print(f"'{folder_path}' folder is watching...")

    try:
        excel_files = [f for f in os.listdir(folder_path)
                       if f.endswith('.xlsx') or f.endswith('.xls')]

        for file in excel_files:
            file_path = os.path.join(folder_path, file)

            df = process_excel_file(file_path, target_columns)
            if df is not None:
                processed_files.add(file_path)

        #time.sleep(check_interval)

    except Exception as e:
        print(f"An error occurred when folder is watching: {e}")

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

    file_path = os.path.join(excel_folder, "Outscraper.xlsx")
    df = process_excel_file(file_path, target_columns)

    #monitor_folder(excel_folder,target_columns)
