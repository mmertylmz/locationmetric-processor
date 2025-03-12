import pandas as pd

def read_excel_columns(file_path):
    try:
        df = pd.read_excel(file_path)

        print("\nFirst Five Rows")
        print(df.head())

        return df

    except FileNotFoundError:
        print(f"Error: '{file_path}' file could not found.")
    except Exception as e:
        print(f"An error occurred: '{e}'")
    return ""


if __name__ == "__main__":
    file_path = "Excel Files/Outscraper.xlsx"
    df = read_excel_columns(file_path)