import os
from datetime import datetime
import pandas as pd

def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def clean_data_frame(df, target_columns=None):
    try:
        string_columns = ['name', 'type', 'phone', 'full_address', 'state',
                          'location_link', 'place_id', 'google_id']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('')
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(r'\.0$', '', regex=True)
                df[col] = df[col].replace({'nan': '', 'None': '', 'NaN': ''})


        float_columns = ['latitude', 'longitude', 'rating']
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0.0)
                df[col] = df[col].apply(
                    lambda x: 0.0 if not isinstance(x, (int, float)) or pd.isna(x) else float(x))
                df[col] = df[col].astype(float)


        int_columns = ['reviews', 'reviews_per_score_1', 'reviews_per_score_2',
                       'reviews_per_score_3', 'reviews_per_score_4',
                       'reviews_per_score_5', 'photos_count', 'cid']
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0)
                df[col] = df[col].apply(lambda x: 0 if not isinstance(x, (int, float)) or pd.isna(x) else int(x))
                df[col] = df[col].astype(int)

        if 'verified' in df.columns:
            df['verified'] = pd.to_numeric(df['verified'], errors='coerce')
            df['verified'] = df['verified'].fillna(0)
            df['verified'] = df['verified'].apply(lambda x: bool(x) if isinstance(x, (int, float)) else False)

        if 'postal_code' in df.columns:
            df['postal_code'] = df['postal_code'].fillna('')
            df['postal_code'] = df['postal_code'].astype(str)
            df['postal_code'] = df['postal_code'].replace(r'\.0$', '', regex=True)
            df['postal_code'] = df['postal_code'].replace({'nan': '', 'None': '', 'NaN': ''})

        return df

    except Exception as e:
        import logging
        logging.error(f"Error cleaning data frame: {e}")
        return df