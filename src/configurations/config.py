import os
from dotenv import load_dotenv

# .env dosyasını yükle
load_dotenv()

TARGET_COLUMNS = [
    "name",
    "type",
    "phone",
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
    "cid",
    "country",
    "country_code",
    "time_zone"
]

DB_CONFIGS = {
    'primary': {
        'driver': 'SQL Server',
        'server': os.getenv('PRIMARY_DB_SERVER'),
        'database': os.getenv('PRIMARY_DB_NAME'),
        'username': os.getenv('PRIMARY_DB_USERNAME'),
        'password': os.getenv('PRIMARY_DB_PASSWORD'),
        'main_table': 'rdlmDLMOutscraperLocation',
        'metric_table': 'rdlmDLMOutscraperLocationMetric',
        'type_table': 'rdlmDLMOutscraperLocationTypes',
    },
    'secondary': {
        'driver': 'SQL Server',
        'server': os.getenv('SECONDARY_DB_SERVER'),
        'database': os.getenv('SECONDARY_DB_NAME'),
        'username': os.getenv('SECONDARY_DB_USERNAME'),
        'password': os.getenv('SECONDARY_DB_PASSWORD'),
        'main_table': 'rmertDLMOutscraperLocation',
        'metric_table': 'rmertDLMOutscraperLocationMetric',
        'type_table': 'rmertDLMOutscraperLocationTypes',
    }
}

DB_CONFIG = DB_CONFIGS['secondary']

EXCEL_CONFIG = {
    'watch_folder': os.getenv('EXCEL_WATCH_FOLDER'),
    'archive_folder': os.getenv('EXCEL_ARCHIVE_FOLDER'),
}

LOG_CONFIG = {
    'log_folder': './logs',
    'log_level': 'INFO',
}