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
    "cid"
]

DB_CONFIG = {
    'driver': 'SQL Server',
    'server': r'SP-GRCRUNNER',
    'database': 'DLM_Repo',
    'username': 'outscraper',
    'password': 'QgCXlMgd_I'
}

EXCEL_CONFIG = {
    'watch_folder': 'C:/WORKRUNNER/ExcelData',
    'archive_folder': 'C:/WORKRUNNER/ExcelData/Archive',
}

LOG_CONFIG = {
    'log_folder': './logs',
    'log_level': 'INFO',
}

