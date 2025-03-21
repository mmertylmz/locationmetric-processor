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
    'server': r'DESKTOP-RHIE06K\SQLEXPRESS',
    'database': 'WR',
    'trusted_connection': 'yes' # Windows Authentication
}

EXCEL_CONFIG = {
    'watch_folder': './excel_files',
    'archive_folder': './excel_files/archives',
}

LOG_CONFIG = {
    'log_folder': './Logs',
    'log_level': 'INFO',
}

