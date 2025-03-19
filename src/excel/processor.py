import pandas as pd
import os
import uuid
import logging
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from ..database.database import get_session
from ..database.models import OutscraperLocation, OutscraperLocationMetric
from config import EXCEL_CONFIG, TARGET_COLUMNS
from ..utils.helpers import ensure_directory_exists, get_timestamp


class ExcelProcessor:
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.setup_logging()

    def setup_logging(self):
        from config import LOG_CONFIG
        import os
        import sys

        log_folder = LOG_CONFIG['log_folder']
        ensure_directory_exists(log_folder)

        log_file = os.path.join(log_folder, f"{datetime.now().strftime('%Y-%m-%d')}.log")

        logging.basicConfig(
            level=getattr(logging, LOG_CONFIG['log_level']),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )

    def process_file(self, file_path):
        try:
            logging.info(f"Processing file: {file_path}")
            df = pd.read_excel(file_path)

            missing_columns = [col for col in TARGET_COLUMNS if col not in df.columns]
            if missing_columns:
                logging.warning(f"Missing columns in Excel: {missing_columns}")

            df = df[[col for col in TARGET_COLUMNS if col in df.columns]]

            logging.info(f"File read successfully: {file_path}, {len(df)} rows found.")

            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            locations_added = 0
            locations_updated = 0
            metrics_added = 0

            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(df))

                batch_df = df.iloc[start_idx:end_idx]
                batch_results = self._process_batch(batch_df)

                locations_added += batch_results.get('locations_added', 0)
                locations_updated += batch_results.get('locations_updated', 0)
                metrics_added += batch_results.get('metrics_added', 0)

                logging.info(f"Batch {batch_num + 1}/{total_batches} processed: " +
                             f"{batch_results.get('locations_added', 0)} locations added, " +
                             f"{batch_results.get('locations_updated', 0)} locations updated, " +
                             f"{batch_results.get('metrics_added', 0)} metrics added.")

            logging.info(f"File processed successfully: {file_path}")
            logging.info(f"Total records: {locations_added} locations added, " +
                         f"{locations_updated} locations updated, {metrics_added} metrics added.")

            self.move_processed_file(file_path)

            return True

        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            return False

    def _process_batch(self, batch_df):
        session = get_session()
        results = {
            'locations_added': 0,
            'locations_updated': 0,
            'metrics_added': 0
        }

        def safe_numeric(value):
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return value
            if isinstance(value, str):
                try:
                    if '.' in value:
                        return float(value)
                    else:
                        return int(value)
                except ValueError:
                    return None
            return None

        try:
            for _, row in batch_df.iterrows():
                google_id = row.get('google_id')
                existing_location = None

                if google_id:
                    with session.no_autoflush:
                        existing_location = session.query(OutscraperLocation).filter(
                            OutscraperLocation.GoogleId == google_id
                        ).first()

                rating = safe_numeric(row.get('rating'))
                reviews = safe_numeric(row.get('reviews'))
                reviews_per_score1 = safe_numeric(row.get('reviews_per_score_1'))
                reviews_per_score2 = safe_numeric(row.get('reviews_per_score_2'))
                reviews_per_score3 = safe_numeric(row.get('reviews_per_score_3'))
                reviews_per_score4 = safe_numeric(row.get('reviews_per_score_4'))
                reviews_per_score5 = safe_numeric(row.get('reviews_per_score_5'))
                photos_count = safe_numeric(row.get('photos_count'))

                latitude = safe_numeric(row.get('latitude'))
                longitude = safe_numeric(row.get('longitude'))
                postal_code = row.get('postal_code')
                if isinstance(postal_code, (int, float)):
                    postal_code = str(int(postal_code))

                metric_id = uuid.uuid4()
                metric = OutscraperLocationMetric(
                    Id=metric_id,
                    Rating=rating,
                    Reviews=reviews,
                    ReviewsPerScore1=reviews_per_score1,
                    ReviewsPerScore2=reviews_per_score2,
                    ReviewsPerScore3=reviews_per_score3,
                    ReviewsPerScore4=reviews_per_score4,
                    ReviewsPerScore5=reviews_per_score5,
                    PhotosCount=photos_count,
                    CreateDate=datetime.utcnow()
                )
                session.add(metric)
                results['metrics_added'] += 1

                if existing_location:
                    existing_location.MetricId = metric_id
                    existing_location.Name = row.get('name', existing_location.Name)
                    existing_location.Type = row.get('type', existing_location.Type)
                    existing_location.Phone = row.get('phone', existing_location.Phone)
                    existing_location.FullAddress = row.get('full_address', existing_location.FullAddress)
                    existing_location.PostalCode = postal_code
                    existing_location.State = row.get('state', existing_location.State)
                    existing_location.Latitude = latitude
                    existing_location.Longitude = longitude
                    existing_location.Verified = row.get('verified', existing_location.Verified)
                    existing_location.LocationLink = row.get('location_link', existing_location.LocationLink)

                    metric.LocationId = existing_location.Id
                    results['locations_updated'] += 1
                else:
                    location_id = uuid.uuid4()
                    location = OutscraperLocation(
                        Id=location_id,
                        MetricId=metric_id,
                        PlaceId=row.get('place_id'),
                        GoogleId=google_id,
                        Name=row.get('name'),
                        Type=row.get('type'),
                        Phone=row.get('phone'),
                        FullAddress=row.get('full_address'),
                        PostalCode=postal_code,
                        State=row.get('state'),
                        Latitude=latitude,
                        Longitude=longitude,
                        Verified=row.get('verified'),
                        LocationLink=row.get('location_link')
                    )
                    session.add(location)

                    metric.LocationId = location_id
                    results['locations_added'] += 1

            session.commit()
            return results
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"SQL error in batch processing: {e}")
            raise
        finally:
            session.close()

    def move_processed_file(self, file_path):
        file_name = os.path.basename(file_path)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_file_name = f"{timestamp}_{file_name}"

        archive_folder = EXCEL_CONFIG['archive_folder']
        ensure_directory_exists(archive_folder)

        new_path = os.path.join(archive_folder, new_file_name)
        os.rename(file_path, new_path)
        logging.info(f"File archived: {new_path}")

    def watch_folder(self):
        watch_folder = EXCEL_CONFIG['watch_folder']
        ensure_directory_exists(watch_folder)

        excel_files = [f for f in os.listdir(watch_folder) if f.endswith(('.xlsx', '.xls')) and
                       os.path.isfile(os.path.join(watch_folder, f))]

        if not excel_files:
            logging.info("No Excel files found to process.")
            return 0

        files_processed = 0
        for file in excel_files:
            file_path = os.path.join(watch_folder, file)
            logging.info(f"Processing Excel file: {file_path}")
            if self.process_file(file_path):
                files_processed += 1

        return files_processed