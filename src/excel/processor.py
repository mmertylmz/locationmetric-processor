import pandas as pd
import os
import uuid
import logging
import concurrent.futures
from datetime import datetime, timezone
from sqlalchemy.exc import SQLAlchemyError
from ..database.database import get_session, execute_with_retry
from ..database.models import OutscraperLocation, OutscraperLocationMetric
from ..configurations.config import EXCEL_CONFIG, TARGET_COLUMNS
from ..utils.helpers import ensure_directory_exists, clean_data_frame
from collections import defaultdict

class ExcelProcessor:
    def __init__(self, batch_size=50, max_workers=4):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.setup_logging()

    def setup_logging(self):
        from src.configurations.config import LOG_CONFIG
        import os
        import sys

        log_folder = LOG_CONFIG['log_folder']
        ensure_directory_exists(log_folder)

        log_file = os.path.join(log_folder, "application.log")

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
            df = clean_data_frame(df)

            total_rows = len(df)
            logging.info(f"File cleaned successfully: {file_path}, {total_rows} rows found.")

            total_batches = (total_rows + self.batch_size - 1) // self.batch_size
            locations_added = 0
            locations_updated = 0
            types_added = 0
            types_updated = 0
            metrics_added = 0
            error_batches = 0

            logging.info(f"Starting batch processing with {self.max_workers} worker threads: 0/{total_batches} (0%)")
            batches = []
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, total_rows)
                batches.append(df.iloc[start_idx:end_idx])

            batch_results = []
            completed = 0
            last_logged_percentage = 0

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_batch = {executor.submit(self._process_batch, batch) : i for i, batch in enumerate(batches)}

                for future in concurrent.futures.as_completed(future_to_batch):
                    batch_num = future_to_batch[future]
                    completed += 1

                    try:
                        result = future.result()
                        batch_results.append(result)

                        locations_added += result.get('locations_added', 0)
                        locations_updated += result.get('locations_updated', 0)
                        metrics_added += result.get('metrics_added', 0)
                        types_added += result.get('types_added', 0)
                        types_updated += result.get('types_updated', 0)

                        current_percentage = int(completed / total_batches * 100)
                        if current_percentage - last_logged_percentage >= 10 or completed == total_batches:
                            logging.info(f"Progress: {completed}/{total_batches} batches ({current_percentage}%)")
                            last_logged_percentage = current_percentage
                    except Exception as e:
                        error_batches += 1
                        logging.error(f"Error processing batch {batch_num + 1}/{total_batches}: {e}")
            
            if error_batches > 0:
                logging.warning(f"{error_batches} out of {total_batches} batches failed.")

            if error_batches < total_batches:
                logging.info(f"File processed with success: {file_path}")
                logging.info(f"Total records: {locations_added} locations added, " +
                             f"{locations_updated} locations updated, {metrics_added} metrics added, " +
                             f"{types_added} types added, {types_updated} types updated.")

                self.move_processed_file(file_path)
                return True
            else:
                logging.error(f"File processing completely failed: {file_path}")
                return False

        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            return False

    def _process_batch(self, batch_df):
        session = get_session()
        results = {
            'locations_added': 0,
            'locations_updated': 0,
            'metrics_added': 0,
            'types_added': 0,
            'types_updated': 0
        }

        try:
            unique_types = set()
            for _, row in batch_df.iterrows():
                type_value = str(row.get('type', '')).strip()
                if type_value and type_value.lower() not in ('nan', 'none', ''):
                    unique_types.add(type_value)

            existing_types = {}
            if unique_types:
                from ..database.models import OutscraperLocationTypes
                db_types = session.query(OutscraperLocationTypes).filter(
                    OutscraperLocationTypes.Name.in_(unique_types)
                ).all()

                for type_obj in db_types:
                    existing_types[type_obj.Name] = type_obj

                for type_name in unique_types:
                    if type_name not in existing_types:
                        new_type = OutscraperLocationTypes(
                            Id=uuid.uuid4(),
                            Name=type_name
                            )
                        session.add(new_type)
                        existing_types[type_name] = new_type
                        results['types_added'] += 1
                    else:
                        results['types_updated'] += 1

            for _, row in batch_df.iterrows():
                try:
                    google_id = row.get('google_id')
                    existing_location = None

                    if google_id and str(google_id).strip() not in ('', 'nan', 'None'):
                        with session.no_autoflush:
                            existing_location = OutscraperLocation.find_by_google_id(session, google_id)

                    try:
                        rating = float(row.get('rating', 0.0))
                    except:
                        rating = 0.0

                    try:
                        reviews = int(row.get('reviews', 0))
                    except:
                        reviews = 0

                    try:
                        reviews_per_score1 = int(row.get('reviews_per_score_1', 0))
                        reviews_per_score2 = int(row.get('reviews_per_score_2', 0))
                        reviews_per_score3 = int(row.get('reviews_per_score_3', 0))
                        reviews_per_score4 = int(row.get('reviews_per_score_4', 0))
                        reviews_per_score5 = int(row.get('reviews_per_score_5', 0))
                    except:
                        reviews_per_score1 = reviews_per_score2 = reviews_per_score3 = reviews_per_score4 = reviews_per_score5 = 0

                    try:
                        photos_count = int(row.get('photos_count', 0))
                    except:
                        photos_count = 0

                    try:
                        latitude = float(row.get('latitude', 0.0))
                        longitude = float(row.get('longitude', 0.0))
                    except:
                        latitude = longitude = 0.0

                    postal_code = str(row.get('postal_code', ''))
                    if postal_code.lower() in ['nan', 'none']:
                        postal_code = ''

                    name = str(row.get('name', ''))
                    type_value = str(row.get('type', ''))
                    phone = str(row.get('phone', ''))
                    full_address = str(row.get('full_address', ''))
                    state = str(row.get('state', ''))
                    location_link = str(row.get('location_link', ''))
                    place_id = str(row.get('place_id', ''))
                    verified = bool(row.get('verified', False))
                    country = str(row.get('country', ''))
                    country_code = str(row.get('country_code', ''))
                    time_zone = str(row.get('time_zone', ''))

                    metric_id = uuid.uuid4()
                    current_date = datetime.now().replace(tzinfo=timezone.utc)
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
                        CreateDate=current_date,
                        Year=current_date.year,
                        Month=current_date.month
                    )
                    session.add(metric)
                    results['metrics_added'] += 1

                    if existing_location:
                        existing_location.MetricId = metric_id

                        if name.strip():
                            existing_location.Name = name
                        if type_value.strip():
                            existing_location.Type = type_value
                        if phone.strip():
                            existing_location.Phone = phone
                        if full_address.strip():
                            existing_location.FullAddress = full_address
                        if postal_code.strip():
                            existing_location.PostalCode = postal_code
                        if state.strip():
                            existing_location.State = state

                        if latitude != 0.0:
                            existing_location.Latitude = latitude
                        if longitude != 0.0:
                            existing_location.Longitude = longitude

                        existing_location.Verified = verified

                        if location_link.strip():
                            existing_location.LocationLink = location_link

                        if country.strip():
                            existing_location.Country = country

                        if country_code.strip():
                            existing_location.CountryCode = country_code
                        
                        if time_zone.strip():
                            existing_location.Timezone = time_zone

                        metric.LocationId = existing_location.Id
                        results['locations_updated'] += 1
                    else:
                        location_id = uuid.uuid4()
                        location = OutscraperLocation(
                            Id=location_id,
                            MetricId=metric_id,
                            PlaceId=place_id,
                            GoogleId=google_id,
                            Name=name,
                            Type=type_value,
                            Phone=phone,
                            FullAddress=full_address,
                            PostalCode=postal_code,
                            State=state,
                            Latitude=latitude,
                            Longitude=longitude,
                            Verified=verified,
                            LocationLink=location_link,
                            Country = country,
                            CountryCode = country_code,
                            Timezone = time_zone
                        )
                        session.add(location)

                        metric.LocationId = location_id
                        results['locations_added'] += 1

                except Exception as row_error:
                    if not hasattr(self, '_error_counts'):
                        self._error_counts = defaultdict(int)
                    error_msg = str(row_error)
                    self._error_counts[error_msg] += 1
                    
                    if self._error_counts[error_msg] <= 3:
                        logging.error(f"Error processing row: {error_msg}")
                    elif self._error_counts[error_msg] == 4:
                        logging.error(f"Error processing row: {error_msg} (suppressing further identical errors)")
                
                    session.rollback()

                if results['metrics_added'] % 50 == 0:
                    try:
                        session.commit()
                        logging.debug(f"Intermediate commit successful after {results['metrics_added']} metrics added.")
                    except SQLAlchemyError as commit_error:
                        session.rollback()
                        logging.error(f"Error during intermediate commit: {commit_error}")

            try:
                # Commit changes with retry logic
                execute_with_retry(session, lambda s: s.commit())
                #logging.info(f"Final commit successful for batch with {results['metrics_added']} metrics.")
            except SQLAlchemyError as commit_error:
                session.rollback()
                logging.error(f"Error during final commit: {commit_error}")
                raise

            return results

        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"SQL error in batch processing: {e}")
            raise
        except Exception as e:
            session.rollback()
            logging.error(f"Unexpected error in batch processing: {e}")
            raise
        finally:
            session.close()

    def move_processed_file(self, file_path):
        try:
            file_name = os.path.basename(file_path)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            new_file_name = f"{timestamp}_{file_name}"

            archive_folder = EXCEL_CONFIG['archive_folder']
            ensure_directory_exists(archive_folder)

            new_path = os.path.join(archive_folder, new_file_name)

            counter = 1
            while os.path.exists(new_path):
                new_file_name = f"{timestamp}_{counter}_{file_name}"
                new_path = os.path.join(archive_folder, new_file_name)
                counter += 1

            os.rename(file_path, new_path)
            logging.info(f"File archived: {new_path}")
            return True
        except Exception as e:
            logging.error(f"Error moving processed file: {e}")
            return False

    def watch_folder(self):
        watch_folder = EXCEL_CONFIG['watch_folder']
        ensure_directory_exists(watch_folder)

        excel_files = [f for f in os.listdir(watch_folder) if f.endswith(('.xlsx', '.xls')) and
                       os.path.isfile(os.path.join(watch_folder, f))]

        if not excel_files:
            logging.info("No Excel files found to process.")
            return 0

        files_processed = 0
        files_failed = 0

        for file in excel_files:
            file_path = os.path.join(watch_folder, file)
            logging.info(f"Processing Excel file: {file_path}")

            try:
                if self.process_file(file_path):
                    files_processed += 1
                else:
                    files_failed += 1
            except Exception as e:
                files_failed += 1
                logging.error(f"Unhandled exception processing file {file_path}: {e}")

        logging.info(f"Watch folder processing complete. Processed: {files_processed}, Failed: {files_failed}")
        return files_processed