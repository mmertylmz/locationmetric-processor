import pandas as pd
import os
import uuid
import logging
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from ..database.database import get_session
from ..database.models import OutscraperLocation, OutscraperLocationMetric
from config import EXCEL_CONFIG, TARGET_COLUMNS, DTYPE_DICT
from ..utils.helpers import ensure_directory_exists


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

            try:
                df = pd.read_excel(file_path, dtype=DTYPE_DICT)
                logging.info(f"File read with specified data types")
            except Exception as e:
                logging.warning(f"Error reading with specified data types: {e}. Trying without data types...")
                df = pd.read_excel(file_path)
                logging.info("File read with default data types")

            missing_columns = [col for col in TARGET_COLUMNS if col not in df.columns]
            if missing_columns:
                logging.warning(f"Missing columns in Excel: {missing_columns}")

            df = df[[col for col in TARGET_COLUMNS if col in df.columns]]

            df = self.clean_data_frame(df)

            logging.info(f"File cleaned successfully: {file_path}, {len(df)} rows found.")

            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            locations_added = 0
            locations_updated = 0
            metrics_added = 0
            error_batches = 0

            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(df))

                batch_df = df.iloc[start_idx:end_idx]

                try:
                    batch_results = self._process_batch(batch_df)

                    locations_added += batch_results.get('locations_added', 0)
                    locations_updated += batch_results.get('locations_updated', 0)
                    metrics_added += batch_results.get('metrics_added', 0)

                    logging.info(f"Batch {batch_num + 1}/{total_batches} processed: " +
                                 f"{batch_results.get('locations_added', 0)} locations added, " +
                                 f"{batch_results.get('locations_updated', 0)} locations updated, " +
                                 f"{batch_results.get('metrics_added', 0)} metrics added.")
                except Exception as e:
                    error_batches += 1
                    logging.error(f"Error processing batch {batch_num + 1}/{total_batches}: {e}")

            if error_batches > 0:
                logging.warning(f"{error_batches} out of {total_batches} batches failed.")

            if error_batches < total_batches:
                logging.info(f"File processed with some success: {file_path}")
                logging.info(f"Total records: {locations_added} locations added, " +
                             f"{locations_updated} locations updated, {metrics_added} metrics added.")

                self.move_processed_file(file_path)
                return True
            else:
                logging.error(f"File processing completely failed: {file_path}")
                return False

        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            return False

    def clean_data_frame(self, df):
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

                logging.info(f"Verified column processed. True count: {df['verified'].sum()}, "
                             f"False count: {len(df) - df['verified'].sum()}")

            if 'postal_code' in df.columns:
                df['postal_code'] = df['postal_code'].fillna('')
                df['postal_code'] = df['postal_code'].astype(str)
                df['postal_code'] = df['postal_code'].replace(r'\.0$', '', regex=True)
                df['postal_code'] = df['postal_code'].replace({'nan': '', 'None': '', 'NaN': ''})

            return df

        except Exception as e:
            logging.error(f"Error cleaning data frame: {e}")
            return df

    def _process_batch(self, batch_df):
        session = get_session()
        results = {
            'locations_added': 0,
            'locations_updated': 0,
            'metrics_added': 0
        }

        try:
            for _, row in batch_df.iterrows():
                try:
                    google_id = row.get('google_id')
                    existing_location = None

                    if google_id and str(google_id).strip() not in ('', 'nan', 'None'):
                        with session.no_autoflush:
                            existing_location = session.query(OutscraperLocation).filter(
                                OutscraperLocation.GoogleId == google_id
                            ).first()

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

                    # Boolean değerinden emin ol
                    verified = bool(row.get('verified', False))

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
                            LocationLink=location_link
                        )
                        session.add(location)

                        metric.LocationId = location_id
                        results['locations_added'] += 1

                except Exception as row_error:
                    logging.error(f"Error processing row: {row_error}")
                    session.rollback()
                if results['metrics_added'] % 10 == 0:
                    try:
                        session.commit()
                        logging.debug(f"Intermediate commit successful after {results['metrics_added']} metrics added.")
                    except SQLAlchemyError as commit_error:
                        session.rollback()
                        logging.error(f"Error during intermediate commit: {commit_error}")

            try:
                session.commit()
                logging.info(f"Final commit successful for batch with {results['metrics_added']} metrics.")
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

    def format_phone_number(self, phone_value):
        if pd.isna(phone_value) or phone_value == '':
            return ''

        if isinstance(phone_value, (float, int)):
            phone_str = str(int(phone_value)) if phone_value == int(phone_value) else str(phone_value)
        else:
            phone_str = str(phone_value)

        if phone_str.endswith('.0'):
            phone_str = phone_str[:-2]

        if phone_str.lower() in ['nan', 'none']:
            return ''

        return phone_str

    def clean_decimal(self, value, precision=19, scale=4):
        if pd.isna(value):
            return 0.0

        try:
            float_value = float(value)
            if not (float_value == float_value):
                return 0.0

            max_value = 10 ** (precision - scale) - 10 ** (-scale)
            if abs(float_value) > max_value:
                return 0.0

            return round(float_value, scale)
        except:
            return 0.0

    def clean_integer(self, value):
        if pd.isna(value):
            return 0

        try:
            int_value = int(float(value))
            return int_value
        except:
            return 0

    def process_files_in_folder(self, folder_path=None):
        if folder_path is None:
            folder_path = EXCEL_CONFIG['watch_folder']

        ensure_directory_exists(folder_path)

        excel_files = [f for f in os.listdir(folder_path) if f.endswith(('.xlsx', '.xls')) and
                       os.path.isfile(os.path.join(folder_path, f))]

        if not excel_files:
            logging.info(f"No Excel files found in folder: {folder_path}")
            return 0

        logging.info(f"Found {len(excel_files)} Excel files in folder: {folder_path}")

        files_processed = 0

        for file in excel_files:
            file_path = os.path.join(folder_path, file)
            if self.process_file(file_path):
                files_processed += 1

        return files_processed