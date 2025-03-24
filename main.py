import logging
import sys
import time
import os
import signal
from src.excel.processor import ExcelProcessor
from src.monitoring.file_watcher import start_file_monitoring
from src.configurations.config import EXCEL_CONFIG
from src.database.test_mssql_connection import test_mssql_connection

log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'application.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

os.chdir(os.path.dirname(os.path.abspath(__file__)))

running = True

def signal_handler(sig, frame):
    global running
    logging.info("Received stop signal. Shutting down...")
    running = False

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        processor = ExcelProcessor()
        logging.info("Starting Program")
        
        watch_folder = EXCEL_CONFIG['watch_folder']
        logging.info(f"Starting to monitor folder: {watch_folder}")
        
        processed_count = processor.watch_folder()
        
        if processed_count > 0:
            logging.info(f"Initial processing completed. {processed_count} Excel files processed.")
        else:
            logging.info("No Excel files found. Monitoring for new files...")
        
        observer = start_file_monitoring(watch_folder, processor.process_file)
        
        while running:
            time.sleep(1)
            
        observer.stop()
        observer.join()
        logging.info("Program completed.")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        
        if 'observer' in locals():
            observer.stop()
            observer.join()
            
        sys.exit(1)