import os
import logging
import datetime
import pandas as pd
from hdfs import InsecureClient

# Tell Kaggle to look in the current project folder for kaggle.json, NOT the hidden ~/.kaggle folder
os.environ['KAGGLE_CONFIG_DIR'] = os.path.dirname(os.path.abspath(__file__))

# Now import Kaggle (it will find the file sitting right next to this script)
from kaggle.api.kaggle_api_extended import KaggleApi


# 2. Setup Logging (Records successes, warnings, and errors)
logging.basicConfig(
    filename='ingestion_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    # --- Configuration Variables ---
    dataset_slug = 'daveianhickey/2000-16-traffic-flow-england-scotland-wales'
    download_path = './raw_data'
    hdfs_url = 'http://localhost:9870' # Default WebHDFS port
    hdfs_user = 'saad' # Change this if your HDFS username is different
    
    logging.info("--- Starting Automated Ingestion Pipeline ---")

    # 2. Load: Download Data via Kaggle API
    try:
        logging.info(f"Connecting to Kaggle to download {dataset_slug}")
        api = KaggleApi()
        api.authenticate() 
        api.dataset_download_files(dataset_slug, path=download_path, unzip=True)
        logging.info("Download and extraction successful.")
    except Exception as e:
        logging.error(f"Kaggle download failed: {e}. Check your kaggle.json file.")
        return

    # 3. Validate: Pre-upload checks
    csv_files = [f for f in os.listdir(download_path) if f.endswith('.csv')]
    if not csv_files:
        logging.error("No CSV files found after extraction.")
        return
        
    for file in csv_files:
        file_path = os.path.join(download_path, file)
        
        # Check File Size
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logging.info(f"Validating {file}: Size is {file_size_mb:.2f} MB")
        
        try:
            # Check Encoding and Row Count
            # We read the file line-by-line to count rows without overloading RAM
            with open(file_path, 'r', encoding='utf-8') as f:
                row_count = sum(1 for row in f) - 1 # Subtract 1 for the header
            logging.info(f"Validation passed for {file}. Total rows: {row_count}. Encoding: UTF-8.")
        except Exception as e:
            logging.error(f"Validation failed for {file}. Error: {e}")
            return

    # 4. Organize: Connect to HDFS and create partitioned directories
    try:
        client = InsecureClient(hdfs_url, user=hdfs_user)
        
        # Dynamically create the path based on the current date
        current_date = datetime.datetime.now()
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        
        hdfs_dir = f'/warehouse/raw/uk_road_safety/year={year}/month={month}/'
        
        # Create directories in HDFS
        client.makedirs(hdfs_dir)
        logging.info(f"Created HDFS directory structure: {hdfs_dir}")
    except Exception as e:
        logging.error(f"Failed to connect to HDFS or create directories: {e}")
        return

    # 5. Upload: Push files to HDFS
    for file in csv_files:
        local_file_path = os.path.join(download_path, file)
        hdfs_file_path = f"{hdfs_dir}{file}"
        
        try:
            logging.info(f"Uploading {file} to HDFS at {hdfs_file_path}...")
            client.upload(hdfs_file_path, local_file_path, overwrite=True)
            logging.info(f"Upload successful for {file}.")
        except Exception as e:
            logging.error(f"Failed to upload {file}: {e}")
            return
            
    logging.info("--- Ingestion Pipeline Completed Successfully ---")

if __name__ == "__main__":
    # Ensure the local download folder exists before starting
    if not os.path.exists('./raw_data'):
        os.makedirs('./raw_data')
    main()