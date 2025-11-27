import os
import logging
import sys
import pandas as pd
from google.cloud import storage
from src.logger import get_logger
from src.custom_exception import CustomException
from config.paths_config import *
from utils.common_functions import read_yaml

logger = get_logger(__name__)

class DataIngestion:
    def __init__(self, config):
        self.config = config["data_ingestion"]
        self.bucket_name = self.config["bucket_name"]
        self.file_names = self.config["bucket_file_names"]

        os.makedirs(RAW_DIR, exist_ok=True)

        logger.info("Data Ingestion Started....")

    def download_csv_from_gcp(self):
        try:
            # ✅ Use correct service account JSON
            client = storage.Client.from_service_account_json(
                r"D:\MLOPS\mlops-new-478508-efb27342a0d4.json"
            )

            bucket = client.bucket(self.bucket_name)

            for file_name in self.file_names:
                file_path = os.path.join(RAW_DIR, file_name)

                blob = bucket.blob(file_name)

                # Check if file exists in GCP bucket
                if not blob.exists():
                    raise Exception(f"GCP file '{file_name}' does not exist in bucket '{self.bucket_name}'")

                # Download file
                blob.download_to_filename(file_path)

                if file_name == "animelist.csv":
                    # Download only 5M rows for large file
                    data = pd.read_csv(file_path, nrows=5000000)
                    data.to_csv(file_path, index=False)
                    logger.info("Large file detected — Only 5M rows downloaded.")
                else:
                    logger.info(f"Downloaded smaller file: {file_name}")

        except Exception as e:
            logging.error(f"Original Error: {e}")
            raise CustomException(str(e), sys)

    def run(self):
        try:
            logger.info("Starting Data Ingestion Process....")
            self.download_csv_from_gcp()
            logger.info("Data Ingestion Completed...")
        except CustomException as ce:
            logger.error(f"CustomException : {str(ce)}")
        finally:
            logger.info("Data Ingestion DONE...")


if __name__ == "__main__":
    data_ingestion = DataIngestion(read_yaml(CONFIG_PATH))
    data_ingestion.run()

