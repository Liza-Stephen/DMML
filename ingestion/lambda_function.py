from dotenv import load_dotenv
from datetime import datetime, timezone
import requests
import os
import zipfile
import boto3
import pg8000
import csv
import logging
import json

load_dotenv()

KAGGLE_URL = f"https://www.kaggle.com/api/v1/datasets/download/blastchar/telco-customer-churn"

file_path = "data/kaggle_dataset"
zip_file_path = file_path +".zip"

db_file_path = "data/db_dataset"

S3_BUCKET = "dmml-assignment-liza"
S3_KAGGLE_FOLDER = "raw/kaggle-data"
S3_DB_FOLDER = "raw/db-data"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Authenticate with Kaggle API
def fetch_kaggle_data():
    logger.info("Downloading dataset from Kaggle API...")

    response = requests.get(KAGGLE_URL)

    if response.status_code == 200:
        with open(zip_file_path, "wb") as file:
            file.write(response.content)
        logger.info(f"Dataset downloaded successfully: {zip_file_path}")

        try:
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(file_path)
            logger.info(f"Dataset extracted successfully: {file_path}")

            csv_files = [file for file in os.listdir(file_path) if file.endswith(".csv")]
            if not csv_files:
                raise FileNotFoundError

        except zipfile.BadZipFile:
            logger.error("Error: The downloaded file is not a valid zip file.")

        except FileNotFoundError:
            logger.error("No CSV file found in the Kaggle dataset directory.")

    else:
        logger.error(f"Failed to download dataset. HTTP {response.status_code}: {response.text}")


def upload_data_to_s3(file_path, folder):
    logger.info("Uploading to S3...")

    now = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(timestamp)

    s3_client = boto3.client("s3")

    for file in os.listdir(file_path):
        local_file_path = os.path.join(file_path, file)
        file_name_with_timestamp = f"{os.path.basename(file_path)}_{timestamp}"
        s3_key = f"{folder}/{now}/{file_name_with_timestamp}"

        try:
            s3_client.upload_file(local_file_path, S3_BUCKET, s3_key)
            logger.info(f"Uploaded {file} to s3://{S3_BUCKET}/{s3_key}")

        except Exception as e:
            logger.error(f"Failed to upload {file} to S3: {e}")


def write_csv(rows, cursor):
    logger.info("Writing to csv...")

    csv_file = db_file_path+"/customers_data.csv"
    os.makedirs("data/db_dataset", exist_ok=True)

    with open(csv_file, "w", newline="") as file:
        writer = csv.writer(file)
        column_names = [desc[0] for desc in cursor.description]
        writer.writerow(column_names)
        writer.writerows(rows)

    logger.info(f"Data saved to {csv_file}")


def connect_rds():
    logger.info("Connecting to RDS...")
    try:
        conn = pg8000.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST")
        )

        cur = conn.cursor()
        query = "SELECT * FROM customers;"
        cur.execute(query)
        rows = cur.fetchall()

        write_csv(rows, cur)

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Failed to connect: {e}")


def ingest_data():
    logger.info("Lambda function triggered")
    # logger.debug(f"Event: {json.dumps(event, indent=2)}")
    # logger.info(f"Function Name: {context.function_name}")
    # logger.info(f"Request ID: {context.aws_request_id}")

    fetch_kaggle_data()
    upload_data_to_s3(file_path, S3_KAGGLE_FOLDER)
    os.remove(zip_file_path)

    connect_rds()
    upload_data_to_s3(db_file_path, S3_DB_FOLDER)


ingest_data()
