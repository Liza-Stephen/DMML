from dotenv import load_dotenv
import requests
import os
import zipfile
import boto3

load_dotenv()

KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")
KAGGLE_URL = f"https://www.kaggle.com/api/v1/datasets/download/blastchar/telco-customer-churn"

file_path = "data/kaggle_dataset"
zip_file_path = file_path +".zip"

S3_BUCKET = "dmml-assignment-liza"
S3_FOLDER = "raw/kaggle-data/"


# Authenticate with Kaggle API
def fetch_kaggle_data():
    print("Downloading dataset from Kaggle API...")

    response = requests.get(KAGGLE_URL, auth=(KAGGLE_USERNAME, KAGGLE_KEY))

    if response.status_code == 200:
        with open(zip_file_path, "wb") as file:
            file.write(response.content)
        print(f"Dataset downloaded successfully: {zip_file_path}")

        try:
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(file_path)
            print(f"Dataset extracted successfully: {file_path}")

            csv_files = [file for file in os.listdir(file_path) if file.endswith(".csv")]
            if not csv_files:
                raise FileNotFoundError

        except zipfile.BadZipFile:
            print("Error: The downloaded file is not a valid zip file.")

        except FileNotFoundError:
            print("No CSV file found in the Kaggle dataset directory.")

    else:
        print(f"Failed to download dataset. HTTP {response.status_code}: {response.text}")


def upload_data_to_s3():
    s3_client = boto3.client("s3")
    for file in os.listdir(file_path):
        local_file_path = os.path.join(file_path, file)
        s3_key = os.path.join(S3_FOLDER, file)
        try:
            s3_client.upload_file(local_file_path, S3_BUCKET, s3_key)
            print(f"Uploaded {file} to s3://{S3_BUCKET}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload {file} to S3: {e}")


def ingest_kaggle_data():
    fetch_kaggle_data()
    upload_data_to_s3()

    os.remove(zip_file_path)


ingest_kaggle_data()