from dotenv import load_dotenv
import os
import psycopg2
import csv
import boto3

load_dotenv()

db_file_path = "data/db_dataset"

S3_BUCKET = "dmml-assignment-liza"
S3_FOLDER = "raw/db-data/"


def write_csv(rows, cursor):
    csv_file = db_file_path+"/customers_data.csv"

    with open(csv_file, "w", newline="") as file:
        writer = csv.writer(file)
        column_names = [desc[0] for desc in cursor.description]
        writer.writerow(column_names)
        writer.writerows(rows)

    print(f"Data saved to {csv_file}")


def upload_data_to_s3():
    s3_client = boto3.client("s3")
    for file in os.listdir(db_file_path):
        local_file_path = os.path.join(db_file_path, file)
        s3_key = os.path.join(S3_FOLDER, file)
        try:
            s3_client.upload_file(local_file_path, S3_BUCKET, s3_key)
            print(f"Uploaded {file} to s3://{S3_BUCKET}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload {file} to S3: {e}")


def connect_rds():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        cur = conn.cursor()
        query = "SELECT * FROM customers;"  # Change 'customers' to your table name
        cur.execute(query)
        rows = cur.fetchall()

        write_csv(rows, cur)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Failed to connect: {e}")


def ingest_db_data():
    connect_rds()
    upload_data_to_s3()


ingest_db_data()