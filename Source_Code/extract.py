import csv
import string
import os
from google.cloud import storage

# Upload CSV file to GCS bucket
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f'File {source_file_name} upload to {destination_blob_name} in {bucket_name}')

# Set GCS Bucket name, Input & destination file name
bucket_name = 'employee-data-etl'
source_file_name = 'C:\\Users\\siram\\OneDrive\\Desktop\\ETL Projects\\Raw data\\employee_raw_data.csv'
destination_blob_name = 'employee_raw_data'
source_folder = 'C:\\Users\\siram\\OneDrive\\Desktop\\ETL Projects\\Raw data\\'

# Upload data to GCS bucket
upload_to_gcs(bucket_name, source_file_name, destination_blob_name)

def upload_files(bucket_name, source_folder, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    for filename in os.listdir(source_folder):
        local_file_path = os.path.join(source_folder, filename)
        if os.path.isfile(local_file_path):
            blob.upload_from_file(local_file_path)
            print(f'File {filename} upload to {destination_blob_name} in {bucket_name}')

#upload_files(bucket_name, source_folder, destination_blob_name)



    
