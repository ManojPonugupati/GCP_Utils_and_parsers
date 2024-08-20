import os
import datetime
from google.cloud import storage

# Initialize the GCS client
client = storage.Client()


# Function to upload file to GCS bucket
def upload_to_gcs(source_file_path, bucket_name, destination_blob_name):
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name}.")


# Function to get list of directories in the GCS bucket
def list_gcs_directories(bucket_name, prefix):
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter='/')
    return [blob.name for blob in blobs.prefixes]


# Function to delete a GCS directory
def delete_gcs_directory(bucket_name, directory_name):
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=directory_name)
    for blob in blobs:
        blob.delete()
    print(f"Directory {directory_name} deleted.")


# Function to move the directory to backup location
def move_to_backup(bucket_name, old_directory, backup_directory):
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=old_directory)
    for blob in blobs:
        new_blob_name = blob.name.replace(old_directory, backup_directory, 1)
        bucket.rename_blob(blob, new_blob_name)
    print(f"Moved {old_directory} to {backup_directory}")


# Main function to handle the task
def manage_logging_directory(file_path, bucket_name, backup_bucket_name):
    # Calculate run_date in yyyyJJJ format
    run_date = datetime.datetime.utcnow().strftime('%Y%j')
    logging_path = f"logging/{run_date}/output/"

    # Upload the file to the GCS bucket
    upload_to_gcs(file_path, bucket_name, logging_path + os.path.basename(file_path))

    # List directories in logging path
    logging_directories = list_gcs_directories(bucket_name, 'logging/')

    # If there are more than 30 directories, move the oldest one to backup and delete it
    if len(logging_directories) > 30:
        logging_directories.sort()
        oldest_directory = logging_directories[0]

        # Move to backup location before deletion
        backup_directory = oldest_directory.replace('logging/', 'backup/')
        move_to_backup(bucket_name, oldest_directory, backup_directory)

        # Delete the old directory
        delete_gcs_directory(bucket_name, oldest_directory)


# Example usage
file_path = "/root/admin/output/your_file.txt"
bucket_name = "gcs_project_admin"
backup_bucket_name = "gcs_project_admin"  # Assuming backup is in the same bucket

manage_logging_directory(file_path, bucket_name, backup_bucket_name)
