# Import the Google Cloud Storage client, os for path handling, dotenv for environment variables, and glob for file listing
from google.cloud import storage
import os
from dotenv import load_dotenv
import glob

def upload_to_gcs(bucket_name, local_folder):
    """
    Uploads all files from the specified local directory to Google Cloud Storage.

    Args:
    - bucket_name (str): Name of the GCS bucket where files will be uploaded.
    - local_folder (str): Local directory of the files to be uploaded.
    """

    # Authenticate with Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Find all files in the local directory
    files = glob.glob(os.path.join(local_folder, '*'))

    # Iterate over the list of files and upload each one
    for source_file_name in files:
        file_name = os.path.basename(source_file_name)
        destination_blob_name = file_name

        # Create a blob object for file upload
        blob = bucket.blob(destination_blob_name)
        # Perform the file upload to GCS
        blob.upload_from_filename(source_file_name)
        # Output the result to console
        print(f'File {file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.')

# Main execution block to run the upload process
if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Set the parameters for the upload
    bucket_name = os.getenv('BUCKET_NAME')
    local_folder = './Datasets/'

    # Call the function to upload files to GCS
    upload_to_gcs(bucket_name, local_folder)
