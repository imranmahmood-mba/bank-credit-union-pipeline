from google.cloud import storage


def upload_file_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """
    Uploads a file to a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        source_file_path (str): Local path to the file to upload.
        destination_blob_name (str): Name to save the file as in the bucket.

    Returns:
        None
    """
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Upload the file
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)

    print(f"File {source_file_path} uploaded to {bucket_name}/{destination_blob_name}.")
