from google.cloud import storage
import os

def upload_file_to_gcs(bucket_name,file_paths):
    # Initialize a client using the service account credentials
    client = storage.Client.from_service_account_json('C:/Users/hangl/UNIGAP/Project05/GCS authentication/changproj-0b0714e7806e.json')
    bucket = client.get_bucket(bucket_name)
    blobs = []

    try:
        for file_path in file_paths:
            blob = bucket.blob(file_path)
            blob.upload_from_filename(file_path)
            blobs.append(blob)
            print(f"A file {file_path} has uploaded")
    except Exception as e:
        #handle any exceptions that occured during the transaction
        print(f"An error occured: {str(e)}")

bucket_name = 'tikiproducts'
file_paths = ['test_split_files/'+f for f in os.listdir('test_split_files')]

upload_file_to_gcs(bucket_name,file_paths)
# print(file_paths[0])
# print(file_paths[0][len('test_split_files')+1:])
# print(len('test_split_files'))