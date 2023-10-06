import functions_framework
import json
from google.cloud import bigquery, storage


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def load_jsonl_to_bigquery(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket_name = data["bucket"]
    name = data["name"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    # set the meta data
    meta_data = {
        'Cloud_event_id': event_id,
        'Cloud_event_type': event_type,
        'Bucket_name': bucket_name,
        'File_name': name,
        'Created': timeCreated,
        'Updated': updated,
        'Status_upload': "Successfully!"
    }

    # initialize cloud storage and big query clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    #specify your cloud storage bucket and big query dataset and table
    dataset_id = "project05_tiki"
    table_id = "products"

    # list jsonl files in the folder
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(name)

    print('blob is',blob)

    # initialize a list to hold rows to be inserted
    rows_to_insert = []

    with blob.open("r") as file:
        for line in file:
            row = json.loads(line)
            rows_to_insert.append(row)

    print('lengths of rows to insert are: ', len(rows_to_insert))

    # Create a job configuration with WRITE_APPEND
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')

    # insert rows into bigquery table
    if len(rows_to_insert):
        dataset_ref = bigquery_client.get_dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # insert data into bigquery table
        bigquery_client.load_table_from_json(rows_to_insert,table_ref,job_config=job_config)
        print("loaded to BigQuery")

    # UPLOAD Meta data
    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("Cloud_event_id", "STRING"),
        bigquery.SchemaField("Cloud_event_type", "STRING"),
        bigquery.SchemaField("Bucket_name", "STRING"),
        bigquery.SchemaField("File_name", "STRING"),
        bigquery.SchemaField("Created", "TIMESTAMP"),
        bigquery.SchemaField("Updated", "TIMESTAMP"),
        bigquery.SchemaField("Status_upload", "STRING")
    ]


    meta_table_id = "metadata"

    # Retrieve an existing BigQuery table
    meta_table_ref = bigquery_client.dataset(dataset_id).table(meta_table_id)
    metadata_table = bigquery_client.get_table(meta_table_ref)

    # Insert the row into BigQuery
    try:
        bigquery_client.insert_rows(metadata_table,[meta_data])
        print(meta_data)
    except Exception as e:
        print(meta_data)
        print('this is error',e)
    print('insert rows to metadata')