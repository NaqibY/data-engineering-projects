from google.cloud import storage
from google.cloud import bigquery as bq
from google.cloud.storage import bucket
from google.cloud.storage.blob import Blob

def pq_to_bigquery(pq_path, dataset : str, table_id : str):
    """
    load local parquet to bigquery

    Args:
        pq_path = 'local/path/file.pq'

        dataset = 'your-dataset-name'

        table_id = 'your-table-id'

    Return:
        ``load_job.result()``

    """
    client=bq.Client()
    job_config=bq.LoadJobConfig(source_format=bq.enums.SourceFormat.PARQUET)
    table_ref=client.dataset(dataset).table(table_id)
    
    with open(pq_path,'rb') as pq_file:
        load_job=client.load_table_from_file(pq_file, table_ref, job_config=job_config)
    return load_job.result()

def pq_gcs_to_bigquery(uri, dataset : str, table_id : str, write_disposition):
    """
    load local parquet to bigquery

    Args:
        uri = 'gs://bucket/blob.pq'

        dataset = 'your-dataset-name'

        table_id = 'your-table-id'

    Return:
        ``load_job.result()``

    """
    client=bq.Client()
    job_config=bq.LoadJobConfig(source_format=bq.enums.SourceFormat.PARQUET, write_disposition=write_disposition)
    dataset_id=client.create_dataset(dataset, exists_ok=True)
    table_ref=client.dataset(dataset).table(table_id)
    
    load_job=client.load_table_from_uri(uri,table_ref, job_config=job_config)
    return load_job.result()


def df_to_bigquery(df, dataset :str, table_id : str):
    """
    load Dataframe to bigquery

    Args:
        df = Dataframe ``object``

        dataset = 'your-dataset-name'

        table_id = 'your-table-id'

    Return:
        ``load_job.result()``

    """
    client=bq.Client()
    job_config=bq.LoadJobConfig(write_disposition='WRITE_APPEND')
    table_ref=client.dataset(dataset).table(table_id)
    load_job=client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    return load_job.result()

def load_blob(bucket_name, destination_blob, source_file_path):
    """
    upload file to cloudstorage

    Args:
        bucket_name = 'your-bucket-name'

        destination_blob = 'file/blob.filetype'

        source_file_path= 'local/path/blob'
    """
    client=storage.Client()
    bucket=client.bucket(bucket_name)
    blob=bucket.blob(destination_blob)

    return blob.upload_from_filename(source_file_path)

def download_blob(bucket_name, source_blob, destination_file_path):
    """
    upload file to cloudstorage

    Args:
        bucket_name = 'your-bucket-name'

        source_blob = 'file/blob.filetype'

        destination_file_path= 'local/path/blob'
    """
    client=storage.Client()
    bucket=client.bucket(bucket_name)
    blob=bucket.blob(source_blob)

    return blob.download_to_filename(destination_file_path)