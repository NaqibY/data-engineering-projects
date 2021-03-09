from google.cloud import bigquery as bq

def pq_to_bigquery(pq_path, dataset : str, table_id : str, client):
    client=client

    job_config=bq.LoadJobConfig(souce_format=bq.enums.SourceFormat.PARQUET)

    table_ref=client.dataset(dataset).table(table_id)

    with open(pq_path,'rb') as pq_file:
        load_job=client.load_table_from_file(pq_file, table_ref, job_config=job_config)

    
    load_job.result()


def df_to_bigquery(df, dataset :str, table_id : str, client):
    client=client

    job_config=bq.LoadJobConfig(write_disposition='WRITE_APPEND')

    table_ref=client.dataset(dataset).table(table_id)

    load_job=client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    load_job.result()
