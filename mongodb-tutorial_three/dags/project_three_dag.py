from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import json
import pandas as pd
from bson import ObjectId
from itertools import islice
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)}
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': True,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
dag = DAG( dag_id='project_three', 
           description= 'mongodb_tripdata_to_bigquery_task_2',
           schedule_interval= timedelta(days=1), 
           default_args=default_args, 
           start_date=datetime(2021, 2, 28, 20-8, 30), 
           end_date=datetime(2021, 3, 20, 20-8, 30), 
           tags=['test'])

func_param={
    'dbs':'test',
    'coll':'tripdata',
    'bucket_name':'data-engineering-data-sources',
    'dest_blob_initial':'raw/project_three_airflow/tripdata-{}.json',                                   
    'dest_blob':'raw/project_three_airflow/tripdata-{}.json'.format(datetime.now().date()),
    'dest_blob_transform':'transformed/project_three_airflow/tripdata-{}.pq'.format(datetime.now().date()),
    'source':'/tmp/tripdata-{}.json'.format(datetime.now().date()),
    'source_transform':'/tmp/tripdata-{}.pq'.format(datetime.now().date())}

def get_initial_id(bucket_name, dest_blob_initial):
    """
    get object_id from last extracted tripdata-yesterday.json in cloudstorage
    
    Return:
        ObjectId()
    TODO: this function only return bson.Objectid(), change to any keys according to your specific id in mongodb schema
    """
    list_of_blob=GCSHook(gcp_conn_id='google_cloud_default').list(bucket_name=bucket_name)

    for blob in list_of_blob:
        name=blob.name
        if name==dest_blob_initial.format(datetime.now().date()-datetime.timedelta(days=1)):
            print(blob.name)
            file_exist=blob.name
            downloaded_blob='/tmp/tripdata-{}.json'.format(datetime.now().date()-datetime.timedelta(days=1))
            GCSHook(gcp_conn_id='google_cloud_default').download(bucket_name=bucket_name, object_name=file_exist, filename=downloaded_blob)
            # load.download_blob(bucket_name, file_exist,downloaded_blob)
            with open(downloaded_blob, 'r') as previous_tripdata:
                json_yesterday=json.loads(previous_tripdata)

            index_id=json_yesterday[-1]['_id']
            object_id=ObjectId(index_id)            
        else:
            object_id=None
    
    initial_id=object_id
    return initial_id

def check_data(fetch_last_id, task_instance):
    
    last_objectid_from_transformed_pq=task_instance.xcom_pull(task_ids='first_run')
    with fetch_last_id:
        fetch_last_id=MongoHook(conn_id='mongo_localhost').find().sort({'id': -1})

        for doc in fetch_last_id:
            last_objectid_from_mongodb=doc['_id']
            if last_objectid_from_mongodb==last_objectid_from_transformed_pq:
                return 'bigquery_is_up_to_date'
            else:
                return 'get_data_from_mongodb'

def extract_mongodb(client, dbs, coll, source, task_instance, extract_by_batch=None):
    """
    export data from mongodb to json.
    
    Arg:
        client = ``MongoClient()``.

        dbs = name of database.
        
        coll = name of collection.
      
        initial_id = document id in ``objectID`` or any unique keys, default ``None``

        extract_by_batch = ``int`` batch of rows , default ``None`` 
    Return:
        list_of_docs
    """ 
    initial_id=task_instance.xcom_pull(task_ids='first_run')
    with client:
        fetch=MongoHook(conn_id='mongo_localhost').find(mongo_collection=coll,
                                                      mongo_db=dbs)
        list_of_docs=[]
        count=0
        if initial_id is not None:                                          # determine which row to start 
            for doc in fetch:
                count+=1
                if initial_id == None:
                    count=0
                    break
                if initial_id == doc['_id']:
                    break

        if extract_by_batch is None and initial_id is None:
            for docs in fetch:
                docs['_id']=str(docs['_id'])
                list_of_docs.append(docs)   
            print('extract all')
        elif extract_by_batch is None and initial_id is not None:
            for docs in islice(fetch, count):
                docs['_id']=str(docs['_id'])
                list_of_docs.append(docs)   
            print('extract all start at {}'.format(count))
        elif extract_by_batch is not None and initial_id is None:
            for docs in islice(fetch, 0, count+extract_by_batch):
                docs['_id']=str(docs['_id'])
                list_of_docs.append(docs)   
            print('extract_by_batch {} at {}'.format(extract_by_batch, count))
        elif extract_by_batch is not None and initial_id is not None:
            for docs in islice(fetch, count, count+extract_by_batch):
                docs['_id']=str(docs['_id'])
                list_of_docs.append(docs)   
            print('extract_by_batch {} at {}'.format(extract_by_batch, count))
        print(len(list_of_docs),"'s rows from {} is being extract'".format(coll))
        del fetch
        with open(source,'w') as json_tripdata:
            json.dump(list_of_docs, json_tripdata,indent=1)

    return list_of_docs

def transform_tripdata(source):
    '''
    transformation function task 2, format schema for bigquery
    
    Args:
        tripdata_file_path = 'local/path/file.json'

    return:
        tripdata dataframe
    '''
    df_raw=pd.read_json(source)
    def format_columns(df):                                                             ## Format columns name
        cols=df.columns
        cols=cols.str.lower()
        cols=cols.str.replace(' ','_', regex=True)
        return cols

    df_tripdata=df_raw.copy()
    df_tripdata.columns=format_columns(df_tripdata)
    df_tripdata['starttime']=pd.to_datetime(df_raw['starttime'])                        ## Change dataypes accordingly 
    df_tripdata['stoptime']=pd.to_datetime(df_raw['stoptime'])
    df_tripdata['start_station_id']=df_tripdata['start_station_id'].astype(str)
    df_tripdata['end_station_id']=df_tripdata['end_station_id'].astype(str)
    df_tripdata['bikeid']=df_tripdata['bikeid'].astype(str)
    df_tripdata['end_station_id']=df_tripdata['end_station_id'].astype(str)
    return df_tripdata


first_run = PythonOperator(task_id='first_run',
                            python_callable=get_initial_id,
                            op_kwargs=func_param
                            dag=dag)

check_data=BranchPythonOperator(task_id='check_data',
                                python_callable=check_data,
                                dag=dag)

get_data_from_mongodb = PythonOperator(task_id='get_data_from_mongodb',
                                        python_callable=extract_mongodb,
                                        op_kwargs=func_param,
                                        dag=dag)

load_to_staging= LocalFilesystemToGCSOperator(task_id='load_to_staging',
                                            src=func_param['source'],
                                            dst=func_param['dest_blob'],
                                            bucket=func_param['bucket_name'],
                                            gcp_conn_id='google_cloud_default',
                                            dag=dag)

# update_bigquery_fact_table=BranchPythonOperator(task_id='update_bigquery_fact_table',
#                                             python_callable=table_existence,
#                                             dag=dag)

transform_tripdata= PythonOperator(task_id='transform_tripdata',                        # pull xcom from extract_json
                                    python_callable=transform_tripdata,
                                    op_kwargs=func_param,
                                    dag=dag)

local_parquet_to_gcs= LocalFilesystemToGCSOperator(task_id='local_parquet_to_gcs',
                                                    src=func_param['source_transform'],
                                                    dst=func_param['dest_blob_transform'],
                                                    bucket=func_param['bucket_name'],
                                                    gcp_conn_id='google_cloud_default',
                                                    dag=dag)

load_gcs_pq_to_bq=GCSToBigQueryOperator(task_id='load_gcs_pq_to_bq',
                                        bucket=func_param['bucket_name'],
                                        source_objects=[func_param['dest_blob_transform']],
                                        destination_project_dataset_table='project_three_airflow.tripdata',
                                        source_format='PARQUET',
                                        write_disposition='WRITE_APPEND',
                                        google_cloud_storage_conn_id='google_cloud_default',
                                        dag=dag)

bigquery_is_up_to_date=DummyOperator(task_id='bigquery_is_up_to_date', dag=dag)


first_run>>check_data
check_data>>get_data_from_mongodb>>load_to_staging>>transform_tripdata>>local_parquet_to_gcs>>load_gcs_pq_to_bq
check_data>>bigquery_is_up_to_date