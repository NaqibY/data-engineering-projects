from datetime import timedelta,datetime
import os
import pandas as pd
import urllib.request
from zipfile import ZipFile
from itertools import islice

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.api_core.exceptions import GoogleAPIError

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
    
dag = DAG(dag_id='project_four',
          description= ' csv_to_mysql_to_bigquery', 
          schedule_interval= timedelta(hours=1), 
          default_args=default_args, 
          start_date=datetime(2021, 3, 4, 13-8, 30),        # it start 24 hours after set date, if you want to start today, set it yesterday            
          tags=['test'])                                    # it follows utc time, if in malaysia's time +08.00

func_param = {
    'download_url': 'http://eforexcel.com/wp/wp-content/uploads/2017/07/50000-Sales-Records.zip', 
    'destination_folder': '{}/airflow/dags/data/'.format(os.getcwd()), 
    'filename': '50000 Sales Records.csv',
    'create_table_query_file':'{}/airflow/dags/data/sales_records.sql'.format(os.getcwd()),
    'insert_query_file': '{}/airflow/dags/data/sales_insert.sql'.format(os.getcwd()),
    'bucket_name':'data-engineering-data-sources',
    'source_transform':'/tmp/sales-{}.pq'.format(datetime.now().date()),
    'dest_blob_transform':'transformed/project_four_aiflow/sales-{}.pq'.format(datetime.now().date())}


#PWD /home/user_name/
def retrive_csv_file(download_url,destination_folder):
    """
    Get file from url

    Arg:
        1. download_url
        2. destination_folder= downloaded files directory

    """

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    urllib.request.urlretrieve(download_url, destination_folder+download_url.split('/')[-1])
    with ZipFile(destination_folder+download_url.split('/')[-1]) as zipobj:
        zipobj.extractall(destination_folder)

        print('download and extraction complete')

def csv_load_to_db(destination_folder, filename , insert_query_file, by_rows_batch=10000):
    """
    parse csv file and execute query to load into database.
    
    Arg: 
        1. filename = name of csv file 'filename.csv'
        2. destination_folder = downloaded files directory 'data/'
        3. insert_query = dir or .sql file ,'path/local/query.sql'
    
    """    
    csv_file=open(destination_folder+filename, 'r')
    sql_file=open(insert_query_file, 'r')
    sql=sql_file.read()
    insert_query=sql.split(';')[1]

    conn=MySqlHook(mysql_conn_id='mysql_localhost').get_conn()
    cur=conn.cursor()
    cur.execute('use sales_records_airflow')
    cur.execute('select count(*) from sales LIMIT 1')
    row_count=cur.fetchone()[0]+1    # add one because we want to exclude header when slicing csv for loop

    if row_count is 1:
        print('empty')
        for row in islice(csv_file,row_count, row_count+by_rows_batch):    # start 1, stop 10000 return 10000 rows 
            val=row.rstrip().split(',')
            dt1=datetime.strptime(val[5], '%m/%d/%Y').date()
            dt2=datetime.strptime(val[7], '%m/%d/%Y').date()
            val[5]=dt1
            val[7]=dt2
            params= val
            cur.execute(query=insert_query, args=params)
            conn.commit()

    elif row_count > 1:
        print('not empty')
        for row in islice(csv_file,row_count,row_count+by_rows_batch):     # previous rows add 1 start at 10001, stop at 10001+10000 return 10000 rows end at row 20000
            val=row.rstrip().split(',')
            dt1=datetime.strptime(val[5], '%m/%d/%Y').date()
            dt2=datetime.strptime(val[7], '%m/%d/%Y').date()
            val[5]=dt1
            val[7]=dt2
            params= val
            cur.execute(query=insert_query, args=params)
            conn.commit()
    elif row_count==50001: pass
    conn.close()
    csv_file.close()

def mysql_to_pq(source_transform, name_of_dataset='project_four_airflow', by_row_batch=1000):
    '''
    extract mysql database and save into local pq ``tmp/sales-date.pq``. this function take the last rows of bq dataset and compared againts current
    mysql database to avoid duplication, only extract load new data from mysql to bq. if dataset not exist it will create dataset using name given
    
    Args:

        1. source_transform = 'path/local/file.pq'

        2. by_row_batch = number of row you want to extract ``int``

    return: 
        ``str`` of local pq file path
    '''
    client=BigQueryHook(gcp_conn_id='google_cloud_default').get_client()
    row_id=client.query('select id from project_four_airflow.sales order by id desc limit 1')
    try:
        for i in row_id:
            last_row_id=i[0]
            print(i[0])
    except GoogleAPIError:
        row_id.error_result['reason']=='notFound'
        last_row_id=0
        print('no dataset.table')
        client.create_dataset(name_of_dataset)
        print('new dataset, {} created'.format(name_of_dataset))
    conn=MySqlHook(mysql_conn_id='mysql_localhost').get_conn()
    cur=conn.cursor()
    cur.execute('use sales_records_airflow')
    cur.execute('select * from sales where id>={} and id<={}'.format(last_row_id+1, last_row_id+by_row_batch))

    list_row=cur.fetchall()
    rows_of_extracted_mysql=[]
    for i in list_row:
        rows_of_extracted_mysql.append(list(i))
    print('extracting from mysql')
    df=pd.DataFrame(rows_of_extracted_mysql, columns=['id', 'region', 'country', 'item_type', 'sales_channel', 'Order Priority',
                                                'order_date','order_id','ship_date', 'units_sold', 'unit_price', 'unit_cost',
                                                'total_revenue', 'total_cost',
                                                'total_profit'])
    df.to_parquet(source_transform)
    print('task complete check,',source_transform)

def check_data(task_instance, create_table_query_file):
    conn=MySqlHook(mysql_conn_id='mysql_localhost').get_conn()
    cur=conn.cursor()
    try:
        cur.execute('use sales_records_airflow')
        cur.execute('select count(*) from sales')
        total_rows=cur.fetchone()[0]
        task_instance.xcom_push(key='mysql_total_rows', value=total_rows)
        if type(total_rows) is int:
            print('appending new data')
            return 'csv_file_exist'
        elif total_rows==50000:
            print('up to date')
            return 'check_dataset'
    except cur.OperationalError:
        print('sql_file execute')
        sql_file=open(create_table_query_file, 'r')
        sql_query=sql_file.read()
        for query in sql_query.split(';', maxsplit=2):
            cur.execute('{}'.format(query))
            conn.commit()
        return 'csv_file_not_exist'

def check_dataset(task_instance):
    client=BigQueryHook(gcp_conn_id='google_cloud_default').get_client()
    rows=client.query('select count(*) from project_four_airflow.sales')
    mysql_total_rows=task_instance.xcom_pull(key='mysql_total_rows',task_ids='check_data')
    try:
        for item in rows:
            dataset_total_rows=item[0]
            if dataset_total_rows== mysql_total_rows:
                print('bigquery_is_up_to_date')
                return 'bigquery_is_up_to_date'
            elif dataset_total_rows < mysql_total_rows:
                print('rows in bq dataset is not up to date to mysql')
                return 'extract_mysql_to_local_pq'
    except GoogleAPIError: 
        rows.error_result['reason']=='notFound'
        print(rows.error_result)
        return 'extract_mysql_to_local_pq'
    

check_data=BranchPythonOperator(task_id='check_data',
                                python_callable=check_data,
                                op_kwargs=func_param,
                                dag=dag)

download_zip=PythonOperator(task_id='download_zip',
                            python_callable=retrive_csv_file,
                            op_kwargs=func_param,
                            trigger_rule='none_skipped',
                            dag=dag)

load_to_mysql=PythonOperator(task_id='load_to_mysql',
                              python_callable=csv_load_to_db,
                              op_kwargs=func_param,
                              trigger_rule='none_failed_or_skipped',
                              dag=dag)

check_dataset=BranchPythonOperator(task_id='check_dataset',
                                   python_callable=check_dataset,
                                   dag=dag,
                                   trigger_rule='none_failed_or_skipped')

extract_mysql_to_local_pq=PythonOperator(task_id='extract_mysql_to_local_pq',
                              python_callable=mysql_to_pq,
                              op_kwargs=func_param,
                              trigger_rule='all_done',
                              dag=dag)


local_pq_to_gcs= LocalFilesystemToGCSOperator(task_id='local_pq_to_gcs',
                                                    src=func_param['source_transform'],
                                                    dst=func_param['dest_blob_transform'],
                                                    bucket=func_param['bucket_name'],
                                                    gcp_conn_id='google_cloud_default',
                                                    trigger_rule='all_done',
                                                    dag=dag)

load_gcs_pq_to_bq=GCSToBigQueryOperator(task_id='load_gcs_pq_to_bq',
                                        bucket=func_param['bucket_name'],
                                        source_objects=[func_param['dest_blob_transform']],     #default take list of objects, 
                                        destination_project_dataset_table='project_four_airflow.sales',
                                        source_format='PARQUET',
                                        write_disposition='WRITE_APPEND',
                                        google_cloud_storage_conn_id='google_cloud_default',
                                        trigger_rule='all_done',
                                        dag=dag)


                                    
bigquery_is_up_to_date= DummyOperator(task_id='bigquery_is_up_to_date')
csv_file_not_exist= DummyOperator(task_id='csv_file_not_exist')
csv_file_exist= DummyOperator(task_id='csv_file_exist')
check_mysql= DummyOperator(task_id='check_mysql')
updating_dataset= DummyOperator(task_id='updating_dataset')



check_data>>check_mysql>>check_dataset
check_data>>csv_file_not_exist>>download_zip>>load_to_mysql>>check_dataset
check_data>>csv_file_exist>>load_to_mysql>>check_dataset
check_dataset>>updating_dataset>>extract_mysql_to_local_pq>>local_pq_to_gcs>>load_gcs_pq_to_bq
check_dataset>>bigquery_is_up_to_date
