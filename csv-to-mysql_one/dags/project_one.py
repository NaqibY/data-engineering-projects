import datetime
import timedelta
import json
import mysql.connector
from mysql.connector import errcode
import sys
import urllib.request
from zipfile import ZipFile

sys.path.insert('~/Documents/git/Simple-Python-ETL/')

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success
}

dag=DAG(
    dag_id= 'simple-python-etl',
    default_args = default_args,
    description= 'extract csv from http to mysql',
    schedule_interval = timedelta(hours=1),
    tags=['etl']

)
etl_arg= {
                "USER":mysql_username,
                "HOST":host_address,
                "DATABASE": names_of_database,
                "PASSWORD": mysql_password,
                "download_url":"http://eforexcel.com/wp/wp-content/uploads/2017/07/50000-Sales-Records.zip",
                "destination_folder":"/home/naqib/Documents/programming/Coursera/project/ETL Python/setup/data/"
}
        
with open('setup/config.json', 'r') as json_file:
    config=json.load(json_file)

db_connection=connection( 
    USER=config['mysql']['USER'],
    HOST=config['mysql']['HOST'],
    DATABASE=config['mysql']['DATABASES'],
    PASSWORD=config['mysql']['PASSWORD']
    )

download_url=config['mysql']['download_url']
destination_folder=config['mysql']['destination_folder']
filename= '50000 Sales Records.csv'

def extract(download_url,destination_folder):

    files.download_data(download_url,destination_folder)

    files.extract_file(download_url,destination_folder)

def transform_load(filename,download_url,destination_folder,db_connection):

    db_connection.init_query('DROP TABLE IF EXISTS Sales')

    db_connection.init_query(query=(
        '''CREATE TABLE Sales(
            Region varchar(256),
            Country varchar(256),
            `Item Type` varchar(20),
            `Sales Channel` varchar(20),
            `Order Priority` varchar(20),
            `Order Date` DATE NOT NULL,
            `Order ID` int(20) NOT NULL,
            `Ship Date` DATE NOT NULL,
            `Units Sold` int(20) NOT NULL,
            `Unit Price` int(20) NOT NULL,
            `Unit Cost` int(20) NOT NULL,
            `Total Revenue` int(20) NOT NULL,
            `Total Cost` int(20) NOT NULL,
            `Total Profit` int(20) NOT NULL)'''))

    data.csv_load_to_db(filename,destination_folder,db_connection)


run_extract = PythonOperator(
    task_id = 'extract',
    python_callable= extract,
    dag=dag
)

run_transform_load = PythonOperator(
    task_id = 'ransform and load',
    python_callable= transform_load,
    dag=dag
)

run_extract.set_downstream(run_transform_load)
