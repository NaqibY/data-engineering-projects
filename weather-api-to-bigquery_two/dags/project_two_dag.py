from airflow import DAG
from airflow.models import xcom
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryHook

import json
from google.api_core.exceptions import GoogleAPIError
import requests
import pandas as pd
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
dag = DAG(dag_id='project_two_copy',
          description='weather_api_to_bigquery',
          schedule_interval= timedelta(weeks=1), 
          default_args=default_args, 
          start_date=datetime(2021, 3, 4, 20-8, 30), 
          tags=['test'])



func_param={'bucket_name':'data-engineering-data-sources',
            'dest_blob':'raw/project_two_airflow/weather-{}.json'.format(datetime.now().date()),
            'dest_blob_fact_transform':'transformed/project_two_airflow/weather-{}.pq'.format(datetime.now().date()),
            'dest_blob_dim_transform':'transformed/project_two_airflow/',
            'source_weather':'/tmp/weather-{}.json'.format(datetime.now().date()),
            'source_weather_transform':'/tmp/weather-{}.pq'.format(datetime.now().date()),
            'source_dim_transform':'/tmp/{}.pq'}

def get_weather_json(source_weather):
    '''
    request kuala lumpur weather json from 7timer api, and save it local to perform transformation, edit the param inside this function if want to use other coordinate

    Args:
        1. source_weather= path yo save json file ``/tmp/file.json``
    
    '''
    param={'lat': 3.139003,
    'lon': 101.686852,
    'unit': 'Metric',
    'output': 'json',
    'product': 'meteo'
        }

    response=requests.get("http://www.7timer.info/bin/api.pl?",params=param)
    json_data=response.json()

    if response.status_code == 200:
        with open(source_weather,'w') as json_file:
            json.dump(json_data, json_file,indent=2)

    
    else: print('something wrong with the api,{}'.format(response.status_code))
    
def table_existence(task_instance):
    '''
    create dataset if not exist, if it exist but empty it means dags is first time runnning and branchoperator will return to its designated task. 
    '''
    bq_hook=BigQueryHook(gcp_conn_id='google_cloud_default')
    bq_hook.create_empty_dataset(dataset_id='project_two_airflow', exists_ok=True)
    list_of_exist_table=bq_hook.get_dataset_tables_list('project_two_airflow')

    if len(list_of_exist_table) == 0:
        return 'transform_raw_html'
    else:
        return 'transform_raw_json'

# requested json data
def transform_json_data(source_weather, task_instance):
    """
    json data setup for data aggregate

    Arg:

        1. source_weather= path where your json data is ``/tmp/weather.json``

    Return:
        dict {FACT_weather : ``/tmp/weather.pq``}
    """
    # if json_data is any:
    # data=task_instance.xcom_pull(task_ids='extract_json_weather')
    json_file=open(source_weather,'r')
    data=json.load(json_file)
    
    df_raw=pd.json_normalize(data)
    tim=pd.to_datetime(str(df_raw['init'][0]), format='%Y%m%d%H')

    #dataframe from downloaded json

    # dataseries table
    df_dataseries=pd.json_normalize(data,'dataseries')
    df_dataseries['time_interval']=list(map(lambda x : tim+pd.Timedelta(hours=x),df_dataseries['timepoint']))
    df_dataseries=df_dataseries.drop(columns=['rh_profile','wind_profile','timepoint'])
    df_dataseries=df_dataseries.set_index('time_interval')
    df_dataseries['wind10m.direction']=df_dataseries['wind10m.direction'].astype(int)


    # relative humidity table
    df_rh=pd.json_normalize(data['dataseries'],'rh_profile', meta='timepoint')
    df_rh['time_interval']=list(map(lambda x : tim+pd.Timedelta(hours=x),df_rh['timepoint']))
    df_rh=df_rh.drop(columns='timepoint')
    df_rh=df_rh.set_index('time_interval')

    df_rh.columns=['rh.layer', 'rh']
    df_rh=df_rh.replace('mb','', regex=True)
    df_rh['rh.layer']=df_rh['rh.layer'].astype(int)

    # wind table
    df_wind=pd.json_normalize(data['dataseries'],'wind_profile', meta='timepoint')
    df_wind['time_interval']=list(map(lambda x : tim+pd.Timedelta(hours=x),df_wind['timepoint']))
    df_wind=df_wind.drop(columns='timepoint')
    df_wind=df_wind.set_index('time_interval')

    df_wind.columns=['wind.layer', 'wind.direction', 'wind.speed']
    df_wind=df_wind.replace('mb','', regex=True)
    df_wind['wind.layer']=df_wind['wind.layer'].astype(int)

    df_fact1=df_dataseries.merge(df_rh, left_index=True, right_on='time_interval')
    df_fact=df_fact1.merge(df_wind, left_index=True, right_on='time_interval')
    cols=['cloudcover', 'highcloud', 'midcloud', 'lowcloud', 'temp2m',
        'lifted_index', 'rh2m', 'rh.layer', 'rh', 'msl_pressure', 'prec_type', 'prec_amount',
        'wind10m.direction', 'wind10m.speed',
        'wind.layer', 'wind.direction', 'wind.speed',
        'snow_depth']
    cols_clean=['cloudcover', 'highcloud', 'midcloud', 'lowcloud', 'temp2m',
        'lifted_index', 'rh2m', 'rh_layer', 'rh', 'msl_pressure', 'prec_type', 'prec_amount',
        'wind10m_direction', 'wind10m_speed',
        'wind_layer', 'wind_direction', 'wind_speed',
        'snow_depth']
    df_fact=df_fact.reindex(columns=cols)
    df_fact.columns=cols_clean
    fact_list={'FACT_weather':df_fact}

    # save parquet to local storage
    pq_path='/tmp/FACT_weather-{}.pq'.format(datetime.now().date())
    fact_list['FACT_weather'].to_parquet(pq_path)
    dict_of_local_pq=task_instance.xcom_pull(task_ids='transform_raw_html')

    if dict_of_local_pq==None:
        dict_of_local_pq={'FACT_weather':pq_path}

    else:
        dict_of_local_pq['FACT_weather']=pq_path

    return dict_of_local_pq

#dataframe of references from website
def transform_html_data():
    """
    html data setup for table aggregate

    :source_transform = ``/tmp/DIM_data.pq``
    Return:
        dict of dataframes,

    dict_of_local_pq={'DIM_cloud':source_transform, 'DIM_lifted_index':source_transform,
                  'DIM_prec_amount':source_transform, 'DIM_rh':source_transform,
                  'DIM_snow_depth':source_transform }

    """
    url='http://7timer.info/doc.php?lang=en'
    # html_data=pd.read_html('7Timer! - numerical weather forecast for anywhere over the world.html')[-4]
    html_data=pd.read_html(url)[-4]
    html_data.columns=html_data.iloc[0]
    html_data=html_data.drop(0,axis=0)
    html_data=html_data.reset_index(drop=True)

    # dimesional table
    # cloud
    di_cloud=html_data.iloc[0:9].copy()
    di_cloud[['min','max']]=di_cloud['Meaning'].str.split('-',expand=True)
    di_cloud=di_cloud.drop(columns=['Variable','Meaning'])
    # di_cloud[['Cloud Cover', 'High Cloud', 'Mid Cloud','Low Cloud']]=di_cloud['Value']
    di_cloud.columns=['cloud','min','max']
    di_cloud['min']=di_cloud['min'].str.strip('%')
    di_cloud['max']=di_cloud['max'].str.strip('%')
    di_cloud[['cloud','min','max']]=di_cloud[['cloud','min','max']].astype(int)

    # lifted index
    di_index=html_data[9:17].copy()
    di_index[['min','1','max']]=di_index['Meaning'].str.split(expand=True)
    di_index=di_index.drop(columns=['Variable','Meaning','1'])
    di_index=di_index.fillna(11)
    di_index.iloc[0,1:3]=[0,-7]
    di_index.iloc[-1,1]=11

    di_index.columns=['lifted_index','min','max']
    di_index[['lifted_index','min','max']]=di_index[['lifted_index','min','max']].astype(int)
    di_index=di_index.reset_index(drop=True)
    # di_index.set_index('lifted_index')

    # Relative Humidity
    di_rh=html_data[34:55].copy()
    di_rh[['min','max']]=di_rh['Meaning'].str.split('-',expand=True)
    di_rh=di_rh.drop(columns=['Variable','Meaning'])
    di_rh.columns=['rh','min','max']
    di_rh['min']=di_rh['min'].str.strip('%')
    di_rh['max']=di_rh['max'].str.strip('%')

    di_rh.iloc[-1,1]='99'
    di_rh=di_rh.fillna(100)

    di_rh[['rh','min','max']]=di_rh[['rh','min','max']].astype(int)
    di_rh=di_rh.reset_index(drop=True)

    # wind_speed
    di_wind=html_data[55:68].copy()
    di_wind['description']=di_wind['Meaning'].str.rsplit(expand=True,n=1).iloc[:,-1]
    di_wind[['Speed','description']]=di_wind['Meaning'].str.rsplit(expand=True,n=1)
    di_wind[['min','max']]=di_wind['Speed'].str.rsplit('-',expand=True)
    di_wind=di_wind.drop(columns=['Variable','Meaning','Speed'])

    di_wind['max']=di_wind['max'].str.strip('m/s')

    di_wind=di_wind.fillna(0.3)
    di_wind.iloc[0,2]=0
    di_wind.iloc[-1,2:4]=[55.9,55.9]

    di_wind.columns=['wind_speed','description','min','max']
    di_wind[['min','max']]=di_wind[['min','max']].astype(float)
    di_wind['wind_speed']=di_wind['wind_speed'].astype(int)
    di_wind['description']=di_wind['description'].str.strip('(+)')
    di_wind=di_wind.reset_index(drop=True)

    # Percipitation Amount
    di_prec=html_data[71:81].copy()
    di_prec[['min','max']]=di_prec['Meaning'].str.split('-',expand=True,)
    di_prec=di_prec.drop(columns=['Variable','Meaning'])
    di_prec['max']=di_prec['max'].str.strip('mm/hr')

    di_prec=di_prec.fillna(0)
    di_prec.iloc[0,1] = 0
    di_prec.iloc[-1,1:3]=[75,75]


    di_prec.columns=['prec_amount','min','max']
    di_prec[['min','max']]=di_prec[['min','max']].astype(float)
    di_prec['prec_amount']=di_prec['prec_amount'].astype(int)
    di_prec=di_prec.reset_index(drop=True)

    # snow_depth
    di_snow=html_data[81:91].copy()
    di_snow[['min','max']]=di_snow['Meaning'].str.split('-',expand=True,)
    di_snow=di_snow.drop(columns=['Variable','Meaning'])
    di_snow['max']=di_snow['max'].str.strip('cm')

    di_snow=di_snow.fillna(0)
    di_snow.iloc[0,1] = 0
    di_snow.iloc[-1,1:3]=[250,250]

    di_snow.columns=['snow_depth','min','max']
    di_snow[['min','max']]=di_snow[['min','max']].astype(float)
    di_snow['snow_depth']=di_snow['snow_depth'].astype(int)
    di_snow=di_snow.reset_index(drop=True)

    df_dimension={'DIM_cloud':di_cloud, 'DIM_lifted_index':di_index, 'DIM_rh':di_rh, 'DIM_wind_speed':di_wind, 'DIM_prec_amount':di_prec, 'DIM_snow_depth':di_snow }
    
    dict_of_local_pq={}         # create dict of local pq {name:path} so that can load multiple using list on gcsoperator
    # save parquet to local storage
    for name, df in df_dimension.items():
        pq_path='/tmp/'+name+'.pq'
        df.to_parquet(pq_path)
        dict_of_local_pq[name]=pq_path
    #return as list of dataframe
    return dict_of_local_pq

def local_pq_to_gcs(task_instance, bucket_name, dest_blob_dim_transform):
    """
    get local transfomed pq file and load into gcs

    Arg:

        1. bucket_name = 'your-gcs-bucket'

        2. dest_blob_dim_transform = 'transformed/project_two_airflow/'

    Return:
        dict_of_local_pq={'DIM_cloud':dest_blob_dim_transform, 'DIM_lifted_index':dest_blob_dim_transform,
                  'DIM_prec_amount':dest_blob_dim_transform, 'DIM_rh':dest_blob_dim_transform,
                  'DIM_snow_depth':dest_blob_dim_transform , FACT_weather : dest_blob_dim_transform}
    """
    dict_of_local_pq=task_instance.xcom_pull(task_ids='transform_raw_json')
    dict_of_gcs_pq={}
    for name, pq_path in dict_of_local_pq.items():
        blob_name=pq_path.split('/')[-1]
        dest_blob_transform=dest_blob_dim_transform+blob_name
        GCSHook(gcp_conn_id='google_cloud_default').upload(bucket_name=bucket_name,
                                                            object_name=dest_blob_transform,
                                                            filename=pq_path)
        dict_of_gcs_pq[name]=dest_blob_transform
    
    return dict_of_gcs_pq

def load_gcs_pq_to_bq(task_instance, bucket_name):
    """
    create external table from uri gcs 

    Arg:

        1. bucket_name = 'your-gcs-bucket'
    """                   
    dict_of_gcs_pq=task_instance.xcom_pull(task_ids='local_pq_to_gcs')
    client=BigQueryHook(gcp_conn_id='google_cloud_default')
    for table_name , source in dict_of_gcs_pq.items():
        source_uri='gs://{}/{}'.format(bucket_name, source)

        if table_name=='FACT_weather':
            client.run_load(source_uris=[source_uri],
                            destination_project_dataset_table='project_two_airflow.{}'.format(table_name),
                            source_format='PARQUET',
                            write_disposition='WRITE_APPEND',
                            create_disposition='CREATE_IF_NEEDED',
                            autodetect=True)
        else:
            try:
                client.run_load(source_uris=[source_uri],
                                destination_project_dataset_table='project_two_airflow.{}'.format(table_name),
                                source_format='PARQUET',
                                write_disposition='WRITE_EMPTY',
                                create_disposition='CREATE_IF_NEEDED',
                                autodetect=True)
            except GoogleAPIError:
                print('{} table already exist, skip loading table'.format(table_name))

get_weather_json= PythonOperator(task_id='get_weather_json',
                                python_callable=get_weather_json,
                                op_kwargs=func_param,
                                trigger_rule='all_done',
                                dag=dag)

load_to_staging= LocalFilesystemToGCSOperator(task_id='load_to_staging',
                                            src=func_param['source_weather'],
                                            dst=func_param['dest_blob'],
                                            bucket=func_param['bucket_name'],
                                            gcp_conn_id='google_cloud_default',
                                            trigger_rule='all_done',
                                            dag=dag)

check_dataset=BranchPythonOperator(task_id='check_dataset',
                                            python_callable=table_existence,
                                            trigger_rule='all_done',
                                            dag=dag)

transform_raw_json= PythonOperator(task_id='transform_raw_json',
                                    python_callable=transform_json_data,
                                    op_kwargs=func_param,
                                    trigger_rule='none_failed_or_skipped',
                                    dag=dag)



transform_raw_html= PythonOperator(task_id='transform_raw_html',
                                    python_callable=transform_html_data,
                                    trigger_rule='all_done',
                                    dag=dag)

local_pq_to_gcs= PythonOperator(task_id='local_pq_to_gcs',
                                python_callable=local_pq_to_gcs,
                                op_kwargs=func_param,
                                trigger_rule='all_done',
                                dag=dag)

load_gcs_pq_to_bq= PythonOperator(task_id='load_gcs_pq_to_bq',
                                python_callable=load_gcs_pq_to_bq,
                                op_kwargs=func_param,
                                trigger_rule='all_done',
                                dag=dag)

check_area_date= DummyOperator(task_id='weather_at_Kuala_Lumpur_on_{}'.format(datetime.now().date()))
create_dataset= DummyOperator(task_id='create_dataset')
updating_dataset= DummyOperator(task_id='updating_dataset')


check_area_date>>get_weather_json>>load_to_staging>>check_dataset
check_dataset>>updating_dataset>>transform_raw_json>>local_pq_to_gcs>>load_gcs_pq_to_bq
check_dataset>>create_dataset>>transform_raw_html>>transform_raw_json>>local_pq_to_gcs>>load_gcs_pq_to_bq
