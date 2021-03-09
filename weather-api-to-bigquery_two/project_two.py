import json
import logging
import os
import datetime
from src._7timerAPI_to_json import get_dataset
import src._7timer_transform as transform
import src.to_gcs as load
import src.logging as logging

from google.cloud import bigquery as bq

param={'lat': 3.139003,
    'lon': 101.686852,
    'unit': 'Metric',
    'output': 'json',
    'product': 'meteo'
        }

def main():
    logger = logging.logger('project_two_logs.log')
    date_now=datetime.datetime.now().date()
    client=bq.Client()
    bucket_name='data-engineering-data-sources'                                   
    dest_blob='raw/project_two/weather-{}.json'
    dest_blob_fact_transform='transformed/project_two/weather-{}.pq'
    dest_blob_dim_transform='transformed/project_two/{}.pq'
    source_weather='/tmp/weather-{}.json'
    # source_dim='/tmp/{}.pq'
    source_weather_transform='/tmp/weather-{}.pq'
    source_dim_transform='/tmp/{}.pq'
    url_fact='gs://'+bucket_name+'/'+dest_blob_fact_transform.format(date_now)
    url_dim='gs://'+bucket_name+'/'+dest_blob_dim_transform
    print(url_fact)
    json_data = get_dataset(param=param)                                # request api and save to local file
    with open(source_weather.format(date_now),'w') as weather_json:
        json.dump(json_data, weather_json)

    load.load_blob(bucket_name, dest_blob.format(date_now), source_weather.format(date_now))       #load fact table into cloud storage

    dict_fact = transform.json_data(json_data=json_data)            # transform fact table
    name_of_fact_table = str(*dict_fact.keys())
    df_of_fact_table = dict_fact[name_of_fact_table]
    df_dim = transform.html_data()                                  # transform html reference data into df
    
    df_of_fact_table.to_parquet(source_weather_transform.format(date_now), engine='pyarrow', compression='gzip') # prepare parquet and load into staging
    load.load_blob(bucket_name, dest_blob_fact_transform.format(date_now), source_weather_transform.format(date_now))
    for name, df in df_dim.items():
        df.to_parquet(source_dim_transform.format(name), engine='pyarrow', compression='gzip')
        load.load_blob(bucket_name, dest_blob_dim_transform.format(name), source_dim_transform.format(name))
    
    load.pq_gcs_to_bigquery(url_fact, 'project_two', name_of_fact_table, 'WRITE_APPEND')            #load into bigquery

    logger.info("{}'s of rows were succesfully loaded into project_two.FACT_weather".format(len(df_of_fact_table))) 
    # load dim only on first run since this reference to fact table so they are not updating every running tasks
    # call list of existing table in bigquery 
    check_table=client.list_tables('project_two')
    table_in_bq=[]
    for table in check_table:
        if table.table_id != 'FACT_weather':
            table_in_bq.append(table.table_id)
            
    def dim_loop(df_dim, table_in_bq):
        '''
        load table into bigquery dataset, get list of existing table in dataset. upload all DIM table if not exist and appending FACT table. 
        
        Args:

            1.df_dim = dataframe of transformed html_data()

            2.table_in_bq = list off table in biquery dataset
        '''
        table_not_exist=set(table_in_bq).symmetric_difference(set(df_dim.keys()))
        table_to_load={k:df_dim[k] for k in table_not_exist}    
        list_of_DIM_table=[]
        for tablename, df_dim in table_to_load.items():
            load.pq_gcs_to_bigquery(url_dim.format(tablename), 'project_two', tablename, 'WRITE_TRUNCATE')
            list_of_DIM_table.append(tablename)

        logger.info('this {} dimensional tables are loaded into project_two '.format(list_of_DIM_table))
    
    if len(table_in_bq) < 5:
        dim_loop(df_dim, table_in_bq)

    print('task complete')


if __name__=='__main__':
    main()    
