import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import mysql.connector
from datetime import datetime
import google.cloud.bigquery as bq 
from mysql.connector import errorcode
from google.api_core.exceptions import GoogleAPIError

import src.to_gcs as load
from src.logging import logger


logger=logger(filename='project_four_logs.log')

func_param = {
    'bucket_name':'data-engineering-data-sources',
    'source_transform':'/tmp/sales-{}.pq'.format(datetime.now().date()),
    'dest_blob_transform':'transformed/project_four/sales-{}.pq'.format(datetime.now().date())}

def mysql_to_pq(conn, source_transform=func_param['source_transform'], name_of_dataset='project_four', by_row_batch=5):
    '''
     
    extract mysql database and save into local pq ``tmp/sales-date.pq``. this function take the last rows of bq dataset and compared againts current
    mysql database to avoid duplication, only extract load new data from mysql to bq. if dataset not exist it will create dataset using name given
    
    Args:
        1. source_transform = 'path/local/file.pq'

        2. by_row_batch = number of row you want to extract ``int``

    return: 
        ``str`` of local pq file path
    '''
    client=bq.Client()
    row_id=client.query('select id from project_four.sales order by id desc limit 1')
    try:
        for i in row_id:
            last_row_id=i[0]
            print('last row in dataset is, '+i[0])
    except GoogleAPIError:
        row_id.error_result['reason']=='notFound'
        last_row_id=0
        print('no dataset.table')
        client.create_dataset(name_of_dataset, exists_ok=True)
        print('new dataset, {} created'.format(name_of_dataset))
    
    cur=conn.cursor()            #mysql conn
    cur.execute('use sales_records')
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
    
    table=pa.Table.from_pandas(df)
    # df.to_csv('test.csv')
    pq.write_table(table, source_transform)  
    # the pd.to_parquet does not working for some reason, segmentation fault
    # as for mean time use pyarrow lib to to create parquet file  

    # df.to_parquet(source_transform, engine='fastparquet')
    logger.info('id {} to {} being extracted'.format(last_row_id+1, last_row_id+by_row_batch))
    print('data extracted from id {} to {}, {} file ready to upload to cloudstorage'.format(last_row_id+1,
                                                                                            last_row_id+by_row_batch,
                                                                                            source_transform)) #,source_transform)

def check_database(conn):
    '''
    find total number of rows in 'sales_records.sales' and return ``int``

    Return:
        total_rows of mysql database
    '''
    # conn=MySqlHook(mysql_conn_id='mysql_localhost').get_conn()
    cur=conn.cursor()           #mysql conn
    try:
        cur.execute('use sales_records')
        cur.execute('select count(*) from sales')
        total_rows=cur.fetchone()[0]
        # task_instance.xcom_push(key='mysql_total_rows', value=total_rows)
        if type(total_rows) is int:
            print('appending new data')
            return total_rows
        # elif total_rows==50000:
        #     print('up to date')
        #     return 'check_dataset'
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_NO_DB_ERROR:
            print('no database found')
            logger.error(err)
            total_rows=None
        else:
            print('something wrong with database')
            logger.error(err)
            total_rows=None
    return total_rows

def check_dataset(conn):
    '''
    if dataset is not exist or need updating, it will execute extraction function and return ``str`` 'ready' if not, return ``str`` 'up to date'
    '''
    client=bq.Client()
    rows=client.query('select count(*) from project_four.sales')
    conn=conn
    mysql_total_rows=check_database(conn)
    try:
        if mysql_total_rows is None:
            print('something wrong with database')
            pass
        else:
            for item in rows:
                dataset_total_rows=item[0]
                if dataset_total_rows== mysql_total_rows:
                    print('bigquery_is_up_to_date')
                    logger.info('bigquery is up to date, extraction process is skipped')
                    return 'up to date'
                elif dataset_total_rows < mysql_total_rows:
                    print('bq dataset is not up to date to mysql')
                    logger.info('bq dataset is not up to date to mysql, begin extraction')
                    mysql_to_pq(conn)
                    return 'ready'
    except GoogleAPIError: 
        rows.error_result['reason']=='notFound'
        print(rows.error_result)
        logger.info('{}, new dataset created and begin extraction'.format(rows.error_result))
        mysql_to_pq(conn)
        return 'ready'
        
if __name__=='__main__':

    with open('config.json', 'r') as json_file:
        config=json.load(json_file)
    
    conn=mysql.connector.connect(user=config['mysql']['USER'],                   # establish target database
                            host=config['mysql']['HOST'],
                            database=config['mysql']['DATABASE'],
                            password=config['mysql']['PASSWORD'])
    job=check_dataset(conn)
    if job is 'ready':
        load.load_blob(bucket_name=func_param['bucket_name'],
                        destination_blob=func_param['dest_blob_transform'],
                        source_file_path=func_param['source_transform'])
        logger.info('parquet file succesfully load into cloudstorage')
        
        load.pq_gcs_to_bigquery(uri='gs://'+func_param['bucket_name']+'/'+func_param['dest_blob_transform'],
                                dataset='project_four',
                                table_id='sales',
                                write_disposition='WRITE_APPEND')
        logger.info('dataset updated')
    else:
        logger.info('dataset is up to date')

        pass

