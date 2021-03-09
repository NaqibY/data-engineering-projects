import os
import json
import pandas as pd
from pymongo import MongoClient
from google.cloud.storage import client

import src.to_gcs as load
import src.mongodb_transform as transform
from src.logging_task import logger
from src.from_mongodb import extract_mongodb

if __name__=='__main__':
    logger=logger(filename='project_three_logs.log', filemode='w')

    client=MongoClient('mongodb://localhost:27017/')                        # begin extraction
    dbs='test'
    coll_city='city_inspection'
    coll_zips='zips'
    inspection_list_dict=extract_mongodb(client, dbs, coll_city)
    zips_list_dict=extract_mongodb(client, dbs, coll_zips)

    bucket_name='data-engineering-data-sources'                             # load to staging, cloudstorage
    dest_blob_inspection='raw/project_three/city_inspection.json'
    dest_blob_zips='raw/project_three/zips.json'
    source_inspecton='/tmp/city_inspection.json'
    source_zips='/tmp/zips.json'
    with open(source_inspecton,'w') as json_inspection:
        json.dump(inspection_list_dict, json_inspection,indent=1)
    with open(source_zips,'w') as json_zips:
        json.dump(zips_list_dict, json_zips,indent=1)
    load.load_blob(bucket_name, dest_blob_inspection, source_inspecton)
    load.load_blob(bucket_name, dest_blob_zips, source_zips)
    logger.info('extraction is complete, file loaded to gcs at {} and {}'.format(dest_blob_inspection,dest_blob_zips))

    list_of_data=transform.transform_inspection(source_inspecton, source_zips)                          # transformation stage
    list_of_data[0].to_parquet('/tmp/city_inspection.pq',
                                compression='gzip')
    list_of_data[1].to_parquet('/tmp/address.pq', 
                                compression='gzip')
    list_of_data[-1].to_parquet('/tmp/zips.pq',
                                 compression='gzip')                                             
    logger.info('extract complete, check file in tmp')

    dest_blob_inspection_transform='transformed/project_three/city_inspection.pq'
    dest_blob_address_transform='transformed/project_three/address.pq'         # after transformation is completed load to staging 
    dest_blob_zips_transform='transformed/project_three/zips.pq'               # and to bigquery
    source_inspecton_transform='/tmp/city_inspection.pq'
    source_address_transform='/tmp/address.pq'
    source_zips_transform='/tmp/zips.pq'
    load.load_blob(bucket_name, 
                   dest_blob_inspection_transform,
                   source_inspecton_transform)
    load.load_blob(bucket_name,
                   dest_blob_address_transform,
                   source_address_transform)        
    load.load_blob(bucket_name,
                   dest_blob_zips_transform,
                   source_zips_transform)       

    url_inspection='gs://'+bucket_name+'/'+dest_blob_inspection_transform         #load parquet into coudstorage and bigquery
    url_address='gs://'+bucket_name+'/'+dest_blob_address_transform
    url_zips='gs://'+bucket_name+'/'+dest_blob_zips_transform
    load.pq_gcs_to_bigquery(url_inspection, 'project_three', 'city_inspection','WRITE_TRUNCATE')
    load.pq_gcs_to_bigquery(url_address, 'project_three', 'address','WRITE_TRUNCATE')
    load.pq_gcs_to_bigquery(url_zips, 'project_three', 'zips', 'WRITE_TRUNCATE')
    logger.info('task is succesfull , the data are ready at project_three dataset')


    os.system('cat logs.log')