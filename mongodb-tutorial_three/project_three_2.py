import os
import pandas as pd
import json
import datetime
from bson import ObjectId
from pymongo import MongoClient
from google.cloud import storage

from src.logging_task import logger
from src.from_mongodb import extract_mongodb
import src.to_gcs as load
import src.mongodb_transform as transform


if __name__=='__main__':
    logger=logger(filename='project_three_logs.log',filemode='a')

    date_now=datetime.datetime.now().date()
    bucket_name='data-engineering-data-sources'                                   
    dest_blob='raw/project_three/tripdata-{}.json'
    dest_blob_transform='transformed/project_three/tripdata-{}.pq'
    source='/tmp/tripdata-{}.json'
    source_transform='/tmp/tripdata-{}.pq'
    url='gs://'+bucket_name+'/'+dest_blob_transform.format(date_now)

    def get_initial_id():
        """
        get object_id from last extracted tripdata-yesterday.json in cloudstorage
        
        Return:
            ObjectId()
        TODO: this function only return bson.Objectid(), change to any keys according to specific mongodb schema
        """
        client=storage.Client()
        bucket=client.bucket(bucket_name)
        list_of_blob=bucket.list_blobs()

        for blob in list_of_blob:
            name=blob.name
            if name==dest_blob.format(date_now-datetime.timedelta(days=1)):
                print(blob.name)
                file_exist=blob.name
                downloaded_blob='/tmp/tripdata={}.json'.format(date_now-datetime.timedelta(days=1))
                load.download_blob(bucket_name, file_exist,downloaded_blob)
                with open(downloaded_blob, 'r') as previous_tripdata:
                    json_yesterday=json.loads(previous_tripdata)

                index_id=json_yesterday[-1]['_id']
                object_id=ObjectId(index_id)
            else:
                object_id=None
        
        initial_id=object_id

        return initial_id

    client=MongoClient('mongodb://localhost:27017/')                                #begin extraction
    dbs='test'
    coll='tripdata'
    list_of_docs=extract_mongodb(client, dbs, coll, initial_id=get_initial_id(), extract_by_batch=10000)
    
    with open(source.format(date_now),'w') as json_tripdata:
        json.dump(list_of_docs, json_tripdata,indent=1)
    load.load_blob(bucket_name, dest_blob.format(date_now), source.format(date_now))
    logger.info('extraction is complete, file loaded to gcs at {}'.format(dest_blob_transform.format(date_now)))

    df_tripdata=transform.transform_tripdata(source.format(date_now))               #transformation stages , save file to parquet
    df_tripdata.to_parquet(source_transform.format(date_now))

    load.load_blob(bucket_name,                                         #load parquet into coudstorage and bigquery
                   dest_blob_transform.format(date_now),
                   source_transform.format(date_now))
    load.pq_gcs_to_bigquery(url, 'project_three','tripdata', 'WRITE_APPEND')
    logger.info('task is succesfull , the data is ready at project_three.tripdata')

    os.system('cat logs.log')
