from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark=SparkSession\
        .builder\
        .appName('wordcountlog')\
        .master('local[1]')\
        .getOrCreate()

log_file=spark.readStream \
        .text('pyspark-streaming-from-filesystem_six/Hadoop/application_1445087491445_0005')

first_part=log_file.withColumn('timestamp', f.regexp_extract('value', '(\d{4}\-\d{2}-\d{2}\W\d{2}\:\d{2}\:\S.*?)\s', 1))\
                    .withColumn('status', f.split(log_file['value'], ' ', limit=6).getItem(2))\
                    .withColumn('process', f.regexp_extract('value','\[(.*?)\]\s\w.*',1))\
                    .withColumn('action', f.regexp_extract('value','\]\s(\w.*?)\:',1))\
                    .withColumn('description', f.regexp_extract('value','\]\s\w.*?\:\W(\w.*)',1))\
                    .drop('value').replace('', None)

clean_df=first_part.dropna('any', subset='timestamp')


stream_log = clean_df.writeStream\
                .format('csv')\
                .option('checkpointlocation', 'kafka-pyspark-to-cassandra/data_test_log2')\
                .option('path','kafka-pyspark-to-cassandra/data_test_log')\
                .start()

stream_log.awaitTermination()