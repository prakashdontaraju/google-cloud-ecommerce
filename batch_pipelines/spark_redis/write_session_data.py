import json
import redis
import logging
import argparse
import numpy as np
import pandas as pd
from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import FloatType, TimestampType



product_attributes = ['category', 'sub_category', 'product','product_details']



def get_product_information(row, product_attributes):

    category_code = row.category_code

    details = category_code.split('.')

    row = row.asDict()

    row['category_code'] = dict(zip(product_attributes, details))
    row['category_code'] = json.dumps(row['category_code'])

    row['event_details'] = str(row['event_time']) +' | '+ row['user_id']

    return Row(**row)



def transform_data(sqlContext, user_sessions_chunk_df, product_attributes):

    # column-level transformations quicker with Spark Dataframes vs RDDs
    user_sessions_spDF = sqlContext.createDataFrame(user_sessions_chunk_df.astype(str))
    user_sessions_spDF.fillna(np.nan)
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'event_time', user_sessions_spDF['event_time'].cast(TimestampType()))
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'price', user_sessions_spDF['price'].cast(FloatType()))

    # some element-wise or row-wise operations are best with RDDs
    user_sessions_rdd = user_sessions_spDF.rdd.map(list)
    # print(user_sessions_rdd.take(5))
    user_sessions_rdd = user_sessions_spDF.rdd.map(
                        lambda row: get_product_information(row, product_attributes))

    user_sessions_spDF = user_sessions_rdd.toDF()

    return user_sessions_spDF



def write_to_redis(redisConnection, user_sessions_spDF):

    user_sessions = user_sessions_spDF.toPandas()
    
    for index, row in user_sessions.iterrows():
        # print(row['category_code'])
        
        hash_name = row['event_details'] + ' | ' + str(index+1)        
        # key = json.dumps(row['event_details'])
        # value = json.dumps(
        #     {
        # 'event_time': str(row['event_time']), 'event_type': row['event_type'], 'product_id': row['product_id'],
        # 'category_id': row['category_id'], 'category_code': row['category_code'], 'brand': row['brand'],
        # 'price':  row['price'], 'user_id': row['user_id'], 'user_session':  row['user_session']
        #     }
        # )

        # columns = ['event_details', 'event_time', 'event_type', 'product_id', 'category_id', 'category_code','brand', 'price',
        #                 'user_id', 'user_session']
        
        # redisConnection.hset(hash_name, key, value)
        redisConnection.hset(hash_name, 'event_time', str(row['event_time']))
        redisConnection.hset(hash_name, 'event_type', row['event_type'])
        redisConnection.hset(hash_name, 'product_id', row['product_id'])
        redisConnection.hset(hash_name, 'category_id', row['category_id'])
        redisConnection.hset(hash_name, 'category_code', row['category_code'])
        redisConnection.hset(hash_name, 'brand', row['brand'])
        redisConnection.hset(hash_name, 'price', row['price'])
        redisConnection.hset(hash_name, 'user_id', row['user_id'])
        redisConnection.hset(hash_name, 'user_session', row['user_session'])

        # redisConnection.hset(hash_name, 'event_time', str(row['event_time']), 'event_type', row['event_type'],
        #                                     'product_id', row['product_id'], 'category_id', row['category_id'],
        #                                     'category_code', row['category_code'], 'brand', row['brand'],
        #                                     'price', row['price'], 'user_id', row['user_id'],
        #                                     'user_session', row['user_session'])



def clear_redis_database(redisConnection):

    redisConnection.flushdb()



def main():

    parser = argparse.ArgumentParser(
        description='Perform Batch processing to send session data to Redis')

    parser.add_argument(
        '--input',
        help='Path to local file. Example: --input C:/Path/To/File/File.csv',
        required=True)

    parser.add_argument(
        '--port',
        help='Port to listen to Redis. Example: --port 6379',
        type=int,
        required=True)
    

    args = parser.parse_args()

    logging.info('Reading Dataset')
    user_sessions_chunks_df = pd.read_csv(args.input,
                                    encoding='utf-8', chunksize=int(10**5))

    conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    logging.info('Initializing Redis Connection')
    redisConnection = redis.Redis(host='127.0.0.1', port=args.port, db=0)

    clear_redis_database(redisConnection)

    for user_sessions_chunk_df in user_sessions_chunks_df:

        logging.info('Transforming data from the Batch')
        # print(user_sessions_chunk_df.count())
        user_sessions_spDF = transform_data(sqlContext, user_sessions_chunk_df, product_attributes)
        # print(user_sessions_spDF.show(n=5))
        # print(column_names)

        logging.info('Loading DF Data from the Batch into batch_data Table')
        write_to_redis(redisConnection, user_sessions_spDF)


    logging.info('Finished Loading DF Data from all Batches into batch_data Table')
    
    # Clear Redis Database
    # clear_redis_database(redisConnection)



if __name__ == '__main__':
    main()