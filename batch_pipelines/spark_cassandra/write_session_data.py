import json
import random
import logging
import argparse
import numpy as np
import pandas as pd
from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import FloatType, TimestampType
from connect_to_cassandra import cassandra_connection , close_cassandra_connection



product_attributes = ['category', 'sub_category', 'product','product_details']



def get_product_information(row, product_attributes):

    category_code = row.category_code

    details = category_code.split('.')

    row = row.asDict()

    row['category_code'] = dict(zip(product_attributes, details))
    row['category_code'] = json.dumps(row['category_code'])

    row['event_details'] = row['user_id']+'|'+ str(row['event_time'])

    row['record_id'] = random.random()

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

    return user_sessions_rdd



def insert_records(session, user_sessions_df, prepared_sessions):


    for index, row in user_sessions_df.iterrows():
        session.execute(prepared_sessions
                    , (row['event_time']
                        , row['event_type']
                        , row['product_id']
                        , row['category_id']
                        , row['category_code']
                        , row['brand']
                        , row['price']
                        , row['user_id']
                        , row['user_session']
                        , row['event_details']
                        , row['record_id']
                        )
    )



def write_to_cassandra(session, cluster, user_sessions_rdd):


    query_insert_session_data = "INSERT INTO batch_data " \
                "(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session, event_details, record_id) " \
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" # use random number rather than uuid (it's long - storage capacity)
    prepared_sessions = session.prepare(query_insert_session_data)
    # RDD to PySpark Dataframe to Pandas Dataframe
    user_sessions_df = user_sessions_rdd.toDF().toPandas()
    insert_records(session, user_sessions_df, prepared_sessions)



def main():

    parser = argparse.ArgumentParser(
        description='Perform Batch processing to send session data to Cassandra')

    parser.add_argument(
        '--input',
        help='Path to local file. Example: --input C:/Path/To/File/File.csv',
        required=True)

    parser.add_argument(
        '--port',
        help='Port to listen to Cassandra. Example: --port=9042',
        type=int,
        required=True)

    args = parser.parse_args()

    user_sessions_chunks_df = pd.read_csv(args.input,
                                    encoding='utf-8', chunksize=int(10**5))

    conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    logging.info('Connecting to Cassandra')
    cluster, session = cassandra_connection(args.port)

    for user_sessions_chunk_df in user_sessions_chunks_df:

        logging.info('Transforming data from the Batch')
        # print(user_sessions_chunk_df.count())
        user_sessions_rdd = transform_data(sqlContext, user_sessions_chunk_df, product_attributes)
        # print(user_sessions_rdd.take(5))
        # example1 = user_sessions_rdd.first()
        # print(example1['category_code'])
        logging.info('Loading Data from the Batch into batch_data Table')
        write_to_cassandra(session, cluster, user_sessions_rdd)

        
    close_cassandra_connection(cluster, session)



if __name__ == '__main__':
    main()