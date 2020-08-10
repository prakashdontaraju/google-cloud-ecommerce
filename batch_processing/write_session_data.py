import json
import logging
import datetime
import numpy as np
import pandas as pd
from functools import partial
from gcsfs.core import GCSFileSystem
from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import split as Split
from pyspark.sql.types import TimestampType, FloatType
from connect_to_cassandra import cassandra_connection, close_cassandra_connection



product_attributes = ['category', 'sub_category', 'product','product_details']



def download_from_gcs():


    return user_sessions_chunks_df



def get_product_information(row, product_attributes):

    category_code = row.category_code

    details = category_code.split('.')

    row = row.asDict()

    row['category_code'] = dict(zip(product_attributes, details))
    row['category_code'] = json.dumps(row['category_code'])

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

    logging.info('SECOND')

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
                        )
    )



def write_to_cassandra(session, cluster, user_sessions_rdd):


    query_insert_session_data = "INSERT INTO batch_data " \
                "(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) " \
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    logging.info('FIRST')
    prepared_sessions = session.prepare(query_insert_session_data)
    logging.info('RDD to PySpark Dataframe to Pandas Dataframe')
    user_sessions_df = user_sessions_rdd.toDF().toPandas()
    insert_records(session, user_sessions_df, prepared_sessions)



def view_table_data(session):

    query_view_data = "SELECT * batch_data LIMIT 100"
    session.execute(query_view_data)



def main():

    user_sessions_chunks_df = pd.read_csv('gs://ecommerce-283019/2019-Nov-Sample.csv',
                                    encoding='utf-8', chunksize=int(10**5))
    user_sessions_chunks_df = download_from_gcs()

    conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    logging.info('Connecting to Cassandra')
    cluster, session = cassandra_connection()

    for user_sessions_chunk_df in user_sessions_chunks_df:

        try:
            logging.info('Transforming data from the Batch')
            user_sessions_rdd = transform_data(sqlContext, user_sessions_chunk_df, product_attributes)
            # print(user_sessions_rdd.take(5))
            # example1 = user_sessions_rdd.first()
            # print(example1['category_code'])
            logging.info('Loading Data from the Batch into batch_data Table')
            write_to_cassandra(session, cluster, user_sessions_rdd)
        
        except Exception as e:
            print(e)

        
    # view_table_data(session)
    close_cassandra_connection(cluster, session)



if __name__ == '__main__':
    main()