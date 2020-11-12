import json
import logging
import argparse
#  import shortuuid
import numpy as np
import pandas as pd
from google.cloud import spanner
from pyspark.sql import Row, SQLContext
from google.cloud.spanner_v1 import param_types
from pyspark import SparkConf, SparkContext



product_attributes = ['category', 'sub_category', 'product','product_details']



def get_product_information(row, product_attributes):

    category_code = row.category_code

    details = category_code.split('.')

    row = row.asDict()

    row['category_code'] = dict(zip(product_attributes, details))
    # row['category_code'] = json.dumps(row['category_code'])

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


def create_database(instance_id, database_id):
    """Creates a database and tables for sample data."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(
        database_id,
        ddl_statements=[
            """CREATE TABLE batch_data ( 
                        record_id INT64 NOT NULL, 
                        event_time TIMESTAMP NOT NULL, 
                        event_type STRING(20) NOT NULL, 
                        product_id STRING(30) NOT NULL, 
                        category_id STRING(30) NOT NULL, 
                        category_code STRING(200) NOT NULL, 
                        brand STRING(30) NOT NULL, 
                        price FLOAT64 NOT NULL, 
                        user_id STRING(200) NOT NULL, 
                        user_session STRING(200) NOT NULL, 
                        ) PRIMARY KEY (record_id), 
                    """
        ],
    )

    operation = database.create()

    print("Waiting for operation to complete...")
    operation.result(120)

    print("Created database {} on instance {}".format(database_id, instance_id))


#  shortuuid.ShortUUID().random(length=5), spanner.COMMIT_TIMESTAMP



def write_to_spanner(database, transaction, row):

    def insert_session(transaction):
        session = transaction.execute_update(
            "INSERT batch_data"
                "(record_id, event_time, event_type, product_id, category_id"
                "category_code, brand, price, user_id, user_session) "
                "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}".format(
                    spanner.COMMIT_TIMESTAMP,row['event_time'], row['event_type'],
                    row['product_id'], row['category_id'], row['category_code'], row['brand'],
                    row['price'], row['user_id'], row['user_session'])
        )

    database.run_in_transaction(insert_session)



def main():

    parser = argparse.ArgumentParser(
        description='Perform Batch processing to send session data to Spanner')

    parser.add_argument(
        '--input',
        help='Path to local file. Example: --input C:/Path/To/File/File.csv',
        required=True)

    parser.add_argument(
        '--port',
        help='Port to listen to Spanner. Example: --port 3306',
        type=int,
        required=True)
    

    mysqlConnection = mysql_connection()
    mysqlCursor = mysqlConnection.cursor()

    table_names = ['batch_data']
    for table_name in table_names:
        create_table(mysqlCursor, table_name)
        # drop_table(mysqlCursor, table_name)


    args = parser.parse_args()

    logging.info('Reading Dataset')
    user_sessions_chunks_df = pd.read_csv(args.input,
                                    encoding='utf-8', chunksize=int(10**5))

    conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    create_database(instance_id, database_id)


    for user_sessions_chunk_df in user_sessions_chunks_df:

        logging.info('Transforming data from the Batch')
        # print(user_sessions_chunk_df.count())
        user_sessions_spDF = transform_data(sqlContext, user_sessions_chunk_df, product_attributes)
        # print(user_sessions_spDF.show(n=5))
        # print(column_names)
        user_sessions_df = user_sessions_spDF.toPandas()

        logging.info('Loading DF Data from the Batch into batch_data Spanner Table')
        for index, row in user_sessions_df.iterrows():
            write_to_spanner(database, transaction, row)


    logging.info('Finished Loading DF Data from all Batches into batch_data Spanner Table')
    


if __name__ == '__main__':
    main()