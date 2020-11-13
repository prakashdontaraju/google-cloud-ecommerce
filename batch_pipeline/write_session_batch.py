import logging
import argparse
# import uuid
# import numpy as np
import pandas as pd
from google.cloud import spanner
from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StructType, StructField, StringType, FloatType


product_attributes = ['category', 'sub_category', 'product','product_details']

schema = StructType([
                        StructField("event_time", StringType(), True), 
                        StructField("event_type", StringType(), True), 
                        StructField("product_id", StringType(), True), 
                        StructField("category_id", StringType(), True), 
                        StructField("category_code", StringType(), True), 
                        StructField("brand", StringType(), True), 
                        StructField("price", StringType(), True), 
                        StructField("user_id", StringType(), True), 
                        StructField("user_session", StringType(), True)
                        ])



def get_product_information(row, product_attributes):

    category_code = row.category_code

    details = category_code.split('.')

    row = row.asDict()

    row['category_code'] = dict(zip(product_attributes, details))
    # back to unicode string
    row['category_code'] = str(row['category_code'])

    return Row(**row)



def transform_data(sqlContext, user_sessions_chunk_df, product_attributes):

    user_sessions_df = user_sessions_chunk_df.astype(str)
    
    # Transform event_time from string to timestamp (datetime)
    # user_sessions_df['event_time'] = pd.to_datetime(user_sessions_df['event_time'],
    # format='%Y-%m-%d %H:%M:%S %Z')
    # Eliminate Time Zone from timestamp (datetime)
    # user_sessions_df['event_time'] = pd.to_datetime(user_sessions_df['event_time'],
    # format=TIME_FORMAT)

    # inplace=True to fill the cell and not just keep a copy
    user_sessions_df['brand'] = user_sessions_df['brand'].fillna(value='Not Specified')
    # user_sessions_df['price'] = pd.to_numeric(user_sessions_df['price'])
    
    # column-level transformations quicker with Spark Dataframes vs RDDs
    user_sessions_spDF = sqlContext.createDataFrame(user_sessions_df, schema=schema)
    # user_sessions_spDF.fillna(np.nan)
    # user_sessions_spDF = user_sessions_spDF.withColumn(
    #                         'event_time', user_sessions_spDF['event_time'].cast(TimestampType()))
    # user_sessions_spDF = user_sessions_spDF.withColumn(
    #                         'price', user_sessions_spDF['price'].cast(FloatType()))

    # some element-wise or row-wise operations are best with RDDs
    user_sessions_rdd = user_sessions_spDF.rdd.map(list)
    # print(user_sessions_rdd.take(5))
    user_sessions_rdd = user_sessions_spDF.rdd.map(
                        lambda row: get_product_information(row, product_attributes))

    user_sessions_spDF = sqlContext.createDataFrame(user_sessions_rdd, schema=schema)
    user_sessions_df = user_sessions_spDF.toPandas()
    

    return user_sessions_df


def get_instance(instance_id):

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    return instance



def create_database(instance, database_id):
    """Creates a database and tables for sample data."""

    database = instance.database(
        database_id,
        ddl_statements=[
            """CREATE TABLE batch_data ( 
                        record_id STRING(20) NOT NULL,
                        event_time STRING(50) NOT NULL, 
                        event_type STRING(50) NOT NULL, 
                        product_id STRING(50) NOT NULL, 
                        category_id STRING(50) NOT NULL, 
                        category_code STRING(200) NOT NULL, 
                        brand STRING(30) NOT NULL, 
                        price STRING(20) NOT NULL, 
                        user_id STRING(50) NOT NULL, 
                        user_session STRING(100) NOT NULL, 
                        ) PRIMARY KEY (record_id)""",
        ],
    )

    operation = database.create()

    logging.info("Waiting for operation to complete...")
    operation.result(120)

    logging.info("Created database {} on instance {}".format(database, instance))


#  shortuuid.ShortUUID().random(length=5), spanner.COMMIT_TIMESTAMP



def write_to_spanner(instance, database_id, user_sessions_values):

    database = instance.database(database_id)

    # def insert_session(transaction):
    #     insert_query = """INSERT batch_data (record_id, event_time, event_type, 
    #     product_id, category_id, category_code, brand, price, user_id, user_session) 
    #     VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})""".format(
    #         uuid.uuid4(), row['event_time'], 
    #         row['event_type'], row['product_id'], row['category_id'], 
    #         row['category_code'], row['brand'], row['price'], 
    #         row['user_id'], row['user_session'])
    #     session = transaction.execute_update(insert_query)

        # logging.info(session)

    # database.run_in_transaction(insert_session)
    
    with database.batch() as batch:
        batch.insert(
            table='batch_data',
            columns = ('record_id', 'event_time', 'event_type', 'product_id', 'category_id', 
                            'category_code', 'brand', 'price', 'user_id', 'user_session'),
            values = user_sessions_values,
        )



def main():

    parser = argparse.ArgumentParser(
        description='Perform Batch processing to send session data to Spanner')

    parser.add_argument(
        '--input',
        help='Path to local file. Example: --input C:/Path/To/File/File.csv',
        required=True)

    parser.add_argument(
        '--instance_id',
        help='Cloud Spanner instance ID Example: --instance_id spanner_instance',
        required=True)

    parser.add_argument(
        '--database_id',
        help='Cloud Spanner database ID Example: --database-id spanner_database',
        required=True)


    args = parser.parse_args()

    logging.info('Reading Dataset')
    user_sessions_chunks_df = pd.read_csv(args.input,
                                    encoding='utf-8', chunksize=int(10**2))

    conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    instance = get_instance(args.instance_id)


    logging.info('Creating user_sessions Spanner Database')
    create_database(instance, args.database_id)


    for user_sessions_chunk_df in user_sessions_chunks_df:

        logging.info('Transforming data from the Batch')
        # print(user_sessions_chunk_df.count())
        user_sessions_df = transform_data(sqlContext, user_sessions_chunk_df, product_attributes)

        logging.info('Loading DF Data from the Batch into batch_data Spanner Table')
        # for index, row in user_sessions_df.iterrows():
        #     write_to_spanner(instance, args.database_id, row)
        #     break
        user_sessions_rows = user_sessions_df.to_records()
        user_sessions_values = list(user_sessions_rows)
        write_to_spanner(instance, args.database_id, user_sessions_values)


    logging.info('Finished Loading DF Data from all Batches into batch_data Spanner Table')
    


if __name__ == '__main__':
    main()