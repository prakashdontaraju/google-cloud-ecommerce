import argparse
import logging
import pandas as pd

from google.cloud import spanner

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, FloatType


def get_product_information(row, product_attributes):
    """Cleans event details and product information"""

    category_code = row.category_code
    details = category_code.split('.')
    row = row.asDict()
    row['category_code'] = dict(zip(product_attributes, details))
    # back to unicode string
    row['category_code'] = str(row['category_code'])

    return Row(**row)


def transform_data(
    sqlContext, user_sessions_chunk_df, product_attributes, schema):
    """Transforms data before inserting into Cloud Spanner table."""

    user_sessions_df = user_sessions_chunk_df.astype(str)

    # inplace=True to fill the cell and not just keep a copy
    user_sessions_df['brand'] = user_sessions_df['brand'].fillna(
        value='Not Specified')
    
    # column-level transformations quicker with Spark Dataframes vs RDDs
    user_sessions_spDF = sqlContext.createDataFrame(
        user_sessions_df, schema=schema)

    # some element-wise or row-wise operations are best with RDDs
    user_sessions_rdd = user_sessions_spDF.rdd.map(list)
    # print(user_sessions_rdd.take(5))
    user_sessions_rdd = user_sessions_spDF.rdd.map(
        lambda row: get_product_information(row, product_attributes))

    user_sessions_spDF = sqlContext.createDataFrame(
        user_sessions_rdd, schema=schema)
    user_sessions_df = user_sessions_spDF.toPandas()
    
    return user_sessions_df


def get_instance(instance_id):
    """Gets Cloud Spanner Instance ID."""

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    return instance


def create_database(instance, database_id):
    """Creates a Cloud Spanner database and table."""

    database = instance.database(
        database_id,
        ddl_statements=[
            """CREATE TABLE events_batch ( 
                        record_id INT64 NOT NULL,
                        event_time STRING(50) NOT NULL, 
                        event_type STRING(50) NOT NULL, 
                        product_id STRING(50) NOT NULL, 
                        category_id STRING(50) NOT NULL, 
                        category_code STRING(200) NOT NULL, 
                        brand STRING(30) NOT NULL, 
                        price STRING(20) NOT NULL, 
                        user_id STRING(50) NOT NULL, 
                        user_session STRING(100) NOT NULL, 
                        ) PRIMARY KEY (record_id, event_time)""",
        ],
    )

    operation = database.create()

    logging.info("Waiting for operation to complete...")
    operation.result(120)

    logging.info(
        "Created database {} on instance {}".format(database, instance))


def write_to_spanner(instance, database_id, user_sessions_values):
    """Writes dataframe into Cloud Spanner table."""

    database = instance.database(database_id)
    
    with database.batch() as batch:
        batch.insert(
            table='events_batch',
            columns = ('record_id', 'event_time', 'event_type', 'product_id',
            'category_id', 'category_code', 'brand', 'price', 'user_id',
            'user_session'),
            values = user_sessions_values,
        )


def main():
    """Executes Batch pipeline to store dataset into Cloud Spanner table."""

    parser = argparse.ArgumentParser(
        description='Perform Batch processing to send session data to Spanner')

    parser.add_argument(
        '--input',
        help='''Path to data set in cloud storage
            Example: --input gs://project/path/to/GCS/file''',
        required=True)

    parser.add_argument(
        '--instance_id',
        help='''Cloud Spanner instance ID
            Example: --instance_id spanner_instance_id''',
        required=True)

    parser.add_argument(
        '--database_id',
        help='''Cloud Spanner database ID
            Example: --database-id spanner_database_id''',
        required=True)

    args = parser.parse_args()

    logging.info('Reading Dataset')
    user_sessions_chunks_df = pd.read_csv(
        args.input, encoding='utf-8', chunksize=int(10**2))

    conf = SparkConf().setAppName(
        "Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    instance = get_instance(args.instance_id)

    logging.info('Creating user_sessions Spanner Database')
    create_database(instance, args.database_id)

    product_attributes = ['category', 'sub_category', 'product',
    'product_details']

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

    for user_sessions_chunk_df in user_sessions_chunks_df:

        logging.info('Transforming data from the Batch')
        # print(user_sessions_chunk_df.count())
        user_sessions_df = transform_data(
            sqlContext, user_sessions_chunk_df, product_attributes, schema)

        logging.info(
            'Loading DF Data from the Batch into events_batch Spanner Table')

        user_sessions_rows = user_sessions_df.to_records(
            index=True, index_dtypes=int)
        user_sessions_values = list(user_sessions_rows)
        write_to_spanner(instance, args.database_id, user_sessions_values)

    spanner_success_message = ('Finished Loading DF Data from all'+
    ' Batches into events_batch Spanner Table')
    logging.info(spanner_success_message)
    

if __name__ == '__main__':
    main()