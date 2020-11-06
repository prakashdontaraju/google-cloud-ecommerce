import json
import logging
import argparse
import numpy as np
import pandas as pd
import sqlalchemy as db
from pyspark.sql import Row, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col, hour, lit, split as Split
from pyspark.sql.types import FloatType, StringType, TimestampType
# from sqlalchemy import MetaData, Table, Column, Float, Integer, String, DateTime



product_attributes = ['category', 'sub_category', 'product','product_details']



def get_product_information(row, product_attributes):


    # fill_category_details = [np.nan, np.nan, np.nan, np.nan]
    
    row = row.asDict()

    category_details = row['category_code'].split('.')

    
    row['category_code'] = dict(zip(product_attributes, category_details))

    row['category'] = row['category_code'].get('category', np.nan)

    row['sub_category'] = row['category_code'].get('sub_category', np.nan)

    # row['product'] = row['category_code'].get('product', np.nan)

    # row['product_details'] = row['category_code'].get('product_details', np.nan)
    
    row['category_code'] = json.dumps(row['category_code'])


    return Row(**row)



def transform_data(sqlContext, user_sessions_chunk_df, product_attributes):

    # column-level transformations quicker with Spark Dataframes vs RDDs
    user_sessions_spDF = sqlContext.createDataFrame(user_sessions_chunk_df.astype(str))
    # user_sessions_spDF.fillna(np.nan)
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'event_time', user_sessions_spDF['event_time'].cast(TimestampType()))
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'price', user_sessions_spDF['price'].cast(FloatType()))
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'hour', hour(col('event_time')))
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'category', lit(None).cast(StringType()))
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'sub_category', lit(None).cast(StringType()))
    

    # some element-wise or row-wise operations are best with RDDs
    user_sessions_rdd = user_sessions_spDF.rdd.map(list)
    # print(user_sessions_rdd.take(5))
    user_sessions_rdd = user_sessions_spDF.rdd.map(
                        lambda row: get_product_information(row, product_attributes))

    user_sessions_spDF = user_sessions_rdd.toDF()

    # print(user_sessions_rdd.take(15))

    # print(user_sessions_spDF.take(15))

    return user_sessions_spDF



def cloudsql_connection(cloudsql_user, project, cloudsql_database, cloudsql_instance):


    config = {
    
        'user': '{0}'.format(cloudsql_user),
        # 'password': '{0}'.format(cloudsql_user_password),
        'project': '{0}'.format(project),
        'database': '{0}'.format(cloudsql_database),
        'instance': '{0}'.format(cloudsql_instance)
        # 'host': '{0}'.format(cloudsql_host)
    }


    cloudsql_user = config.get('user')
    project = config.get('project')
    cloudsql_database = config.get('database')
    cloudsql_instance = config.get('instance')

    connection_config = 'mysql+pymysql://{0}@/{1}?unix_socket=/cloudsql/{2}:{3}'.format(cloudsql_user,
                                                    cloudsql_database, project, cloudsql_instance)
    # print(connection_config)
    
    # connect to database
    cloudsql_engine = db.create_engine(connection_config, echo=True)
    cloudsqlConnection = cloudsql_engine.connect()

    return cloudsqlConnection



def write_to_cloudsql(cloudsqlConnection, table_name, user_sessions_spDF):

    user_sessions_df = user_sessions_spDF.toPandas()
    # print(user_sessions_df.head(15))
    user_sessions_df.to_sql(con=cloudsqlConnection, name=table_name, if_exists='append', index=False)



def main():

    parser = argparse.ArgumentParser(
        description='Perform Batch processing to send session data to Cloud SQL')

    parser.add_argument(
        '--input',
        help='Path to file in GCS bucket. Example: --input gs://$PROJECT/$FILE',
        required=True)

    parser.add_argument(
        '--cloudsql_user',
        help='Cloud SQL Database User; Example: --cloudsql_user user_admin',
        required=True)
    
    parser.add_argument(
        '--project',
        help='GCP Project ID; Example: --project ecommerce',
        required=True)

    parser.add_argument(
        '--cloudsql_database',
        help='Cloud SQL Database Name; Example: --cloudsql_database user_sessions',
        required=True)

    parser.add_argument(
        '--cloudsql_instance',
        help='Cloud SQL Instance Name; Example: --cloudsql_instance batch_data',
        required=True)

    parser.add_argument(
        '--cloudsql_table',
        help='MySQL Database Table; Example: --cloudsql_table batch_table',
        required=True)
    
    args = parser.parse_args()

    cloudsqlConnection = cloudsql_connection(args.cloudsql_user, args.project, args.cloudsql_database, args.cloudsql_instance)


    logging.info('Reading Dataset')
    user_sessions_chunks_df = pd.read_csv(args.input,
                                    encoding='utf-8', chunksize=int(10**5))

    conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)


    for user_sessions_chunk_df in user_sessions_chunks_df:

        logging.info('Transforming data from the Batch')
        # print(user_sessions_chunk_df.count())
        user_sessions_spDF = transform_data(sqlContext, user_sessions_chunk_df, product_attributes)

        logging.info('Loading DF Data from the Batch into batch_table Cloud SQL Table')
        write_to_cloudsql(cloudsqlConnection, args.cloudsql_table, user_sessions_spDF)


    logging.info('Finished Loading DF Data from all Batches into batch_table Cloud SQL Table')
    


if __name__ == '__main__':
    main()