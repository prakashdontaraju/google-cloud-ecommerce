import datetime
import numpy as np
import pandas as pd
from pyspark.sql import SQLContext
from gcsfs.core import GCSFileSystem
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import TimestampType, FloatType




def transform_data(sqlContext, user_sessions_chunk_df):

    user_sessions_spDF = sqlContext.createDataFrame(user_sessions_chunk_df.astype(str))
    user_sessions_spDF.fillna(np.nan)
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'event_time', user_sessions_spDF['event_time'].cast(TimestampType()))
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'price', user_sessions_spDF['price'].cast(FloatType()))
    
    # print(user_sessions_spDF.show(n=5))

    # split category code into sub categories

    return user_sessions_spDF


def main():

    user_sessions_chunks_df = pd.read_csv('gs://ecommerce-283019/2019-Nov-Sample.csv',
                                    encoding='utf-8', chunksize=int(10**5))

    conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    for user_sessions_chunk_df in user_sessions_chunks_df:
        transform_data(sqlContext, user_sessions_chunk_df)
        
        break


if __name__ == '__main__':
    main()