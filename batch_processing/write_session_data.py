import json
import datetime
import numpy as np
import pandas as pd
from pyspark.sql import Row, SQLContext
from gcsfs.core import GCSFileSystem
from pyspark.sql.functions import split as Split, size
# from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import TimestampType, FloatType


product_attributes = ['category', 'sub_category', 'product','product_details']



# def transform_product_information


# @pandas_udf(user_sessions_spDF.schema, PandasUDFType.MAP_ITER)  
# def get_product_information(iterator):


#     for pdf in iterator:

#         yield pdf[pdf.category_code = transform_product_information(pdf.category_code)]

# def get_product_information(user_sessions_spDF, split_columns, product_attributes):

#     spDF = user_sessions_spDF

#     spDF = spDF.withColumn(product_attributes[0], split_columns.getItem(0))
#     spDF = spDF.withColumn(product_attributes[1], split_columns.getItem(1))
#     spDF = spDF.withColumn(product_attributes[2], split_columns.getItem(2))
#     spDF = spDF.withColumn(product_attributes[3], split_columns.getItem(3))


    # detail_level = size(split_columns)
    # print(detail_level)
    # if split_columns.getItem(0):
    #     spDF[product_attributes[0]] = split_columns.getItem(0)
    #     if split_columns.getItem(1):
    #         spDF[product_attributes[1]] = split_columns.getItem(1)
    #         if split_columns.getItem(2):
    #             spDF[product_attributes[2]] = split_columns.getItem(2)
    #             if split_columns.getItem(3):
    #                 spDF[product_attributes[3]] = split_columns.getItem(3)
    #             else:
    #                 spDF[product_attributes[3]] = np.nan
    #         else:
    #             spDF[product_attributes[2]] = np.nan
    #     else:
    #         spDF[product_attributes[1]] = np.nan
    # else:
    #     spDF[product_attributes[0]] = np.nan

    # return spDF


def get_product_information(row, product_attributes):

    category_code = row.category_code

    details = category_code.split('.')
    # detail_level = len(details)

    # for level in range(detail_level):
    #     row[level+9] = details[level]

    row = row.asDict()

    row['category_code'] = dict(zip(product_attributes, details))
    json.dumps(row['category_code'])

    return Row(**row)







def transform_data(sqlContext, user_sessions_chunk_df, product_attributes):

    # column-level transformations quicker with Spark Dataframes vs RDDs
    user_sessions_spDF = sqlContext.createDataFrame(user_sessions_chunk_df.astype(str))
    user_sessions_spDF.fillna(np.nan)
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'event_time', user_sessions_spDF['event_time'].cast(TimestampType()))
    user_sessions_spDF = user_sessions_spDF.withColumn(
                            'price', user_sessions_spDF['price'].cast(FloatType()))
    
    # print(user_sessions_spDF.show(n=5))

    # split_columns = Split(user_sessions_spDF['category_code'],'.')
    # user_sessions_spDF = get_product_information(user_sessions_spDF, split_columns, product_attributes)


    # user_sessions_spDF = user_sessions_spDF.withColumn(
    #                     'product_information', get_product_information(user_sessions_spDF['category_code']))

    # user_sessions_spDF = user_sessions_spDF.mapInPandas(get_product_information)
    # print(user_sessions_spDF.show(n=5))
    # user_sessions_spDF.select(product_attributes).show(n=5)

    # some element-wise or row-wise operations are best with RDDs
    user_sessions_rdd = user_sessions_spDF.rdd.map(list)
    print(user_sessions_rdd.take(5))
    user_sessions_rdd = user_sessions_spDF.rdd.map(
                        lambda row: get_product_information(row, product_attributes))

    return user_sessions_rdd









def main():

    user_sessions_chunks_df = pd.read_csv('gs://ecommerce-283019/2019-Nov-Sample.csv',
                                    encoding='utf-8', chunksize=int(10**5))

    conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
     
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    for user_sessions_chunk_df in user_sessions_chunks_df:
        user_sessions_rdd = transform_data(sqlContext, user_sessions_chunk_df, product_attributes)
        print(user_sessions_rdd.take(5))
        
        break


if __name__ == '__main__':
    main()