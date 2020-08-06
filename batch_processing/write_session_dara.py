import datetime
import numpy as np
from pyspark import SparkConf, SparkContext



TIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'
INPUT = './data/2019-Nov-Sample.csv'



conf = SparkConf().setAppName("Batch Processing with Spark").setMaster("local")
# Set driver and executor memory and max result size
conf = conf.set('spark.executor.memory', '10G')\
           .set('spark.driver.memory', '10G')\
           .set('spark.driver.maxResultSize', '2G')
sc = SparkContext(conf = conf)



def preprocess_data(user_sessions):

    user_sessions = user_sessions.map(lambda x: x.split("\n"))
    user_sessions = user_sessions.map(lambda x: ','.join(x).split(","))
    fill_missing_data = user_sessions.map(lambda x: [value if value!='' else np.nan for value in x])
    return fill_missing_data




def split_header(user_sessions):

    user_sessions_header = user_sessions.first()
    user_sessions_data = user_sessions.filter(lambda x: x!=user_sessions_header)
    
    return user_sessions_header, user_sessions_data



def transform_data(record):

    record[0] = datetime.datetime.strptime(record[0],TIME_FORMAT)
    record[6] = float(record[6]) 

    return record



user_sessions = sc.textFile(INPUT)
user_sessions = preprocess_data(user_sessions)
header, user_sessions = split_header(user_sessions)
user_sessions = user_sessions.map(transform_data)

print(header)
print(user_sessions.take(10))