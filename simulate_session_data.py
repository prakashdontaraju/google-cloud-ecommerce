import time
import logging
import argparse
import datetime
import pandas as pd
from google.cloud import pubsub


TOPIC = 'user_data_topic'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
INPUT = 'gs://ecommerce-283019/2019-Nov.csv'


def preprocess_data(user_sessions):
    """Transforms dataframe into list of lists """

    # Transform event_time from string to timestamp (datetime)
    user_sessions['event_time'] = pd.to_datetime(user_sessions['event_time'],
    format='%Y-%m-%d %H:%M:%S %Z')
    # Eliminate Time Zone from timestamp (datetime)
    user_sessions['event_time'] = pd.to_datetime(user_sessions['event_time'],
    format=TIME_FORMAT)

    # Display first 5 rows of dataframe
    # logging.info('Dataset CSV to Dataframe: {0}'.format(user_sessions.head()))

    # Transform dataframe into list of lists
    user_sessions = user_sessions.to_string(header=False, index=False,index_names=False).split('\n')
    logging.info('Dataframe to List of Lists, First row - {0}'.format(user_sessions[0]))
      
    return user_sessions


def clean_data(record):
    """Transforms Record from List of Strings to Single ',' separated byte message"""
    record = ','.join(record.split())
    return record.encode('utf-8')



def get_timestamp(record):
    """Returns Timestamp in '%Y-%m-%d %H:%M:%S' format"""

    record = record.decode('utf-8')
    # logging.info('\n event data {} \n'.format(record))
    date_part = record.split(',')[0]
    # logging.info('\n date part {} \n'.format(date_part))
    time_part = record.split(',')[1]
    # logging.info('\n time part {} \n'.format(time_part))
    time_useful = time_part.split('+')[0]
    # logging.info('\n time useful part {} \n'.format(time_useful))
    timestamp = date_part + ' ' + time_useful
    # logging.info('\n timestamp {} \n'.format(timestamp))
    return datetime.datetime.strptime(timestamp, TIME_FORMAT)




def publish(publisher, topic, events):
    """Publishes All Events from a particular Timestamp"""
    logging.info('Publishing events from {0}'.format(get_timestamp(events[0])))
    publisher.publish(topic,events[0])
         


def compute_wait_time(obs_time, prevObsTime):
    """Calculates Wait Time between Events to Simulate Streaming"""
    wait_time = (obs_time - prevObsTime).seconds
    return wait_time



def stream_data_chunk(user_sessions, publisher, pub_topic):
    """Transforms Events from Data into Byte Messages to Simulate Streaming"""
    topublish = list()

   
    first_record = clean_data(user_sessions[0])
    # Calculate timestamp of first row
    firstObsTime = get_timestamp(first_record)
    logging.info('Sending session data from {0}'.format(firstObsTime))

    # logging.info('First Record {}'.format(first_record))

    prevObsTime = firstObsTime
    for event_data in user_sessions:

        event_data = clean_data(event_data)
        # Calculate timestamp of current row
        obs_time = get_timestamp(event_data)
        # logging.info('Obs Time {} '.format(obs_time))
        # logging.info('Previous Obs Time {} '.format(prevObsTime))
        wait_time = compute_wait_time(obs_time, prevObsTime)
        # logging.info('Wait Time {} '.format(wait_time))

        if wait_time > 0:
            # Wait for (wait_time) seconds between events to simulate streaming
            logging.info('Waiting for {0} second(s) to simulate real time stream data'.format(wait_time))
            time.sleep(wait_time)
        else:
            pass

        # Add to list of events ready to publish
        topublish.append(event_data)

        # Publish the accumulated topublish events
        publish(publisher, pub_topic, topublish) # notify accumulated messages

        # Empty the list
        topublish = list()

        # Current event time becomes previous event time for the next event
        prevObsTime = obs_time

    # Publish left-over records
    publish(publisher, pub_topic, topublish)



def main():

   parser = argparse.ArgumentParser(description='Send session data to Cloud Pub/Sub in small groups, simulating real-time behavior')
   parser.add_argument('--project', help='Example: --project $DEVSHELL_PROJECT_ID', required=True)
   args = parser.parse_args()

   # create Pub/Sub notification topic
   logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
   publisher = pubsub.PublisherClient()
   pub_topic = publisher.topic_path(args.project,TOPIC)
   try:
      publisher.get_topic(pub_topic)
      logging.info('Utilizing pub/sub topic {0}'.format(TOPIC))
   except:
      publisher.create_topic(pub_topic)
      logging.info('Creating pub/sub topic {0}'.format(TOPIC))
    

   # Read dataset 1 chunk at once
   logging.info('Reading Data from CSV File')
   user_session_chunks = pd.read_csv(INPUT, chunksize=10**4)
   logging.info('Pre-Processing CSV Data')
   for user_sessions in user_session_chunks:
       # Preprocess data in a chunk
       user_sessions = preprocess_data(user_sessions)
       # Transform Data into Messages and Simulate Streaming
       stream_data_chunk(user_sessions, publisher, pub_topic)



if __name__ == '__main__':
   main()