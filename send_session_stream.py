import time
import logging
import argparse
import datetime
import pandas as pd
from google.cloud import pubsub_v1



TIME_FORMAT = '%Y-%m-%d %H:%M:%S'



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
    # logging.info('Dataframe to List of Lists, First row - {0}'.format(user_sessions[0]))
      
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
    publisher.publish(topic,events[0])
         


def stream_data_chunk(user_sessions, publisher, pub_topic, speedFactor):
    """Transforms Events from Data into Byte Messages to Simulate Streaming"""
    topublish = list()

   
    first_record = clean_data(user_sessions[0])
    # Calculate timestamp of first row
    firstObsTime = get_timestamp(first_record)
    logging.info('Sending session data from {0}'.format(firstObsTime))

    # logging.info('First Record {}'.format(first_record))

  
    # Notify first message from a particular chunk
    logging.info('Publishing event(s) starting from {0}'.format(firstObsTime))

    for event_data in user_sessions:

        event_data = clean_data(event_data)

        # Add to list of events ready to publish
        topublish.append(event_data)

        # Publish the accumulated topublish events
        publish(publisher, pub_topic, topublish)

        # Empty the list
        topublish = list()


    # Wait for 60 seconds between events to simulate streaming
    logging.info('Waiting 1 minute to simulate streaming of the next {0} hour(s) of data'.format(speedFactor))
    time.sleep(60)



def main():

    parser = argparse.ArgumentParser(
        description='Send session data to Cloud Pub/Sub simulating real-time messaging')
   
    parser.add_argument(
        '--project',
        help='Example: --project $DEVSHELL_PROJECT_ID',
        required=True)
       
    parser.add_argument(
        '--topic',
        help='Topic name to publish messages. Example: --topic $TOPIC_NAME',
        required=True)

    parser.add_argument(
        '--input',
        help='Path to file in GCS bucket. Example: --input gs://$PROJECT/$FILE',
        required=True)
    
    parser.add_argument(
        '--speedFactor',
        help='Hours of data (<6) to publish in 1 minute. For 1 hour(s) of data: --speedfactor=1',
        type=int, choices=[1, 2, 3, 4, 5], required=True)
   
    args = parser.parse_args()

    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub_v1.PublisherClient()
    pub_topic = publisher.topic_path(args.project,args.topic)
    try:
        publisher.get_topic(pub_topic)
        logging.info('Utilizing pub/sub topic {0}'.format(args.topic))
    except:
        publisher.create_topic(pub_topic)
        logging.info('Creating pub/sub topic {0}'.format(args.topic))
    

    # Read dataset 1 chunk at once
    logging.info('Reading Data from CSV File')
    user_session_chunks = pd.read_csv(args.input, chunksize=int(args.speedFactor*(10**4)))
    logging.info('Pre-Processing CSV Data')
    for user_sessions in user_session_chunks:
        # Preprocess data in a chunk
        user_sessions = preprocess_data(user_sessions)
        # Transform Data into Messages and Simulate Streaming
        stream_data_chunk(user_sessions, publisher, pub_topic, args.speedFactor)



if __name__ == '__main__':
   main()