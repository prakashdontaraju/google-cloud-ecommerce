import argparse
import datetime
import logging
import time
import pandas as pd
from google.cloud import pubsub_v1


def preprocess_data(user_sessions):
    """Transforms data before inserting into BigQuery table."""

    # Transform event_time from string to timestamp (datetime)
    user_sessions['event_time'] = pd.to_datetime(user_sessions['event_time'],
    format='%Y-%m-%d %H:%M:%S %Z')
    # Eliminate Time Zone from timestamp (datetime)
    user_sessions['event_time'] = pd.to_datetime(user_sessions['event_time'],
    format='%Y-%m-%d %H:%M:%S')

    # Display first 5 rows of dataframe
    # logging.info('Dataset CSV to Dataframe: {0}'.format(user_sessions.head()))

    # Transform dataframe into list of lists
    user_sessions = user_sessions.to_string(
        header=False, index=False,index_names=False).split('\n')
    # logging.info('Dataframe to List of Lists, First row - {0}'.format(user_sessions[0]))
      
    return user_sessions


def clean_data(record):
    """Cleans data to get single ',' separated byte messages."""
    record = ','.join(record.split())
    return record.encode('utf-8')


def get_timestamp(record):
    """Returns Timestamp in '%Y-%m-%d %H:%M:%S' format."""

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
    return datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')



def publish(publisher, topic_path, events):
    """Publishes All Events from a particular Timestamp."""
    # Notify accumulated messages
    logging.info(
        'Publishing event(s) from {0}'.format(get_timestamp(events[0])))
    # future = publisher.publish(topic_path, events[0])
    # print(future.result())
    publisher.publish(topic_path, events[0])


def compute_wait_time(obs_time, prevObsTime):
    """Calculates wait time between events to simulate streaming."""
    wait_time = (obs_time - prevObsTime).seconds
    return wait_time


def stream_data_chunk(user_sessions, publisher, topic_path):
    """Transforms event data into Byte messages to simulate streaming."""
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
    
        wait_time = compute_wait_time(obs_time, prevObsTime)
        # logging.info('Wait Time {} '.format(wait_time))

        if wait_time > 0:
            # Wait for (wait_time) seconds between events to simulate streaming
            wait_message = 'Waiting for {0} second(s) to simulate real time stream data'.format(wait_time)
            logging.info(wait_message)
            time.sleep(wait_time)
        else:
            pass

        # Add to list of events ready to publish
        topublish.append(event_data)

        # Publish the accumulated topublish events
        publish(publisher, topic_path, topublish)

        # Empty the list
        topublish = list()

        # Current event time becomes previous event time for the next event
        prevObsTime = obs_time
        # logging.info('Previous Obs Time {} '.format(prevObsTime))


def main():
    """Executes Steaming pipeline to store dataset into BigQuery table."""

    parser = argparse.ArgumentParser(
        description=('Send session data to Cloud Pub/Sub' +
        ' simulating real-time messaging'))
   
    parser.add_argument(
        '--project',
        help='Example: --project $DEVSHELL_PROJECT_ID',
        required=True)
   
    parser.add_argument(
        '--topic',
        help='Topic name to publish messages. Example: --topic $TOPIC_NAME',
        required=True)

    parser.add_argument(
        '--subscription',
        help='''Subscription name to receive messages. 
            Example: --subscription $SUBSCRIPTION_NAME''',
        required=True)
        
    parser.add_argument(
        '--input',
        help='Path to file in GCS bucket. Example: --input gs://$PROJECT/$FILE',
        required=True)
   
    args = parser.parse_args()

    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(args.project,args.topic)
    subscription_path = subscriber.subscription_path(
        args.project, args.subscription)

    # create topic to publish and subscription to receive messages
    topic = publisher.create_topic(request={"name": topic_path})
    logging.info('Created pub/sub topic {0}'.format(topic))
    with subscriber:
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
    logging.info('Created pub/sub subscription {0}'.format(subscription))
    
    # Read dataset 1 chunk at once
    logging.info('Reading Data from CSV File')
    user_session_chunks = pd.read_csv(args.input, chunksize=10**5)
    logging.info('Pre-Processing CSV Data')
    for user_sessions in user_session_chunks:
        # Preprocess data in a chunk
        user_sessions = preprocess_data(user_sessions)
        # Transform Data into Messages and Simulate Streaming
        stream_data_chunk(user_sessions, publisher, topic_path)


if __name__ == '__main__':
   main()