import logging
import argparse
from connect_to_cassandra import cassandra_connection, close_cassandra_connection


logging.basicConfig(level=logging.INFO)


def create_tables(cluster, session, table):


    drop_table_if_exists = 'DROP TABLE IF EXISTS {table}'.format(table=table)
    session.execute(drop_table_if_exists)

    create_batch_data_table = "CREATE TABLE IF NOT EXISTS batch_data (" \
                                    "event_time timestamp" \
                                    ", event_type text" \
                                    ", product_id text" \
                                    ", category_id text" \
                                    ", category_code text" \
                                    ", brand text" \
                                    ", price float" \
                                    ", user_id text" \
                                    ", user_session text" \
                                    ", event_details text" \
                                    ", record_id float" \
                                    ", PRIMARY KEY (event_details, record_id))"
                                    # ", category text" \
                                    # ", sub_category text" \
                                    # ", product text" \
                                    # ", product_details text" \
    
    logging.info('Creating Batch Processing table in Cassandra')
    session.execute(create_batch_data_table)
    
    # close_cassandra_connection(cluster, session)



def delete_tables(cluster, session, table):


    logging.info('Deleting Batch Processing table in Cassandra')
    drop_table = 'DROP TABLE {table}'.format(table=table)
    session.execute(drop_table)

    close_cassandra_connection(cluster, session)



def main():
    """
    Main script that tears down and rebuilds the tables within Cassandra
    """

    parser = argparse.ArgumentParser(
        description='Create Cassandra Table to Perform Batch Processing')

    parser.add_argument(
        '--port',
        help='Port to listen to Cassandra. Example: --port=9042',
        type = int,
        required=True)

    args = parser.parse_args()

    cluster, session = cassandra_connection(args.port)

    table_list = ['batch_data']

    for table in table_list:
        create_tables(cluster, session, table)
        # delete_tables(cluster, session, table)
        
    


if __name__ == "__main__":
    main()