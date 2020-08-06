import logging
import connect_to_cassandra


logging.basicConfig(level=logging.INFO)


def main():
    """
    Main script that tears down and rebuilds the tables within Cassandra
    """
    cluster, session = connect_to_cassandra.cassandra_connection()

    try:
        table_list = ['batch_data']

        for table in table_list:
            drop_table = 'DROP TABLE IF EXISTS {table}'.format(table=table)
            session.execute(drop_table)

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
                                    ", hour int" \
                                    ", category text" \
                                    ", sub_category text" \
                                    ", product text" \
                                    ", product_details text" \
                                    ", PRIMARY KEY (event_id))"
        logging.info('Creating Batch Processing table in Cassandra')
        session.execute(create_batch_data_table)

    except Exception as e:
        print(e)

    finally:
        logging.info('Closing connection to Cassandra')
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()