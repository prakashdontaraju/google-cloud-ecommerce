import logging
from cassandra.cluster import Cluster


logging.basicConfig(level=logging.INFO)


def cassandra_connection():
    """Connection object for Cassandra"""

    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ecommerce_user_sessions
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }
        """)
    session.set_keyspace('ecommerce_user_sessions')
    return session, cluster


if __name__ == "__main__":
    logging.info('Not callable')