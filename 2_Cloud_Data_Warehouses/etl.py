import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Extracts song and events data and load it into staging tables in redshift

    This function gets from s3 the raw json files which contains data about the
    songs and about the events in the platform (users listening of songs).
    It loads the data into two staging tables in redshift:
    staging_songs and staging_events.

    Args:
        cur (psycopg2.extensions.cursor): database cursor
        conn (psycopg2.extensions.connection): database connection

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Extracts data from the staging tables to fill the tables of the star schema

    This function gets data from the staging tables and uses it to fill the tables
    of the star schema. Staging tables and tables of the star schema are in the same
    redshift database, accesible by the to cur and conn arguments.

    Args:
        cur (psycopg2.extensions.cursor): database cursor
        conn (psycopg2.extensions.connection): database connection

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()