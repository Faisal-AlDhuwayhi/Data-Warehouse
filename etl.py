import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy the Song and Log datasets from S3 and load them into staging tables on Redshift.
    
    Args:
    1) cur (psycopg2 cursor object): a database cursor object
    2) conn (psycopg2 connection object): a database connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data into the analytics tables on Redshift from the staging tables.
    
    Args:
    1) cur (psycopg2 cursor object): a database cursor object
    2) conn (psycopg2 connection object): a database connection object
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