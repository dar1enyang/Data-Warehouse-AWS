import configparser
import psycopg2
import sys
import os
sys.path.append('..')
from query.sql_queries import copy_table_queries, insert_table_queries, set_schema_queries


def load_staging_tables(cur, conn):
    """
    The function to load all staging tables to the database
    
    Returns:
        cur  : Use the connection to get a cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
    """

    for schema in set_schema_queries:
        cur.execute(schema)
        conn.commit()
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()


def insert_tables(cur, conn):
    """
    The function to insert records into the table in the database
    
    Returns:
        cur  : Use the connection to get a cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
    """

    for schema in set_schema_queries:
        cur.execute(schema)
        conn.commit()
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()