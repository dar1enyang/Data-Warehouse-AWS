import configparser
import psycopg2
import sys
import os
sys.path.append('..')
from query.sql_queries import * 


def drop_tables(cur, conn):
    """
    The function to drop all tables in the database
    
    Returns:
        cur  : Use the connection to get a cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
    """

    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()

    for schema in set_schema_queries:
        cur.execute(schema)
        conn.commit()
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()


def create_tables(cur, conn):
    """
    The function to create tables in the database
    
    Returns:
        cur  : Use the connection to get a cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
    """

    cur.execute(nodist_schema_set)
    conn.commit()
    for query in create_no_dist_table_queries:
        cur.execute(query)
        conn.commit()
    cur.execute(dist_schema_set)
    conn.commit()
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()