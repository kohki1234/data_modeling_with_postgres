import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    This function drop database if exists, and creates database. Also it creates connection to DB and cursor \
    instance.
    
    Args:
        this function doesn't accept arguments

    Returns:
        cur(psycopg2.extensions.cursor): cursor instance 
        conn(psycopg2.extensions.connection) : psycopg2 connection instance
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    This function drops tables according to drop_table_queries array list in sql_queries.py file.
    
    Args:
        cur(psycopg2.extensions.cursor): cursor instance 
        conn(psycopg2.extensions.connection) : psycopg2 connection instance

    Returns:
        This fuction returns nothing.
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates tables according to create_table_queries array list in sql_queries.py file.
    
    Args:
        cur(psycopg2.extensions.cursor): cursor instance 
        conn(psycopg2.extensions.connection) : psycopg2 connection instance

    Returns:
        This fuction returns nothing.
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function calls drop_tables() and create_tables() and close the connection in the end.
    
    Args:
        this function doesn't accept arguments

    Returns:
        This fuction returns nothing.
    """
    
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()