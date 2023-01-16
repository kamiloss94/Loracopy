import os

import psycopg2
import sys

# Connection parameters
param_dic = {
    "host"      : os.getenv("DB_HOST"),
    "port"      : os.getenv("DB_PORT"),
    "database"  : os.getenv("DB_NAME"),
    "user"      : os.getenv("DB_USER"),
    "password"  :  os.getenv("DB_PASSWORD")
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

conn = connect(param_dic)
