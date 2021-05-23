import os
from datetime import datetime

import pymysql
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def get_connection():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    cursor = conn.cursor()
    return conn, cursor


def close_connection(conn):
    if conn:
        conn.close()


def volume_break_load_cached():
    name = datetime.now().strftime("%b%d")
    if os.path.exists('%s.cached' % name):
        with(open('%s.cached' % name, 'rt')) as f:
            return f.read()
    else:
        return None


def volume_break_save_cached(s):
    name = datetime.now().strftime("%b%d")
    with open('%s.cached' % name, 'wt') as f:
        f.write(s)
