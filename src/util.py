import multiprocessing as mp
import os
import re
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from db.database import get_connection, close_connection

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
CHANNEL_ID = os.getenv('DISCORD_CHANNEL')
PYTHON_ENVIRONMENT = os.getenv('PYTHON_ENVIRONMENT')


def get_pool():
    """
    Spare 1 core for other command
    :return:
    """
    return mp.Pool(mp.cpu_count() - 1)


async def send_info(ctx):
    """
    send message if wrong command
    :param ctx:
    :return:
    """
    with(open('./qbot.md', 'rt')) as f:
        msg = f.read()
        if PYTHON_ENVIRONMENT == 'production':
            await ctx.send('```%s```' % msg, delete_after=300.0)
        else:
            print(msg)


def insert_mentioned_code(matches, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message_id, message):
    if len(matches) > 0:
        conn, cursor = get_connection()
        sql_string = """INSERT INTO tbl_mentions(symbol, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message_id, message_title, message) VALUES (%s, timestamp \'%s\', %s, %s, %s, %s, %s, %s)"""
        print(sql_string)
        for x in matches:
            print('symbol', x)
            print('mentioned_at', mentioned_at)
            print('mentioned_by_id', mentioned_by_id)
            print('mentioned_by', mentioned_by)
            print('mentioned_in', mentioned_in)
            print('message_id', message_id)
            print('message', message)
            try:
                cursor.execute(sql_string, (x, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message_id, 'discord', message))
                print("Query", cursor.query)
            except Exception as ex:
                print('[ERROR] %s' % ex)
            finally:
                conn.commit()
        close_connection(conn)


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


def is_women(roles):
    print('Roles', roles)
    return any(x.name == 'Nữ' for x in roles)


def get_title(roles):
    ac = 'chị' if any(x.name == 'Nữ' for x in roles) else 'anh'
    return ac


def resample_trending_interval(dataframe: pd.DataFrame, interval):
    df = dataframe.copy()
    df = df.set_index(pd.DatetimeIndex(df["date"]))
    z_dict = {"symbol": "first", "count": "sum"}
    df = df.resample(str(interval) + "min", kind='timestamp').agg(z_dict).dropna()
    df.reset_index(inplace=True)
    return df


def split(txt):
    return re.split(r'[`\-=~!@#$%^&*()_+\[\]{};\'\\:"|<,./<> ]', txt)
