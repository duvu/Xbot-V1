import os
import multiprocessing as mp
from datetime import datetime, time
import pandas as pd
from datequarter import DateQuarter
from dotenv import load_dotenv

from db.database import get_connection

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


def get_title():
    return ""


def resample_trending_interval(dataframe: pd.DataFrame, interval):
    df = dataframe.copy()
    df = df.set_index(pd.DatetimeIndex(df["date"]))
    z_dict = {"symbol": "first", "count": "sum"}
    df = df.resample(str(interval) + "min", kind='timestamp').agg(z_dict).dropna()
    df.reset_index(inplace=True)
    return df

