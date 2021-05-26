import os
from datetime import datetime
import pandas as pd


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


def resample_trending_interval(dataframe: pd.DataFrame, interval):
    df = dataframe.copy()
    df = df.set_index(pd.DatetimeIndex(df["date"]))
    z_dict = {"symbol": "first", "count": "sum"}
    df = df.resample(str(interval) + "min", kind='timestamp').agg(z_dict).dropna()
    df.reset_index(inplace=True)
    return df
