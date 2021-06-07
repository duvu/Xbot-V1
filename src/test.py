import pandas as pd

from stock.stock import Stock

base_group = ['gvr', 'dpr', 'hrc', 'trc']


def get_stock(s):
    x = Stock(s)
    x.load_price_board_minute(length=250)
    x.re_sample_h()
    df = x.df_day.copy()
    df['price changed%'] = df['close'].pct_change()
    df['volume changed%'] = df['volume'].pct_change()
    xf = df.iloc[-1][['close', 'price changed%', 'volume', 'volume changed%']]
    return pd.Series({'close': xf['close'], 'volume': xf['volume']})



print(get_stock('FPT'))
