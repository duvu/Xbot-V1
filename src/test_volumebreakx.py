# Moi ngay 4h giao dich --> 4 * 60 = 240 cay nen phut
# 50 phien -> 50 * 240 = 12000 phut
import numpy as np
import talib

from stock.stock import Stock

sf = Stock('APG')
sf.load_price_board_minute(length=6600)
sf.re_sample_h()
df = sf.df_day.copy()

df['value'] = df['volume'] * df['close']

xdf = df.iloc[[-20, -11, -6, -1]].copy()
xdf.reset_index(inplace=True, drop=True)
xdf['value_changed'] = xdf['value'].diff()
xdf['VALUE_1B'] = np.where(xdf['value_changed'] > 1000000000, True, False)
DK1 = xdf['VALUE_1B'].tail(3).all()
print(xdf)
print('DK1', DK1)

df['VOL_SMA_20'] = talib.SMA(df['volume'], timeperiod=20)
df['VOL_SMA_10'] = talib.SMA(df['volume'], timeperiod=10)
df['VOL_SMA_5'] = talib.SMA(df['volume'], timeperiod=5)


df['VOL_SMA_5_100K'] = np.where(df['VOL_SMA_5'] > 100000, True, False)
df['VOL_SMA_10_100K'] = np.where(df['VOL_SMA_10'] > 100000, True, False)
df['VOL_SMA_20_100K'] = np.where(df['VOL_SMA_20'] > 100000, True, False)

DK2 = df['VOL_SMA_5_100K'].tail(20).all() and df['VOL_SMA_10_100K'].tail(20).all() and df['VOL_SMA_20_100K'].tail(20).all()

# print(df)
print('DK2', DK2)

df['volume_break'] = np.where(df['volume'] >= (df['VOL_SMA_10'] * 1.5), True, False)
df['value_break'] = np.where(df['value'] > 5000000000, True, False)
df['break_out'] = df['volume_break'] & df['value_break']
DK3 = df['value_break'].tail(1).all()

print('DK3', DK3)


del sf
