# Moi ngay 4h giao dich --> 4 * 60 = 240 cay nen phut
# 50 phien -> 50 * 240 = 12000 phut
import numpy as np
import talib

from stock.stock import Stock


def __is_break(ticker) -> bool:
    """
    a)    Kiểm tra thanh khoản mức ổn định
        1.    20 phiên trước tăng >= 1 tỷ.
        2.    10 phiên trước tăng >= 1 tỷ.
        3.    5 phiên trước tăng >= 1 tỷ.
    b)    Khớp lệnh tối thiểu
        1.    Volume trung bình 20 phiên >=100.000
        2.    5 phiên trước Volume >=100.000
        3.    10 phiên trước Volume >=100.000
        4.    10 phiên trước volume >=100.000
    c)    Tăng đột biến
        1.    Volume >50% Trung bình Volume 10 phiên trước đó.
        2.    Giá trị >=5 tỷ
    :param ticker:
    :return:
    """
    target = Stock(ticker)
    target.load_price_board_minute(length=6700)

    target.re_sample_h()
    dataframe = target.df_day.copy()
    dataframe['value'] = dataframe['volume'] * dataframe['close']

    xdf = dataframe.iloc[[-20, -11, -6, -1]].copy()
    xdf.reset_index(inplace=True, drop=True)
    xdf['value_changed'] = xdf['value'].diff()
    xdf['VALUE_1B'] = np.where(xdf['value_changed'] > 1000000000, True, False)
    DK1 = xdf['VALUE_1B'].tail(3).all()

    dataframe['VOL_SMA_20'] = talib.SMA(dataframe['volume'], timeperiod=20)
    dataframe['VOL_SMA_10'] = talib.SMA(dataframe['volume'], timeperiod=10)
    dataframe['VOL_SMA_5'] = talib.SMA(dataframe['volume'], timeperiod=5)

    dataframe['VOL_SMA_5_100K'] = np.where(dataframe['VOL_SMA_5'] > 100000, True, False)
    dataframe['VOL_SMA_10_100K'] = np.where(dataframe['VOL_SMA_10'] > 100000, True, False)
    dataframe['VOL_SMA_20_100K'] = np.where(dataframe['VOL_SMA_20'] > 100000, True, False)

    DK2 = dataframe['VOL_SMA_5_100K'].tail(20).all() and dataframe['VOL_SMA_10_100K'].tail(20).all() and dataframe['VOL_SMA_20_100K'].tail(20).all()

    dataframe['volume_break'] = np.where(dataframe['volume'] >= (dataframe['VOL_SMA_10'] * 1.5), True, False)
    dataframe['value_break'] = np.where(dataframe['value'] > 5000000000, True, False)
    dataframe['break_out'] = dataframe['volume_break'] & dataframe['value_break']
    DK3 = dataframe['value_break'].tail(1).all()
    # Delete instance of Ticker
    del target
    return DK1 and DK2 and DK3
