import pandas as pd
import numpy as np
import talib

import stock
import matplotlib.pyplot as plt

from db.database import get_connection, close_connection


def calc_sma(df: pd.DataFrame):
    if df is not None:
        dataframe = df.copy()
        dataframe['SMA_5'] = talib.SMA(dataframe['close'], timeperiod=5)
        dataframe['SMA_10'] = talib.SMA(dataframe['close'], timeperiod=10)
        dataframe['SMA_20'] = talib.SMA(dataframe['close'], timeperiod=20)
        dataframe['SMA_50'] = talib.SMA(dataframe['close'], timeperiod=50)
        return dataframe


# -- Dellphic from @Phan Hieu -- #
# function isOver (dgTren, dgDuoi, batdau, ketthuc)
# {
# 	kq=True;
# 	for (i=batdau; i<ketthuc; i++)
# 	{
# 		kq= kq AND IIf(Ref (dgTren,i) > Ref (dgDuoi,i), True, False);
# 	}
# 	return kq;
#
# }
# DK1= LLV (MA(C, 5), 40) < LLV (MA(C, 20), 40); // Gia tri nho nhat Ma5 < MA20 hay Ma5<Ma20
# DK2= LLV (MA(C, 20), 40) < LLV (MA (C, 50), 40);
# DK3= HHV (MA(C,5), 20) > HHV (MA (C, 20), 20);
# DK4= HHV (MA(C,20), 20) > HHV (MA (C, 50), 20);
# DK5= isOver(MA(C, 5), MA(C, 50), -10,0);
# DK6= Cross(MA(C,5), MA(C,20));
# DK7=V>150000;
# Buy = DK1 AND DK2 AND DK3 AND DK4 AND DK5 AND DK6 AND DK7;
# Filter=Buy;

def dellphic(df):
    if df is not None:
        dataframe = calc_sma(df)
        DK1 = dataframe['SMA_5'].rolling(window=40).min() < dataframe['SMA_20'].rolling(window=40).min()
        DK3 = dataframe['SMA_5'].rolling(window=20).max() > dataframe['SMA_20'].rolling(window=20).max()

        DK2 = dataframe['SMA_20'].rolling(window=40).min() < dataframe['SMA_50'].rolling(window=40).min()
        DK4 = dataframe['SMA_20'].rolling(window=20).max() > dataframe['SMA_50'].rolling(window=20).max()
        DK5 = dataframe['SMA_5'] > dataframe['SMA_20']

        # Check if SMA5 cross SMA20
        dataframe['X_CROSS'] = np.where(dataframe['SMA_5'] > dataframe['SMA_20'], 1, 0)
        DK6 = dataframe['X_CROSS'].diff() == -1
        # DK7 = dataframe['volume'] > 150000
        return DK1 & DK2 & DK3 & DK4 & DK6
    else:
        return pd.Series([False])


def reload_company_list():
    # Update company list
    conn, cursor = get_connection()
    sql_query = pd.read_sql_query('''select distinct code from tbl_price_board_day where v > 150000 and t > (unix_timestamp() - (86400 * 7))''', conn)
    close_connection(conn)
    return list(pd.DataFrame(sql_query)['code'])


# AAS', 'BID', 'CTF', 'CTS', 'DRI', 'IDJ', 'NDN', 'OIL', 'PET', 'PVD', 'SBS

def main():
    company_list = reload_company_list()
    goot_codes = []
    for c in company_list:
        print('Code: ', c)
        s = stock.Stock(code=c)
        s.load_price_board_day(100)
        # s.load_price_board_minute(6000)
        # s.re_sample_h()
        # df_h1 = s.df_h1
        df = s.df_day.copy()
        if dellphic(df).tail(1).any():
            goot_codes.append(c)

        del s

    print('Good codes: %s' % goot_codes)

    # s = stock.Stock(code='HPG')
    # s.load_price_board_day(200)
    # print(s.df_day)
    # df_day = s.df_day.reindex(index=s.df_day.index[::-1])
    # df_day.reset_index(inplace=True, drop=True)
    # print(df_day)
    #
    # df_day_with_smas = calc_sma(df_day)
    # df_day_with_smas['SMA_5_MIN_40'] = df_day_with_smas['SMA_5'].rolling(window=40).min()
    # df_day_with_smas['SMA_5_MAX_20'] = df_day_with_smas['SMA_5'].rolling(window=20).max()
    #
    # df_day_with_smas['SMA_20_MIN_40'] = df_day_with_smas['SMA_20'].rolling(window=40).min()
    # df_day_with_smas['SMA_20_MAX_20'] = df_day_with_smas['SMA_20'].rolling(window=20).max()
    #
    # df_day_with_smas['SMA_50_MIN_40'] = df_day_with_smas['SMA_50'].rolling(window=40).min()
    # df_day_with_smas['SMA_50_MAX_20'] = df_day_with_smas['SMA_50'].rolling(window=20).max()
    #
    # df_day_with_smas['CROSS_MA5_MA20'] = np.where(df_day_with_smas['SMA_5'] > df_day_with_smas['SMA_20'], 1.0, 0.0)
    # df_day_with_smas['CROSS_MA20_MA50'] = np.where(df_day_with_smas['SMA_5'] > df_day_with_smas['SMA_20'], 1.0, 0.0)
    # df_day_with_smas['MA5_X_MA20'] = df_day_with_smas['CROSS_MA5_MA20'].diff()
    # df_day_with_smas['MA20_X_MA50'] = df_day_with_smas['CROSS_MA20_MA50'].diff()
    # # df_day_with_smas['dellphic'] = dellphic(df_day)
    #
    # df_day_with_smas['CCI'] = talib.CCI(df_day_with_smas['high'], df_day_with_smas['low'], df_day_with_smas['close'], timeperiod=20)
    # df_day_with_smas['CCI_DIFF'] = df_day_with_smas['CCI'].diff()
    #
    # df_day_with_smas['CURVE'] = np.where(df_day_with_smas['CCI_DIFF'] > 0, 1, 0)
    # df_day_with_smas['CURVE_DIFF'] = df_day_with_smas['CURVE'].diff()
    #
    # print(df_day_with_smas)
    # # df_day_with_smas.to_excel('./outputs/vpb_dellphic.xlsx')
    #
    # # -------------
    # # fig, (ax1, ax2) = plt.subplots(2, sharex=True)
    # plt.figure(figsize=(10, 20))
    # plt.tick_params(axis='both', labelsize=14)
    # df_day_with_smas['close'].plot(color='k', lw=1, label='Close Price')
    # df_day_with_smas['SMA_5'].plot(color='b', lw=1, label='SMA_5')
    # df_day_with_smas['SMA_20'].plot(color='g', lw=1, label='SMA_20')
    # df_day_with_smas['SMA_50'].plot(color='m', lw=1, label='SMA_50')
    # df_day_with_smas['CCI'].plot(color='c', lw=1, label='CCI')
    #
    # # plot 'buy' signals
    # plt.plot(df_day_with_smas[df_day_with_smas['MA5_X_MA20'] == 1].index,
    #          df_day_with_smas['SMA_5'][df_day_with_smas['MA5_X_MA20'] == 1],
    #          '^', markersize=15, color='g', alpha=0.7, label='buy')
    #
    # plt.plot(df_day_with_smas[df_day_with_smas['MA20_X_MA50'] == 1].index,
    #          df_day_with_smas['SMA_5'][df_day_with_smas['MA20_X_MA50'] == 1],
    #          '^', markersize=15, color='c', alpha=0.7, label='buy')
    #
    # # plot 'buy' signals
    # plt.plot(df_day_with_smas[df_day_with_smas['MA5_X_MA20'] == -1].index,
    #          df_day_with_smas['SMA_5'][df_day_with_smas['MA5_X_MA20'] == -1],
    #          'v', markersize=15, color='r', alpha=0.7, label='sell')
    #
    # plt.legend()
    # plt.grid()
    # plt.show()
    # # -------------
    #
    # d = dellphic(df_day)
    #
    # print(d)
    # del s


if __name__ == "__main__":
    main()
