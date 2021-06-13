import numpy as np
import pandas as pd
import talib


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

        dataframe['MA520'] = np.where(dataframe['SMA_5'] > dataframe['SMA_20'], 1, 0)
        dataframe['MA2050'] = np.where(dataframe['SMA_20'] > dataframe['SMA_50'], 1, 0)
        dataframe['MA550'] = np.where(dataframe['SMA_5'] > dataframe['SMA_50'], 1, 0)

        DK5 = dataframe['MA2050'].rolling(window=11).apply(lambda xdf: (xdf.head(10) == 1).all())  # --> MA20 trên MA50 trong 10 phiên trước
        DK5x = dataframe['MA550'].rolling(window=11).apply(lambda xdf: (xdf.head(10) == 1).all())  # --> MA5 trên MA50 trong 10 phiên trước

        # DK5 = dataframe['MA520'].shift().tail(10).all()
        # Check if SMA5 cross SMA20
        # dataframe['X_CROSS'] = np.where(dataframe['SMA_5'] > dataframe['SMA_20'], 1, 0)
        DK6 = dataframe['MA520'].diff() == 1  # --> MA5 cat len MA20
        # DK7 = dataframe['MA2050'].tail(1).all()
        # DK7 = dataframe['volume'] > 150000
        return DK1 & DK2 & DK3 & DK4 & DK5 & DK5x & DK6
    else:
        return pd.Series([False])

