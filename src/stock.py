import pandas as pd
import numpy as np
import talib

from ichimoku5 import ichimoku5
from util import get_connection


class Stock:
    df_minute = None
    df_h1 = None
    df_h4 = None
    df_day = None
    df_w1 = None
    df_finance = None

    def __init__(self, code, resolution='D', update_last=False, length=365):
        self.code = code
        self.length = length
        self.resolution = resolution  # main resolution
        self.conn, cursor = get_connection()
        self.init()

    def init(self):
        # Load finance info
        self.__load_finance_info()
        # Load price board at $D
        self.__load_price_board_day(length=self.length)
        # Load price board at $M
        self.__load_price_board_minute(length=(5 * self.length))

        # reverse
        self.df_day = self.df_day.reindex(index=self.df_day.index[::-1])
        self.df_day = self.df_day.drop_duplicates(subset='date', keep="first")

        self.df_minute = self.df_minute.reindex(index=self.df_minute.index[::-1])

        # re-sample
        self.df_h1 = resample_to_interval(self.df_minute, interval=60)
        self.df_h4 = resample_to_interval(self.df_minute, interval=240)
        self.df_w1 = resample_to_interval(self.df_day, interval=10080)

    def __load_finance_info(self):
        # Load finance info
        try:
            sql_finance_info = """select * from tbl_finance_info as ti where ti.code='""" + self.code + """' order by year_period desc, quarter_period desc limit 4"""
            finance_info_data = pd.read_sql_query(sql_finance_info, self.conn)
            self.df_finance = pd.DataFrame(finance_info_data)  # data-frame finance information
        except pd.io.sql.DatabaseError as ex:
            print("No finance information")

    # Load 1000 days = 3 years
    def __load_price_board_day(self, length=1000):
        try:
            sql_resolution_d = """select code, t as date, o as open, h as high, l as low, c as close, v as volume  from tbl_price_board_day as pb where pb.code='""" + self.code + """' order by t desc limit """ + str(
                length)
            self.df_day = pd.DataFrame(pd.read_sql_query(sql_resolution_d, self.conn))
            self.df_day['date'] = pd.to_datetime(self.df_day['date'], unit='s')
        except pd.io.sql.DatabaseError as ex:
            print("Something went wrong", ex)

    # Load 6k minute = 100 hours
    def __load_price_board_minute(self, length=6000):
        try:
            sql_resolution_m = """select code, t as date, o as open, h as high, l as low, c as close, v as volume from tbl_price_board_minute as pb where pb.code='""" + self.code + """' order by t desc limit """ + str(
                length)
            self.df_minute = pd.DataFrame(pd.read_sql_query(sql_resolution_m, self.conn))
            self.df_minute['date'] = pd.to_datetime(self.df_minute['date'], unit='s')
        except pd.io.sql.DatabaseError as ex:
            print("Something went wrong")

    def calculate_finance_info(self):
        if not self.df_finance.empty:
            # finance info #EPS means()
            self.EPS = self.df_finance['eps'].iloc[0]
            self.EPS_MEAN4 = self.df_finance['eps'].mean()
            rev_df_fi = self.df_finance['eps'][::-1]
            self.df_finance['eps_changed'] = rev_df_fi.pct_change()

            self.BVPS = self.df_finance['bvps'].iloc[0]
            self.BVPS_MEAN4 = self.df_finance['bvps'].mean()
            self.PE = self.df_finance['pe'].iloc[0]
            self.PE_MEAN4 = self.df_finance['pe'].mean()
            self.ROS = self.df_finance['ros'].iloc[0]
            self.ROS_MEAN4 = self.df_finance['ros'].mean()
            self.ROEA = self.df_finance['roea'].iloc[0]
            self.ROEA_MEAN4 = self.df_finance['roea'].mean()
            self.ROAA = self.df_finance['roaa'].iloc[0]
            self.ROAA_MEAN4 = self.df_finance['roaa'].mean()
            self.CURRENT_ASSETS = self.df_finance['current_assets'].iloc[0]
            self.CURRENT_ASSETS_MEAN4 = self.df_finance['current_assets'].mean()
            self.TOTAL_ASSETS = self.df_finance['total_assets'].iloc[0]
            self.TOTAL_ASSETS_MEAN4 = self.df_finance['total_assets'].mean()
            self.LIABILITIES = self.df_finance['liabilities'].iloc[0]
            self.LIABILITIES_MEAN4 = self.df_finance['liabilities'].mean()
            self.SHORT_LIABILITIES = self.df_finance['short_term_liabilities'].iloc[0]
            self.SHORT_LIABILITIES_MEAN4 = self.df_finance['short_term_liabilities'].mean()
            self.OWNER_EQUITY = self.df_finance['owner_equity'].iloc[0]
            self.OWNER_EQUITY_MEAN4 = self.df_finance['owner_equity'].mean()
            self.MINORITY_INTEREST = self.df_finance['minority_interest'].iloc[0]
            self.MINORITY_INTEREST_MEAN4 = self.df_finance['minority_interest'].mean()
            self.NET_REVENUE = self.df_finance['net_revenue'].iloc[0]
            self.NET_REVENUE_MEAN4 = self.df_finance['net_revenue'].mean()
            self.GROSS_PROFIT = self.df_finance['gross_profit'].iloc[0]
            self.GROSS_PROFIT_MEAN4 = self.df_finance['gross_profit'].mean()
            self.OPERATING_PROFIT = self.df_finance['operating_profit'].iloc[0]
            self.OPERATING_PROFIT_MEAN4 = self.df_finance['operating_profit'].mean()
            self.PROFIT_AFTER_TAX = self.df_finance['profit_after_tax'].iloc[0]
            self.PROFIT_AFTER_TAX_MEAN4 = self.df_finance['profit_after_tax'].mean()
            self.NET_PROFIT = self.df_finance['net_profit'].iloc[0]
            self.NET_PROFIT_MEAN4 = self.df_finance['net_profit'].mean()
        else:
            # finance info #EPS means()
            self.EPS = 0
            self.EPS_MEAN4 = 0
            rev_df_fi = self.df_finance['eps'][::-1]
            self.df_finance['eps_changed'] = rev_df_fi.pct_change()

            # print(self.df_fi['eps'])
            # print(self.df_fi['eps_changed'].mean())

            self.BVPS = 0
            self.BVPS_MEAN4 = 0
            self.PE = 0
            self.PE_MEAN4 = 0
            self.ROS = 0
            self.ROS_MEAN4 = 0
            self.ROEA = 0
            self.ROEA_MEAN4 = 0
            self.ROAA = 0
            self.ROAA_MEAN4 = 0
            self.CURRENT_ASSETS = 0
            self.CURRENT_ASSETS_MEAN4 = 0
            self.TOTAL_ASSETS = 0
            self.TOTAL_ASSETS_MEAN4 = 0
            self.LIABILITIES = 0
            self.LIABILITIES_MEAN4 = 0
            self.SHORT_LIABILITIES = 0
            self.SHORT_LIABILITIES_MEAN4 = 0
            self.OWNER_EQUITY = 0
            self.OWNER_EQUITY_MEAN4 = 0
            self.MINORITY_INTEREST = 0
            self.MINORITY_INTEREST_MEAN4 = 0
            self.NET_REVENUE = 0
            self.NET_REVENUE_MEAN4 = 0
            self.GROSS_PROFIT = 0
            self.GROSS_PROFIT_MEAN4 = 0
            self.OPERATING_PROFIT = 0
            self.OPERATING_PROFIT_MEAN4 = 0
            self.PROFIT_AFTER_TAX = 0
            self.PROFIT_AFTER_TAX_MEAN4 = 0
            self.NET_PROFIT = 0
            self.NET_PROFIT_MEAN4 = 0

    def calculate_indicators(self):
        if not self.df_h1.empty:
            self.df_h1['SMA_5'] = talib.SMA(self.df_h1['close'], timeperiod=5)
            self.df_h1['SMA_10'] = talib.SMA(self.df_h1['close'], timeperiod=10)
            self.df_h1['SMA_20'] = talib.SMA(self.df_h1['close'], timeperiod=20)
            self.df_h1['SMA_50'] = talib.SMA(self.df_h1['close'], timeperiod=50)
            self.df_h1['SMA_150'] = talib.SMA(self.df_h1['close'], timeperiod=150)
            self.df_h1['SMA_200'] = talib.SMA(self.df_h1['close'], timeperiod=200)

        # print('Price List', self.df)
        if not self.df_day.empty:
            self.LAST_SESSION = self.df_day['date'].iloc[-1]
            self.df_day['changed'] = self.df_day['close'].pct_change()
            self.df_day['rsi'] = talib.RSI(self.df_day['close'])
            self.df_day['cci'] = talib.CCI(self.df_day['high'], self.df_day['low'], self.df_day['close'], timeperiod=20)

            self.df_day['macd'], self.df_day['macdsignal'], self.df_day['macdhist'] = talib.MACD(self.df_day['close'], fastperiod=12,
                                                                                                 slowperiod=26, signalperiod=9)

            self.CURRENT_CLOSE = self.df_day['close'].iloc[-1]
            self.df_day['SMA_5'] = talib.SMA(self.df_day['close'], timeperiod=5)
            self.df_day['SMA_10'] = talib.SMA(self.df_day['close'], timeperiod=10)
            self.df_day['SMA_20'] = talib.SMA(self.df_day['close'], timeperiod=20)
            self.df_day['SMA_50'] = talib.SMA(self.df_day['close'], timeperiod=50)
            self.df_day['SMA_150'] = talib.SMA(self.df_day['close'], timeperiod=150)
            self.df_day['SMA_200'] = talib.SMA(self.df_day['close'], timeperiod=200)
            # Volume SMA 20
            self.df_day['V_SMA_20'] = talib.SMA(self.df_day['volume'], timeperiod=20)
            self.LAST_V_SMA_20 = self.df_day['V_SMA_20'].iloc[-1]

            self.CDL2CROWS = talib.CDL2CROWS(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDL3BLACKCROWS = talib.CDL3BLACKCROWS(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDL3INSIDE = talib.CDL3INSIDE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDL3LINESTRIKE = talib.CDL3LINESTRIKE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDL3OUTSIDE = talib.CDL3OUTSIDE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDL3STARSINSOUTH = talib.CDL3STARSINSOUTH(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDL3WHITESOLDIERS = talib.CDL3WHITESOLDIERS(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLABANDONEDBABY = talib.CDLABANDONEDBABY(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDLADVANCEBLOCK = talib.CDLADVANCEBLOCK(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLBELTHOLD = talib.CDLBELTHOLD(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLBREAKAWAY = talib.CDLBREAKAWAY(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLCLOSINGMARUBOZU = talib.CDLCLOSINGMARUBOZU(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLCONCEALBABYSWALL = talib.CDLCONCEALBABYSWALL(self.df_day['open'].values, self.df_day['high'].values,
                                                                 self.df_day['low'].values, self.df_day['close'].values)
            self.CDLCOUNTERATTACK = talib.CDLCOUNTERATTACK(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDLDARKCLOUDCOVER = talib.CDLDARKCLOUDCOVER(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLDOJI = talib.CDLDOJI(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                         self.df_day['close'].values)
            self.CDLDOJISTAR = talib.CDLDOJISTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLDRAGONFLYDOJI = talib.CDLDRAGONFLYDOJI(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDLENGULFING = talib.CDLENGULFING(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLEVENINGDOJISTAR = talib.CDLEVENINGDOJISTAR(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLEVENINGSTAR = talib.CDLEVENINGSTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLGAPSIDESIDEWHITE = talib.CDLGAPSIDESIDEWHITE(self.df_day['open'].values, self.df_day['high'].values,
                                                                 self.df_day['low'].values, self.df_day['close'].values)
            self.CDLGRAVESTONEDOJI = talib.CDLGRAVESTONEDOJI(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLHAMMER = talib.CDLHAMMER(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLHANGINGMAN = talib.CDLHANGINGMAN(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                     self.df_day['close'].values)
            self.CDLHARAMI = talib.CDLHARAMI(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLHARAMICROSS = talib.CDLHARAMICROSS(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLHIGHWAVE = talib.CDLHIGHWAVE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLHIKKAKE = talib.CDLHIKKAKE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDLHIKKAKEMOD = talib.CDLHIKKAKEMOD(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                     self.df_day['close'].values)
            self.CDLHOMINGPIGEON = talib.CDLHOMINGPIGEON(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLIDENTICAL3CROWS = talib.CDLIDENTICAL3CROWS(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLINNECK = talib.CDLINNECK(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLINVERTEDHAMMER = talib.CDLINVERTEDHAMMER(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLKICKING = talib.CDLKICKING(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDLKICKINGBYLENGTH = talib.CDLKICKINGBYLENGTH(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLLADDERBOTTOM = talib.CDLLADDERBOTTOM(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLLONGLEGGEDDOJI = talib.CDLLONGLEGGEDDOJI(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLLONGLINE = talib.CDLLONGLINE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLMARUBOZU = talib.CDLMARUBOZU(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLMATCHINGLOW = talib.CDLMATCHINGLOW(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLMATHOLD = talib.CDLMATHOLD(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDLMORNINGDOJISTAR = talib.CDLMORNINGDOJISTAR(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLMORNINGSTAR = talib.CDLMORNINGSTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLONNECK = talib.CDLONNECK(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLPIERCING = talib.CDLPIERCING(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                 self.df_day['close'].values)
            self.CDLRICKSHAWMAN = talib.CDLRICKSHAWMAN(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLRISEFALL3METHODS = talib.CDLRISEFALL3METHODS(self.df_day['open'].values, self.df_day['high'].values,
                                                                 self.df_day['low'].values, self.df_day['close'].values)
            self.CDLSEPARATINGLINES = talib.CDLSEPARATINGLINES(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLSHOOTINGSTAR = talib.CDLSHOOTINGSTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLSHORTLINE = talib.CDLSHORTLINE(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLSPINNINGTOP = talib.CDLSPINNINGTOP(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                       self.df_day['close'].values)
            self.CDLSTALLEDPATTERN = talib.CDLSTALLEDPATTERN(self.df_day['open'].values, self.df_day['high'].values,
                                                             self.df_day['low'].values, self.df_day['close'].values)
            self.CDLSTICKSANDWICH = talib.CDLSTICKSANDWICH(self.df_day['open'].values, self.df_day['high'].values,
                                                           self.df_day['low'].values, self.df_day['close'].values)
            self.CDLTAKURI = talib.CDLTAKURI(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                             self.df_day['close'].values)
            self.CDLTASUKIGAP = talib.CDLTASUKIGAP(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLTHRUSTING = talib.CDLTHRUSTING(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                   self.df_day['close'].values)
            self.CDLTRISTAR = talib.CDLTRISTAR(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                               self.df_day['close'].values)
            self.CDLUNIQUE3RIVER = talib.CDLUNIQUE3RIVER(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values,
                                                         self.df_day['close'].values)
            self.CDLUPSIDEGAP2CROWS = talib.CDLUPSIDEGAP2CROWS(self.df_day['open'].values, self.df_day['high'].values,
                                                               self.df_day['low'].values, self.df_day['close'].values)
            self.CDLXSIDEGAP3METHODS = talib.CDLXSIDEGAP3METHODS(self.df_day['open'].values, self.df_day['high'].values,
                                                                 self.df_day['low'].values, self.df_day['close'].values)

            # 4 weeks
            try:
                self.SMA_200_20 = talib.SMA(self.df_day['close'], timeperiod=200).iloc[-20]
            except:
                self.SMA_200_20 = 0

            self.LOW_52W = self.df_day['low'].head(260).min()
            self.HIGH_52W = self.df_day['high'].head(260).max()

    def f_get_current_price(self):
        return self.df_day['close'].iloc[-1]

    def last_volume(self):
        return self.df_day['volume'].iloc[-1]

    # Oversold --> should buy
    def f_is_over_sold(self, horizontal=100):
        return self.cci.iloc[-1] <= (0 - horizontal)

    # Overbought --> should sell
    def f_is_over_bought(self, horizontal=100):
        return self.cci.iloc[-1] > horizontal

    def f_check_uptrend_1_month(self):
        return self.CURRENT_CLOSE > self.df_day['SMA_5'].iloc[-1] > self.df_day['SMA_10'].iloc[-1] > self.df_day['SMA_20'].iloc[-1]

    # check price jump 1%
    def f_check_price_jump(self, step=0.01):
        return self.df_day['changed'].iloc[-1] >= step

    # minervini conditions
    def f_check_7_conditions(self):
        # Condition 1: Current Price > 150 SMA and > 200 SMA
        condition_1 = self.CURRENT_CLOSE > self.df_day['SMA_150'].iloc[-1] > self.df_day['SMA_200'].iloc[-1]

        # Condition 2: 150 SMA and > 200 SMA
        condition_2 = self.df_day['SMA_150'].iloc[-1] > self.df_day['SMA_200'].iloc[-1]

        # Condition 3: 200 SMA trending up for at least 1 month
        condition_3 = self.df_day['SMA_200'].iloc[-1] > self.SMA_200_20

        # Condition 4: 50 SMA> 150 SMA and 50 SMA> 200 SMA
        condition_4 = self.df_day['SMA_50'].iloc[-1] > self.df_day['SMA_150'].iloc[-1] > self.df_day['SMA_200'].iloc[-1]

        # Condition 5: Current Price > 50 SMA
        condition_5 = self.CURRENT_CLOSE > self.df_day['SMA_50'].iloc[-1]

        # Condition 6: Current Price is at least 30% above 52 week low
        condition_6 = self.CURRENT_CLOSE >= (1.3 * self.LOW_52W)

        # Condition 7: Current Price is within 25% of 52 week high
        condition_7 = self.CURRENT_CLOSE >= (.75 * self.HIGH_52W)

        return condition_1 and \
               condition_2 and \
               condition_3 and \
               condition_4 and \
               condition_5 and \
               condition_6 and \
               condition_7

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

    def dellphic(self, timeframe='d1'):
        if timeframe == 'd1':
            df = self.df_day
        elif timeframe == 'w1':
            df = self.df_w1
        elif timeframe == 'h1':
            df = self.df_h1
        elif timeframe == 'h4':
            df = self.df_h4
        else:
            df = self.df_day

        DK1 = df['SMA_5'].rolling(window=40).min() < df['SMA_20'].rolling(window=40).min()
        DK2 = df['SMA_20'].rolling(window=40).min() < df['SMA_50'].rolling(window=40).min()
        DK3 = df['SMA_5'].rolling(window=20).max() > df['SMA_20'].rolling(window=20).max()
        DK4 = df['SMA_20'].rolling(window=20).max() > df['SMA_50'].rolling(window=20).max()
        DK5 = df['SMA_5'] > df['SMA_20']
        # Check if SMA5 cross
        sma520diff = df['SMA_5'] - df['SMA_20']
        DK6 = np.select([((sma520diff < 0) & (sma520diff.shift() > 0)), ((sma520diff > 0) & (sma520diff.shift() < 0))], [True, False], False)
        DK7 = df['volume'].iloc[-1] > 150000
        return DK1 & DK2 & DK3 & DK4 & DK5 & DK6 & DK7

    def volume_increase(self, window=5):
        compares = self.df_day['volume'] > self.df_day['volume'].shift()
        return compares.tail(window).all()

    # Khoi luong dot pha 30 so voi trung binh 20 phien truoc do
    def volume_break(self, window=20, breakout=50):
        df = self.df_day.copy()
        df['VOL_SMA20'] = talib.SMA(df['volume'], timeperiod=window)
        return (df['volume'].iloc[-1] > df['VOL_SMA20'].tail(window) * (1.0 + breakout / 100)).tail(1).all()

    # Gi?? t??ng li??n t???c window phi??n.
    def price_increase(self, window=3):
        compares = self.df_day['close'] > self.df_day['close'].shift()
        return compares.tail(window).all()

    def f_check_two_crows(self):
        try:
            res = talib.CDL2CROWS(self.df_day['open'].values, self.df_day['high'].values, self.df_day['low'].values, self.df_day['close'].values)
            return pd.DataFrame({'CDL2CROWS': res}, index=self.df_day.index)
        except:
            return pd.DataFrame({'CDL2CROWS': [-1]})

    def uptrend_ichi(self):
        # up_trends = self.df_day.filter(regex="^(uptrend)_.*").fillna(0)
        # print('dataframe', self.df_day['uptrend_ichimoku'])
        return False

    # ICHIMOKU EVALUATE
    def evaluate_ichimoku5(self, prefix="ichimoku", impact_buy=1, impact_sell=1, ratio=0.1):
        """
        evaluates the ichimoku
        :param ratio:
        :param impact_sell:
        :param impact_buy:
        :param period:
        :param prefix:
        :return:
        """
        print('...EVALUATE_ICHIMOKU5')
        # self._weights(impact_buy, impact_sell)
        # dataframe = self.df_day
        name = '{}'.format(prefix)
        ichimoku = ichimoku5(self.df_day)

        self.df_day['{}_ks9'.format(name)] = ichimoku['ks9']
        self.df_day['{}_ks17'.format(name)] = ichimoku['ks17']
        self.df_day['{}_ks26'.format(name)] = ichimoku['ks26']
        self.df_day['{}_ks65'.format(name)] = ichimoku['ks65']
        self.df_day['{}_ks129'.format(name)] = ichimoku['ks129']
        self.df_day['{}_span_a'.format(name)] = ichimoku['span_a']
        self.df_day['{}_span_b'.format(name)] = ichimoku['span_b']
        self.df_day['{}_span_1'.format(name)] = ichimoku['span_1']
        self.df_day['{}_span_2'.format(name)] = ichimoku['span_2']
        self.df_day['{}_lagging_t2'.format(name)] = ichimoku['lagging_t2']
        self.df_day['{}_lagging_t3'.format(name)] = ichimoku['lagging_t3']

        self.df_day['{}_ks_9_sl_t3'.format(name)] = ichimoku['ks_9_sl_t3']
        self.df_day['{}_ks_17_sl_t3'.format(name)] = ichimoku['ks_17_sl_t3']
        self.df_day['{}_span_2_sl_t2p1'.format(name)] = ichimoku['span_2_sl_t2p1']
        self.df_day['{}_lagging_pt2m1'.format(name)] = ichimoku['lagging_pt2m1']
        self.df_day['{}_lagging_mt2p1'.format(name)] = ichimoku['lagging_mt2p1']

        # UPTREND
        # Uptrend = (KS9 > Ref(Span1,-t3) AND KS9>Ref(Span2,-t3) AND (KS17>Ref(Span1,-t3) AND KS17>Ref(Span2,-t3))) ;
        self.df_day.loc[
            (
                    (self.df_day['{}_ks9'.format(name)] > self.df_day['{}_span_a'.format(name)]) &
                    (self.df_day['{}_ks9'.format(name)] > self.df_day['{}_span_b'.format(name)]) &
                    (self.df_day['{}_ks17'.format(name)] > self.df_day['{}_span_a'.format(name)]) &
                    (self.df_day['{}_ks17'.format(name)] > self.df_day['{}_span_b'.format(name)])

            ),
            'uptrend_{}'.format(name)
        ] = (1 * impact_buy)

        # print('uptrend %s' % self.df_day['uptrend_ichimoku'])

        # DOWNTREND
        # Downtrend=(KS9 < Ref(Span1,-t3) AND KS9<Ref(Span2,-t3) AND (KS17<Ref(Span1,-t3) AND KS17<Ref(Span2,-t3))) ;
        self.df_day.loc[
            (
                (self.df_day['{}_ks9'.format(name)] < self.df_day['{}_span_a'.format(name)])
                # (self.df_day['{}_ks9'.format(name)] < self.df_day['{}_span_b'.format(name)]) &
                # (self.df_day['{}_ks17'.format(name)] < self.df_day['{}_span_a'.format(name)]) &
                # (self.df_day['{}_ks17'.format(name)] < self.df_day['{}_span_b'.format(name)])
            ),
            'downtrend_{}'.format(name)
        ] = (1 * impact_sell)
        # print('downtrend %s' % self.df_day['downtrend_ichimoku'])
        # print('_ks9 %s' % self.df_day['{}_ks9'.format(name)])
        # print('_span_a %s' % self.df_day['{}_span_1'.format(name)])
        # print('compare %s' % (self.df_day['{}_ks9'.format(name)] < self.df_day['{}_span_a'.format(name)]))

        # print('%s' % (self.df_day['{}_span_a'.format(name)]))
        # print('%s' % (self.df_day['{}_span_a'.format(name)] < self.df_day['open']))

        # BreakOutAllKuMoCloud- Mua dai han
        # Chikou back26 > Komu va Kumosen hay chilku cat len Kumo va Kumo sen;
        # hoac Chikou 17 >  Komu va Kumosen hay chilku cat len Kumo va Kumo sen;
        # Kijun 9 huong len hoac Kijun 17 huong len;
        # Gia cong cuar lon hon Kjjun 65;
        self.df_day.loc[
            (
                    (  # CD1_UP
                            (self.df_day['{}_lagging_t3'.format(name)] <= self.df_day['{}_span_b'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] <= self.df_day['{}_ks_17_sl_t3'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] <= self.df_day['{}_span_a'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] <= self.df_day['{}_ks_9_sl_t3'.format(name)])
                    ) &
                    (
                        # CD2_UP
                            (self.df_day['{}_lagging_pt2m1'.format(name)] > self.df_day['{}_span_2_sl_t2p1'.format(name)]) &
                            (self.df_day['close'.format(name)] > self.df_day['{}_ks17'.format(name)]) &
                            (self.df_day['close'.format(name)] > self.df_day['{}_span_1'.format(name)]) &
                            (self.df_day['close'.format(name)] > self.df_day['{}_ks9'.format(name)])
                    ) &
                    (  # CD3_UP
                            (self.df_day['{}_ks9'.format(name)] >= (self.df_day['{}_ks9'.format(name)].shift(-1))) |
                            (self.df_day['{}_ks17'.format(name)] >= (self.df_day['{}_ks17'.format(name)].shift(-1)))
                    ) &
                    (  # CD4_UP
                            self.df_day['close'] > self.df_day['{}_ks65'.format(name)]
                    )
            ),
            'breakout_up_{}'.format(name)
        ] = (1 * impact_buy)

        # DOWN
        self.df_day.loc[
            (
                    (  # CD1_DOWN
                            (self.df_day['{}_lagging_t2'.format(name)] >= self.df_day['{}_span_b'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] >= self.df_day['{}_ks_17_sl_t3'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] >= self.df_day['{}_span_a'.format(name)]) |
                            (self.df_day['{}_lagging_t2'.format(name)] >= self.df_day['{}_ks_9_sl_t3'.format(name)])
                    ) &
                    (  # CD2_DOWN
                            (self.df_day['{}_lagging_pt2m1'.format(name)] < self.df_day['{}_span_2_sl_t2p1'.format(name)]) &
                            (self.df_day['close'.format(name)] < self.df_day['{}_ks17'.format(name)]) &
                            (self.df_day['close'.format(name)] < self.df_day['{}_span_1'.format(name)]) &
                            (self.df_day['close'.format(name)] < self.df_day['{}_ks9'.format(name)])

                    ) &
                    (  # CD3_DOWN
                            (self.df_day['{}_ks9'.format(name)] <= (self.df_day['{}_ks9'.format(name)].shift(-1))) |
                            (self.df_day['{}_ks17'.format(name)] <= (self.df_day['{}_ks17'.format(name)].shift(-1)))
                    ) &
                    (  # CD4_DOWN
                            self.df_day['close'] < self.df_day['{}_ks65'.format(name)]
                    )
            ),
            'breakout_down_{}'.format(name)
        ] = (1 * impact_sell)

        # STRENGTHEN UP
        self.df_day.loc[
            (
                    (((self.df_day['{}_ks9'.format(name)].shift(-1) - self.df_day['{}_ks17'.format(name)].shift(-1)) / self.df_day['{}_ks17'.format(name)].shift(
                        -1)).abs() < ratio) &
                    (((self.df_day['{}_ks9'.format(name)] - self.df_day['{}_ks17'.format(name)]) / self.df_day['{}_ks17'.format(name)]).abs() < ratio) &
                    (self.df_day['{}_ks17'.format(name)] > (self.df_day['{}_ks17'.format(name)].shift(-1))) &
                    (self.df_day['{}_ks65'.format(name)] >= (self.df_day['{}_ks65'.format(name)].shift(-1))) &
                    (self.df_day['close'] >= self.df_day['{}_ks65'.format(name)]) &
                    ((self.df_day['{}_span_1'.format(name)] - self.df_day['{}_span_2'.format(name)]) / self.df_day['{}_span_2'.format(name)]).abs() < 5 * ratio
            ),
            'boom_up_{}'.format(name)
        ] = (1 * impact_buy)

        # STRENGTHEN DOWN
        self.df_day.loc[
            (
                    (((self.df_day['{}_ks9'.format(name)].shift(-1) - self.df_day['{}_ks17'.format(name)].shift(-1)) / self.df_day['{}_ks17'.format(name)].shift(
                        -1)).abs() < ratio) &
                    (((self.df_day['{}_ks9'.format(name)] - self.df_day['{}_ks17'.format(name)]) / self.df_day['{}_ks17'.format(name)]).abs() < ratio) &
                    (self.df_day['{}_ks17'.format(name)] < (self.df_day['{}_ks17'.format(name)].shift(-1))) &
                    (self.df_day['{}_ks65'.format(name)] <= (self.df_day['{}_ks65'.format(name)].shift(-1))) &
                    (self.df_day['close'] >= self.df_day['{}_ks65'.format(name)]) &  # TODO: CHECK?
                    ((self.df_day['{}_span_1'.format(name)] - self.df_day['{}_span_2'.format(name)]) / self.df_day['{}_span_2'.format(name)]).abs() < 5 * ratio
            ),
            'boom_down_{}'.format(name)
        ] = (1 * impact_sell)

        # price is above the cloud
        self.df_day.loc[
            (
                    (self.df_day['{}_span_a'.format(name)] > self.df_day['open']) &
                    (self.df_day['{}_span_b'.format(name)] > self.df_day['open'])
            ),
            'buy_{}'.format(name)
        ] = (1 * impact_buy)

        # price is below the cloud
        self.df_day.loc[
            (
                    (self.df_day['{}_span_a'.format(name)] < self.df_day['open']) &
                    (self.df_day['{}_span_b'.format(name)] < self.df_day['open'])

            ),
            'sell_{}'.format(name)
        ] = (1 * impact_sell)
        print('___EVALUATE_ICHIMOKU5')

    # destructor
    def __del__(self):
        self.conn.close()


def resample_to_interval(dataframe: pd.DataFrame, interval):
    """
    Resamples the given dataframe to the desired interval.
    Please be aware you need to use resampled_merge to merge to another dataframe to
    avoid lookahead bias

    :param dataframe: dataframe containing close/high/low/open/volume
    :param interval: to which ticker value in minutes would you like to resample it
    :return:
    """
    df = dataframe.copy()
    # df['date'] = pd.to_datetime(df['date'], unit='s')
    df = df.set_index(pd.DatetimeIndex(df["date"]))
    ohlc_dict = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    # Resample to "left" border as dates are candle open dates
    # df = df.resample(axis=1, on='date', kind='timestamp', rule=str(interval) + "min", label="left").agg(ohlc_dict).dropna()
    df = df.resample(str(interval) + "min", kind='timestamp').agg(ohlc_dict).dropna()

    df.reset_index(inplace=True)
    # df['date'] = df['date'].apply(lambda x: pd.Timestamp(x))
    return df


# Test
if __name__ == '__main__':
    print('Hello ...')
    s = Stock('FPT')

    # print('%%%', s.price_increase(window=5))

    ohlc_dict = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    # df_h1 = s.df_minute.copy()
    # df_h1 = resample_to_interval(s.df_minute, interval=60)
    # self.df_h1 = resample_to_interval(self.df_minute, interval='60m')
    # print(s.df_minute)
    # print(df_h1)
    # print(s.dellphic(timeframe='h1'))
    print(s.volume_break())
    # print(s.price_increase(window=5))
