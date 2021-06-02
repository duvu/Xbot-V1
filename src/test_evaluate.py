from datetime import datetime, time

import pandas as pd
from datequarter import DateQuarter

from db.database import get_connection, close_connection
from stock.stock import Stock

# sample = DateQuarter(2020, 4)
# # print(sample.end_date() + 1)
# end_of_quarter_date = sample.end_date()
# dt = datetime.combine(end_of_quarter_date, time(23, 59, 59))
#
# print(dt.timestamp())

target = 'nlg'

base_group = ['kdh', 'agg']


# base_group = ['VPB']

def getStock(s):
    x = Stock(s)
    x.load_finance_info()
    return x.df_finance[['year_period', 'quarter_period', 'pb', 'pe']]


base_group_pd = [getStock(ticker) for ticker in base_group]


def process_x(xx):
    print('{} - {}'.format(xx.year_period, xx.quarter_period))

    pe_a = 0
    # print('DF Length', len(base_group_pd))
    loop = 0
    for df in base_group_pd:
        loop = loop + 1
        print('loop: {}, length: {} \n {}'.format(loop, len(df), df[['year_period', 'quarter_period', 'pb', 'pe']].to_string()))
        pe_a += df.loc[(df['year_period'] == xx.year_period) & (df['quarter_period'] == xx.quarter_period)]['pe'].get(0, 0)

    print(pe_a)
    return pe_a  # pe_a / base_group
    # return pe_a / len(base_group)
    # print(test.to_string())
    # print(df)
    # print(test[['year_period', 'quarter_period', 'pb', 'pe']].to_string())

    #     print(sx.df_finance)
    # pe_a += sx.df_finance.loc[sx.df_finance['year_period'] == xx.year_period]
    # x1 = sx.df_finance.loc[sx.df_finance['year_period'] == xx.year_period]
    # print(pe_a.to_string())


# stock = Stock('FPT')
# stock.load_finance_info()
#
# print(stock.df_finance[['year_period', 'quarter_period', 'pb', 'pe']].to_string())

st = Stock(target)
st.load_finance_info(length=5)
# st.df_finance['pe_ext'] =

# st.df_finance['pe_avg'] = st.df_finance[['year_period', 'quarter_period', 'pb', 'pe']].apply(lambda x1: process_x(x1), axis=1)


# print(st.df_finance)
# svpb = Stock('VPB')
# svpb.load_finance_info(length=5)


# merge_pd = pd.merge(st.df_finance, base_group_pd[0], how='left', on=['year_period', 'quarter_period'])
#
# print(merge_pd)
pd_concat = (pd.concat(base_group_pd).groupby(['year_period', 'quarter_period']).sum()/len(base_group_pd)).reset_index()
pd_concat.columns = ['year_period', 'quarter_period', 'pb_average', 'pe_average']

print(pd_concat)

merge_pd = pd.merge(st.df_finance, pd_concat, how='left', on=['year_period', 'quarter_period'])
merge_pd['pe_ratio'] = merge_pd['pe'] / merge_pd['pe_average']
merge_pd['pb_ratio'] = merge_pd['pb'] / merge_pd['pb_average']

print(merge_pd)

pe_ratio_min = merge_pd['pe_ratio'].min()
pe_ratio_max = merge_pd['pe_ratio'].max()
pb_ratio_min = merge_pd['pb_ratio'].min()
pb_ratio_max = merge_pd['pb_ratio'].max()

latest_pe_average = merge_pd.iloc[-1]['pe_average']
latest_pb_average = merge_pd.iloc[-1]['pb_average']
latest_eps = merge_pd.iloc[-1]['eps']
latest_bvps = merge_pd.iloc[-1]['bvps']

print('latest_pe_average', latest_pe_average)
print('latest_pb_average', latest_pb_average)

price_pe_min = pe_ratio_min * latest_pe_average * latest_eps
price_pe_max = pe_ratio_max * latest_pe_average * latest_eps

price_bp_min = pb_ratio_min * latest_pb_average * latest_bvps
price_pb_max = pb_ratio_max * latest_pb_average * latest_bvps

print('{} - {} // {} - {}'.format(price_pe_min, price_pe_max, price_bp_min, price_pb_max))


conn, curso = get_connection()
sql = '''select distinct code from tbl_price_board_day where v > 150000 and t > (unix_timestamp() - (86400 * 7))'''

close_connection(conn)