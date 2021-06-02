import pandas as pd

from db.database import get_connection, close_connection
from stock.stock import Stock

conn, cusor = get_connection()
sql_data = pd.read_sql_query('''select distinct code from tbl_price_board_day where c < 15000 and v*c > 1000000000 and t > (unix_timestamp() - (86400 * 7));''', conn)
company_short_list = list(pd.DataFrame(sql_data)['code'])
# company_short_list = ['VTD']

# print(company_short_list)

good_code = []


def get_stock(s):
    x = Stock(s)
    x.load_finance_info()
    return x.df_finance[['code', 'year_period', 'quarter_period', 'eps', 'bvps', 'price', 'pb', 'net_revenue', 'net_profit']], x.df_finance_ctkh[
        ['code', 'year_period', 'total_revenue', 'profit_after_tax']]
    # return x.df_finance


#
# cp_tra_da_list = ['ASM', 'DAG', 'QBS', 'MBG', 'LMH', 'DLG', 'HAI', 'HQC', 'HVG', 'KLF', 'PVX', 'FLC']
cp_tra_da_list_data = [get_stock(x) for x in company_short_list]
#
for fin, ctkh in cp_tra_da_list_data:
    if fin.iloc[-1]['year_period'] == 2021 and fin.iloc[-1]['price'] * 2 < fin.iloc[-1]['bvps'] and fin.iloc[-1]['eps'] > 0:
        # print(fin)
        good_code.append(fin.iloc[-1]['code'])

close_connection(conn)

print(good_code)
