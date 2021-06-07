import pandas as pd

from db.database import get_connection, close_connection
from stock.stock import Stock

code_dot_bien = ['WSS', 'VND', 'VIX', 'AGR', 'APG', 'ART', 'ORS', 'TVB', 'PET', 'OGC', 'ASM', 'IJC', 'LHG', 'HQC', 'QCG', 'PVL', 'BCG', 'ELC', 'SGP', 'DPM', 'VHG', 'AAA', 'DGC', 'DDV', 'DAH', 'KSB', 'KSH', 'ACM', 'DRC', 'TNI', 'VEA', 'SHB', 'OCB', 'MSB', 'EVF', 'TAR', 'IDI', 'HNG', 'PVB', 'TVC', 'QNS', 'AAV', 'LSS', 'SBT', 'TTA', 'NT2', 'PPC', 'QTP', 'POW', 'OIL', 'TVN', 'VGI', 'PVC', 'SPI', 'HT1', 'VGC', 'VCS', 'JVC', 'BKG', 'VOS', 'FCN', 'NHA', 'S99', 'G36', 'PHC', 'CII', 'LCG', 'DST']


def get_stock_market_cap(s):
    x = Stock(s)
    x.load_price_board_minute(length=1)
    if len(x.df_minute):
        latest_price = x.df_minute['close'].iloc[-1]
        x.load_company_info()
        return [s, x.total_shares, latest_price, latest_price * x.total_shares]
    else:
        return [s, 0, 0, 0]


conn, cursor = get_connection()
sql_query = pd.read_sql_query('''select * from tbl_company order by code ASC''', conn)
company_full_list = list(pd.DataFrame(sql_query)['code'])
close_connection(conn)

# print(company_full_list)
# print(get_stock_market_cap('FPT'))

data = [get_stock_market_cap(x) for x in company_full_list]

df_data = pd.DataFrame(data, columns=["ticker", "total share", "Latest Price", "Market Cap"])
df_data.to_excel('./outputs/market_cap_all.xlsx')
print(df_data)
