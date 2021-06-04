# -- Tốc độ giảm giá giảm dần 3 phiên
# -- Khối lượng tăng dần nhưng chưa đột biến
import numpy as np
import pandas as pd

from db.database import get_connection, close_connection
from stock.stock import Stock

ONE_BILLION = 1000000000
good_code = []
conn, cursor = get_connection()
sql_data = pd.read_sql_query('''select distinct code from tbl_price_board_day where c < 30000 and v*c > ''' + str(ONE_BILLION) + ''' and t > (unix_timestamp() - (86400 * 7));''', conn)
company_short_list = list(pd.DataFrame(sql_data)['code'])


def khoi_luong_tang_tang_dan(x):
    s = Stock(x)
    s.load_price_board_day(length=10)
    df = s.df_day
    # vol_diff = df['volume'] - df['volume'].shift(1)
    #
    # print(vol_diff)
    # print(vol_diff/df['volume'] * 100)

    # volume_change_pct = 100* (df['volume'] - df['volume'].shift(1)) / df['volume']
    df['volume_chg_l1'] = df['volume'].pct_change()
    df['volume_chg_l2'] = df['volume_chg_l1'].pct_change()

    df['volume_chg_l1_cont'] = np.where(df['volume_chg_l1'] > 0, 1, 0)
    df['volume_chg_l2_cont'] = np.where(df['volume_chg_l2'] < -0.05, 1, 0)

    df['volume_chg_l3_cont'] = df['volume_chg_l1_cont'] & df['volume_chg_l2_cont']
    if df['volume_chg_l3_cont'].tail(2).all():
        print(df)
    return df['volume_chg_l3_cont'].tail(2).all()


for x in company_short_list:
    if khoi_luong_tang_tang_dan(x):
        good_code.append(x)

# print(df.shift(1))

# print(company_short_list)
close_connection(conn)

print(good_code)
