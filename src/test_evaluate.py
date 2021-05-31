from datetime import datetime, time

from datequarter import DateQuarter

from stock.stock import Stock

# sample = DateQuarter(2020, 4)
# # print(sample.end_date() + 1)
# end_of_quarter_date = sample.end_date()
# dt = datetime.combine(end_of_quarter_date, time(23, 59, 59))
#
# print(dt.timestamp())

stock = Stock('FPT')
stock.load_finance_info()

print(stock.df_finance['pb'].to_string())
print(stock.df_finance['pe'].to_string())
