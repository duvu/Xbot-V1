from stock.stock import Stock


def get_stock(s):
    x = Stock(s)
    x.load_finance_info()
    return x.df_finance[['code', 'year_period', 'quarter_period', 'eps', 'bvps', 'price', 'pb', 'net_revenue', 'net_profit']], x.df_finance_ctkh[['code', 'year_period', 'total_revenue', 'profit_after_tax']]
    # return x.df_finance


cp_tra_da_list = ['ASM', 'DAG', 'QBS', 'MBG', 'LMH', 'DLG', 'HAI', 'HQC', 'HVG', 'KLF', 'PVX', 'FLC']
cp_tra_da_list_data = [get_stock(x) for x in cp_tra_da_list]

for f, d in cp_tra_da_list_data:
    print(f)
    print(d)
