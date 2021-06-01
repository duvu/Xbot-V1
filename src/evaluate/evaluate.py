import pandas as pd

from evaluate.custom_parser import ThrowingArgumentParser
from stock.stock import Stock


async def evaluate_price(ctx, *args, **kwargs):
    parser = ThrowingArgumentParser(add_help=False)
    parser.add_argument("-t", "--target", type=str, dest="target", help="Cổ phiếu muốn định giá", default='FPT')
    parser.add_argument('symbols', metavar='Ticker', type=str, nargs='+', help='Nhóm cổ phiếu so sánh', default='hpg vpb ssi')
    parser.add_argument('-f')

    try:
        args = parser.parse_args(args)
        pe_min, pe_max, pb_min, pb_max = await evaluateX(args.target, args.symbols)
        await ctx.send('```Eval {} PE# {:.0f} - {:.0f} // PB# {:.0f} - {:.0f}```'.format(args.target, pe_min, pe_max, pb_min, pb_max))
    except Exception as ex:
        print('Ex', ex)
        await ctx.send('```Something went wrong. Please try again```')


def get_stock(s):
    x = Stock(s)
    x.load_finance_info(length=5)
    rtn = x.df_finance[['year_period', 'quarter_period', 'pb', 'pe']]
    del x
    return rtn


async def evaluateX(target, symbols):
    target = Stock(target)
    target.load_finance_info(length=5)
    base_group = [get_stock(ticker) for ticker in symbols]
    base_group_summary = (pd.concat(base_group).groupby(['year_period', 'quarter_period']).sum() / len(base_group)).reset_index()
    print('Base_Group', base_group_summary.to_string())

    base_group_summary.columns = ['year_period', 'quarter_period', 'pb_average', 'pe_average']
    target_with_base = pd.merge(target.df_finance, base_group_summary, how='left', on=['year_period', 'quarter_period'])
    target_with_base['pe_ratio'] = target_with_base['pe'] / target_with_base['pe_average']
    target_with_base['pb_ratio'] = target_with_base['pb'] / target_with_base['pb_average']
    pe_ratio_min = target_with_base['pe_ratio'].min()
    pe_ratio_max = target_with_base['pe_ratio'].max()
    pb_ratio_min = target_with_base['pb_ratio'].min()
    pb_ratio_max = target_with_base['pb_ratio'].max()

    latest_pe_average = target_with_base.iloc[-1]['pe_average']
    latest_pb_average = target_with_base.iloc[-1]['pb_average']
    latest_eps = target_with_base.iloc[-1]['eps']
    latest_bvps = target_with_base.iloc[-1]['bvps']

    price_pe_min = pe_ratio_min * latest_pe_average * latest_eps
    price_pe_max = pe_ratio_max * latest_pe_average * latest_eps

    price_pb_min = pb_ratio_min * latest_pb_average * latest_bvps
    price_pb_max = pb_ratio_max * latest_pb_average * latest_bvps

    return price_pe_min, price_pe_max, price_pb_min, price_pb_max
