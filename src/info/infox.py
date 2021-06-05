import os

from dotenv import load_dotenv

from exceptions.qx_parser import QXParser
from mpt.mpx import mpx_info
from stock.stock import Stock
from util import split

load_dotenv()
PYTHON_ENVIRONMENT = os.getenv('PYTHON_ENVIRONMENT')


async def infoX(ctx, *args):
    if len(args) == 0:
        await mpx_info(ctx, *args)
    elif '-h' in args or '--help' in args:
        await helpX(ctx)
    else:
        target = args[0]
        if target is not None:
            name, exchange, industry_name, total_shares, latest_price, fin = await get_stock_info(target)
            with (open('./info/infox.txt', 'rt')) as f:
                msg = f.read()
                if PYTHON_ENVIRONMENT == 'production':
                    await ctx.send(msg.format(target.upper(), name, latest_price, exchange, industry_name, total_shares, fin.to_string(float_format='%.2f')), delete_after=180.0)
                else:
                    msgX = msg.format(target.upper(), name, latest_price, exchange, industry_name, total_shares, fin.to_string(float_format='%.2f'), delete_after=180.0)
                    print(msgX)


async def get_stock_info(target):
    target = Stock(target)
    target.load_company_info()
    target.load_finance_info(length=5)
    name = target.name
    exchange = target.exchange
    industry_name = target.industry_name
    total_shares = target.total_shares
    latest_price = target.df_minute.iloc[-1]['close']
    fin = target.df_finance[['year_period', 'quarter_period',  'eps', 'bvps', 'pe', 'pb', 'price']].tail(5)
    fin.reset_index(inplace=True, drop=True)
    fin.columns = ['Year', 'Quarter', 'EPS', 'BVPS', 'PE', 'PB', 'PRICE']
    del target
    return name, exchange, industry_name, total_shares, latest_price, fin.head(4)


async def helpX(ctx):
    with open('./info/help.txt', 'rt') as f:
        hlp = f.read()
        if PYTHON_ENVIRONMENT == 'production':
            await ctx.send('```%s```' % hlp)
        else:
            print(hlp)
