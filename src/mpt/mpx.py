import os
from argparse import ArgumentParser

from dotenv import load_dotenv

from mpt.mpt import calc_correlation, optimize_profit
from util import is_women, split

load_dotenv()
PYTHON_ENVIRONMENT = os.getenv('PYTHON_ENVIRONMENT')


async def mpx(ctx, *args, **kwargs):
    parser = ArgumentParser()
    parser.add_argument("-d", "--days", type=int, dest="days", help="number of days", default=120)
    parser.add_argument("-i", "--interest", type=float, dest="interest_rate", help="Bank interest rate", default=0.7)
    parser.add_argument('symbols', metavar='HPG', type=str, nargs='+', help='your stock symbols')
    args = parser.parse_args(args)

    symbols = args.symbols
    if len(args.symbols) == 1 and ',' in args.symbols[0]:
        symbols = split(args.symbols)  # args[0].replace(' ', '').split(',')

    symbols = [x.upper() for x in symbols]
    if PYTHON_ENVIRONMENT == 'production':
        ac = 'chị' if is_women(ctx.message.author.roles) else 'anh'
        await ctx.send('```Người {} em {} đang kiểm tra {} mã : {}```'.format(ac, ctx.message.author, len(symbols), ', '.join(symbols)), delete_after=180.0)
        symbols, mean, corr = calc_correlation(symbols, args.days)
        result = optimize_profit(symbols, mean, corr, bank_interest_rate=args.interest_rate)
        await ctx.send('```%s```' % result.to_string(), delete_after=180.0)
        await ctx.message.delete()
    else:
        ac = 'chị' if is_women(ctx.message.author.roles) else 'anh'
        msg = '`Người {} em #{} đang kiểm tra {}` mã : `{}`'.format(ac, ctx.message.author, len(symbols), ', '.join(symbols))
        print(msg)
        print('Symbols', symbols)
        symbols, mean, corr = calc_correlation(symbols, args.days)
        print('Symbols', symbols)
        result = optimize_profit(symbols, mean, corr)
        print(ctx.message.author.roles)
        print(result.to_string())


async def mpx_info(ctx, *args):
    with(open('./mpt/mpt.txt', 'rt')) as f:
        msg = f.read()
        if PYTHON_ENVIRONMENT == 'production':
            await ctx.send('```%s```' % msg, delete_after=300.0)
            await ctx.message.delete()
        else:
            print(msg)
