from argparse import ArgumentParser


async def evaluate_price(ctx, *args, **kwargs):
    parser = ArgumentParser()
    # parser.add_argument("-d", "--days", type=int, dest="days", help="number of days", default=120)
    # parser.add_argument("-i", "--interest", type=float, dest="interest_rate", help="Bank interest rate", default=0.7)
    # parser.add_argument('symbols', metavar='Ticker', type=str, nargs='+', help='your stock symbols')
    args = parser.parse_args(args)

    await ctx.send('In Development ... {}'.format(args))
    

