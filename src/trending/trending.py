import pandas as pd

from db.database import get_connection, close_connection
from stock.stock import Stock
from util import resample_trending_interval


async def trendingX(ctx, *args):
    conn, cursor = get_connection()
    symbol = args[0].upper() if (len(args) > 0) else ''

    print('%s' % ctx.invoked_subcommand)
    if symbol in ctx.company_full_list:
        window = float(args[1]) if (len(args) > 1) else 7.0  # default 7hour
        limit = int(args[1]) if (len(args) > 2) else 10  # default limit 10 top
        window_str = "'{} days'".format(window) if window > 1 else "'{} day'".format(window)
        query_string = '''select mentioned_at as date, symbol, 1 as count from tbl_mentions where symbol=\'''' + symbol + '''\' and mentioned_at > (now() - interval ''' + window_str + ''') order by date desc'''

        print(query_string)
        sql_query = pd.read_sql_query(query_string, conn)
        trending_list = pd.DataFrame(sql_query)
        close_connection(conn)
        # Re-sample
        x_trend = resample_trending_interval(trending_list, 1440)
        x_trend = x_trend.reindex(index=x_trend.index[::-1])
        x_trend.reset_index(inplace=True, drop=True)
        r_count = x_trend['count'][::-1]
        x_trend['changed'] = r_count.pct_change()
        x_trend['changed'] = pd.Series(["{:.0f}%".format(val * 100) for val in x_trend['changed']], index=x_trend.index)
        x_trend = x_trend.head(int(window))

        await __send_message2(ctx, symbol, window, x_trend.to_string())
        # await ctx.send('%s in trend last %s days ```%s```' % (symbol, window, x_trend.to_string()), delete_after=300.0)
        await ctx.message.delete()

    else:
        window = float(args[0]) if (len(args) > 0) else 24.0  # default 24hour
        limit = int(args[1]) if (len(args) > 1) else 10  # default limit 10 top
        window_str = "'{} hours'".format(window) if window > 1 else "'{} hour'".format(window)
        query_string = '''select symbol, count(symbol) as count from tbl_mentions where mentioned_at > (now() - interval ''' + window_str + ''') group by symbol order by count desc limit ''' + str(
            limit*2)

        print(query_string)
        sql_query = pd.read_sql_query(query_string, conn)
        trending_list = pd.DataFrame(sql_query)
        trending_list[['price', 'price changed', 'volume', 'volume changed']] = trending_list['symbol'].apply(lambda x: get_stock_vpa(x))
        trending_listX = trending_list.loc[(trending_list['price'] > 0.0) & (trending_list['volume'] > 0.0)].head(limit)
        trending_listX.reset_index(inplace=True, drop=True)
        close_connection(conn)

        await __send_message1(ctx, limit, window, trending_listX.to_string())
        await ctx.message.delete()


async def __send_message2(ctx, symbol, window, xtrend):
    with open('./trending/trending2.txt') as f:
        msg = f.read()
        await ctx.send(msg.format(symbol, window, xtrend), delete_after=180.0)


async def __send_message1(ctx, limit, window, trending_list):
    with open('./trending/trending1.txt') as f:
        msg = f.read()
        await ctx.send(msg.format(limit, window, trending_list), delete_after=180.0)


def get_stock_vpa(s):
    x = Stock(s)
    x.load_price_board_minute(length=250)
    x.re_sample_h()
    df = x.df_day.copy()
    df['price changed'] = df['close'].pct_change()
    df['volume changed'] = df['volume'].pct_change()
    if len(df) > 0:
        xf = df.iloc[-1][['close', 'price changed', 'volume', 'volume changed']]
        return pd.Series({'close': xf['close'], 'price changed': '{:.0%}'.format(xf['price changed']), 'volume': xf['volume'], 'volume changed': '{:.0%}'.format(xf['volume changed'])})
    else:
        return pd.Series({'close': 0.0, 'price changed': '{:.0%}'.format(0.0), 'volume changed': '{:.0%}'.format(0.0), 'volume': 0.0})

