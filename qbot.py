# This example requires the 'members' privileged intents
import os
from datetime import datetime, timedelta
from os import path

import discord
import pymysql
import pandas as pd
from discord.ext import commands, tasks
from dotenv import load_dotenv
import queue
import multiprocessing as mp
import aiocron
from itertools import repeat
from mpt import calc_correlation, optimize_profit
from stock import Stock
from util import volume_break_load_cached, volume_break_save_cached

description = '''An example bot to showcase the discord.ext.commands extension
module.

There are a number of utility commands being showcased here.'''

intents = discord.Intents.default()
intents.members = True

bot = commands.Bot(command_prefix='?', description=description, intents=intents)

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
CHANNEL_ID = os.getenv('DISCORD_CHANNEL')
PYTHON_ENVIRONMENT = os.getenv('PYTHON_ENVIRONMENT')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

bot.channel_list = [815900646419071000, 818029515028168714]
bot.message_to_delete = queue.Queue()
bot.stock_objects = {}

bot.allowed_commands = [
    '?MTP', '?MPT',
    '?INFO',
    '?TICKER',
    '?AMARK',
    '?GTLT',
    '?VB'
]
bot.good_code = []
bot.company_list = []
bot.company_list_all = []


def get_connection():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    cursor = conn.cursor()
    return conn, cursor


def close_connection(conn):
    if conn:
        conn.close()


def reload_company_list():
    # Update company list
    conn, cursor = get_connection()
    sql_query = pd.read_sql_query('''select distinct code from tbl_price_board_day where v > 150000 and t > (unix_timestamp() - (86400 * 7))''', conn)
    bot.company_list = list(pd.DataFrame(sql_query)['code'])
    close_connection(conn)


def gtlt_worker(code, window):
    try:
        s = Stock(code=code)
        if s.price_increase(window=window):
            return code
        del s
    except Exception as ex:
        print('Exception', ex)


def volume_break_worker(code, window, breakout):
    try:
        s = Stock(code=code)
        if s.volume_break(window=window, breakout=breakout):
            # return code
            return [
                code,
                s.f_total_vol(),
                s.f_get_current_price(),
            ]
        del s
    except Exception as ex:
        print('Exception', ex)


def dellphic_worker(code, timeframe='d1'):
    try:
        s = Stock(code=code)
        s.calculate_indicators()
        if s.dellphic(timeframe=timeframe).iloc[-1]:
            return code
        del s
    except Exception as ex:
        print('Exception', ex)


def stock_worker(code):
    try:
        s = Stock(code=code)
        if s.f_check_7_conditions() and s.f_total_vol() > 100000:
            return [
                s.LAST_SESSION,
                code,
                s.f_total_vol(),
                s.EPS,
                s.EPS_MEAN4,
                s.f_get_current_price(),
                s.f_last_changed() * 100,
            ]

        # s.consensus_day.evaluate_ichimoku()
        # s.evaluate_ichimoku5()
        # buy_agreement = s_score['buy_agreement'].iloc[-1]
        # buy_disagreement = s_score['buy_disagreement'].iloc[-1]
        # if buy_agreement > buy_disagreement:
        #     print("BUY BUY BUY %s" % code)
        #     return [
        #         s.LAST_SESSION,
        #         code,
        #         s.f_total_vol(),
        #         s.EPS,
        #         s.EPS_MEAN4,
        #         s.f_get_current_price(),
        #         s.f_last_changed() * 100,
        #         buy_agreement,
        #         buy_disagreement
        #     ]
        #  check uptrend & downtrend
        # if s.uptrend_ichi() > 0:
        #     print("BUY BUY BUY %s" % code)
        #     return [
        #         s.LAST_SESSION,
        #         code,
        #         s.f_total_vol(),
        #         s.EPS,
        #         s.EPS_MEAN4,
        #         s.f_get_current_price(),
        #         s.f_last_changed() * 100,
        #         s.uptrend_ichi()
        #     ]

        del s  # dispose s
    except Exception as ex:
        print('Exception', ex)


def insert_mentioned_code(matches, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message):
    if len(matches) > 0:
        conn, cursor = get_connection()
        sql_string = '''insert into tbl_mentions(symbol, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message) VALUES (%s, %s, %s, %s, %s, %s)'''
        for x in matches:
            try:
                cursor.execute(sql_string, (x, mentioned_at, mentioned_by_id, mentioned_by, mentioned_in, message))
                conn.commit()
            except:
                print('[ERROR] Something went wrong')
        close_connection(conn)


@bot.event
async def on_ready():
    print('Logged in as')
    print(bot.user.name)
    print(bot.user.id)
    print('------')
    reload_company_list()
    conn, cursor = get_connection()
    sql_query = pd.read_sql_query('''select * from tbl_company where Exchange='HOSE' or Exchange='HNX' or Exchange='Upcom' order by Code ASC''', conn)
    bot.company_list_all = list(pd.DataFrame(sql_query)['Code'])
    close_connection(conn)

    # if PYTHON_ENVIRONMENT == 'development':
    #     # sql_query = pd.read_sql_query('''select * from tbl_company where Exchange='HOSE' or Exchange='HNX' or Exchange='Upcom' order by Code ASC limit 20''', bot.conn)
    #     sql_query = pd.read_sql_query('''select distinct code from tbl_price_board_day where v > 150000 and t > (unix_timestamp() - (86400 * 7)) limit 20''', bot.conn)
    #     bot.company_list = list(pd.DataFrame(sql_query)['code'])
    #     bot.conn.close()
    # else:
    #     sql_query = pd.read_sql_query('''select distinct code from tbl_price_board_day where v > 150000 and t > (unix_timestamp() - (86400 * 7))''', bot.conn)
    #     bot.company_list = list(pd.DataFrame(sql_query)['code'])
    #     bot.conn.close()


# At 16:00 on every day-of-week from Monday through Friday.
@aiocron.crontab('0 16 * * 1-5')
async def dellphic_daily():
    if PYTHON_ENVIRONMENT == 'development':
        bot.default_channel = bot.get_channel(815900646419071000)
    else:
        bot.default_channel = bot.get_channel(818029515028168714)
    # Reload company list
    reload_company_list()

    print('... dellphic')
    p = mp.Pool(5)
    good_codes = [x for x in p.starmap(dellphic_worker, zip(bot.company_list, repeat('d1'))) if x is not None]
    p.close()
    p.join()

    if len(good_codes) > 0:
        await bot.default_channel.send('Dellphic: ```%s```' % good_codes)


# At minute 1 past every hour from 9 through 15 on every day-of-week from Monday through Friday.
@aiocron.crontab('30 2-8 * * 1-5')
async def dellphic_hourly():
    if PYTHON_ENVIRONMENT == 'development':
        bot.default_channel = bot.get_channel(815900646419071000)
    else:
        bot.default_channel = bot.get_channel(818029515028168714)

    print('... dellphic')
    p = mp.Pool(5)
    good_codes = [x for x in p.starmap(dellphic_worker, zip(bot.company_list, repeat('h1'))) if x is not None]
    p.close()
    p.join()

    if len(good_codes) > 0:
        await bot.default_channel.send('Dellphic: ```%s```' % good_codes)


# @tasks.loop(minutes=1)
# async def refresh_data():
#     print('refresh data')
#     bot.stock_objects.clear()
#     num_cores = mp.cpu_count()
#     p = mp.Pool(num_cores)
#
#     good_codes = [x for x in p.starmap(dellphic_worker, zip(bot.company_list, repeat('d1'))) if x is not None]
#     p.apply_async()
#     p.close()
#     p.join()
#
#
#     bot.stock_objects = {x: Stock(x) for x in bot.company_list}
#     print('Total Stocks loaded %s' % len(bot.stock_objects))


@tasks.loop(seconds=15)
async def slow():
    if PYTHON_ENVIRONMENT == 'development':
        print('slowing ...', bot.message_to_delete.qsize())

    msg = bot.message_to_delete.get() if bot.message_to_delete.qsize() > 0 else None
    if msg is not None:
        # print('slowing ...', msg.created_at, datetime.utcnow() + timedelta(minutes=-3))
        isOld = (msg.created_at < (datetime.utcnow() - timedelta(minutes=3)))
        if isOld:
            # Delete
            await msg.delete()
        else:
            # put back for wait
            bot.message_to_delete.put(msg)


# @bot.command()
# async def add(ctx, left: int, right: int):
#     """Adds two numbers together."""
#     await ctx.send(left + right)
#
#
# @bot.command()
# async def roll(ctx, dice: str):
#     """Rolls a dice in NdN format."""
#     try:
#         rolls, limit = map(int, dice.split('d'))
#     except Exception:
#         await ctx.send('Format has to be in NdN!')
#         return
#
#     result = ', '.join(str(random.randint(1, limit)) for r in range(rolls))
#     await ctx.send(result)
#
#
# @bot.command(description='For when you wanna settle the score some other way')
# async def choose(ctx, *choices: str):
#     """Chooses between multiple choices."""
#     await ctx.send(random.choice(choices))
#
#
# @bot.command()
# async def repeat(ctx, times: int, content='repeating...'):
#     """Repeats a message multiple times."""
#     for i in range(times):
#         await ctx.send(content)
#
#
# @bot.command()
# async def joined(ctx, member: discord.Member):
#     """Says when a member joined."""
#     await ctx.send(f'{member.name} joined in {member.joined_at}')
#
#
# @bot.group()
# async def cool(ctx):
#     """Says if a user is cool.
#
#     In reality this just checks if a subcommand is being invoked.
#     """
#     if ctx.invoked_subcommand is None:
#         await ctx.send(f'No, {ctx.subcommand_passed} is not cool')
#
#
# @cool.command(name='bot')
# async def _bot(ctx):
#     """Is the bot cool?"""
#     await ctx.send('Yes, the bot is cool.')


@bot.event
async def on_message(message):
    # Not process if message is from the bot
    if message.author.id == bot.user.id:
        return

    if PYTHON_ENVIRONMENT == 'development':
        print(message.channel.id, message.author.id, message.author, message.content)

    # Only allow run bot in bot.channel_list
    msg = message.content.upper()
    ctx = message.channel

    # Process message and insert to db for STAT here

    matches = [x for x in bot.company_list_all if x in msg]
    if len(matches) > 0:
        # Insert into database
        print('### MATCH %s' % matches)
        insert_mentioned_code(matches, message.created_at, message.author.id, message.author, 'discord', message.content)
    else:
        print('### NO MATCHES')

    if msg.startswith('?'):
        if message.channel.id not in bot.channel_list:
            xh = "```Chỉ chạy bot trong phòng 'gọi-bot'!```"
            if PYTHON_ENVIRONMENT == 'production':
                await message.channel.send(xh, delete_after=10.0)
            else:
                print(xh)
        elif message.channel.id in bot.channel_list:
            cmd = msg.split(' ')[:1][0]
            if cmd in bot.allowed_commands:
                #     symbols = msg.split(' ')[1:]
                #     await ctx.send('`{}` mã : `{}`'.format(len(symbols), ', '.join(symbols)))
                #     symbols, mean, corr = calc_correlation(symbols, 120)
                #     result = optimize_profit(symbols, mean, corr)
                #     await ctx.send(result.to_string())
                await bot.process_commands(message)
            else:
                xh = "Hãy dùng cú pháp: ```?mtp mã_ck1 mã_ck2 ....```"
                if PYTHON_ENVIRONMENT == 'production':
                    await ctx.send(xh, delete_after=10.0)
                else:
                    print(xh)
    elif msg.endswith('X3') or msg.endswith('X3.') or msg.endswith('.X3') or msg.endswith(':X'):
        bot.message_to_delete.put(message)


@bot.group()
async def ticker(ctx, *args):
    await ctx.send(bot.company_list[:100])


# Gia tang lien tuc x phien
@bot.group()
async def gtlt(ctx, *args):
    p = mp.Pool(5)
    window = int(args[0]) if (len(args) > 0) else 5
    codes = [x for x in p.starmap(gtlt_worker, zip(bot.company_list, repeat(window))) if x is not None]
    p.close()
    p.join()
    await ctx.send(codes)


@bot.group()
async def vb(ctx, *args):
    w = volume_break_load_cached()
    if w is not None:

        if len(w) > 1900:
            await ctx.send('Danh sách khối lượng tăng đột biến đáng chú ý.\n ```%s```' % w[:1900])
            await ctx.send('```%s```' % w[1900:])
        else:
            await ctx.send('Danh sách khối lượng tăng đột biến đáng chú ý.\n ```%s```' % w)
    else:
        p = mp.Pool(mp.cpu_count())
        window = int(args[0]) if (len(args) > 0) else 20
        breakout = int(args[1]) if (len(args) > 1) else 50
        codes = [x for x in p.starmap(volume_break_worker, zip(bot.company_list, repeat(window), repeat(breakout))) if x is not None]
        p.close()
        p.join()

        watchlist = pd.DataFrame(codes, columns=['Code', 'Vol', 'Price'])
        # save to cached
        w = watchlist.to_string()
        volume_break_save_cached(w)
        if len(w) > 1900:
            await ctx.send('Danh sách khối lượng tăng đột biến đáng chú ý.\n ```%s```' % w[:1900])
            await ctx.send('```%s```' % w[1900:])
        else:
            await ctx.send('Danh sách khối lượng tăng đột biến đáng chú ý.\n ```%s```' % w)


@bot.group()
async def amark(ctx, *args):
    if not path.exists('outputs/amark.xlsx'):
        p = mp.Pool(5)
        buy_rows = [x for x in p.map(stock_worker, bot.company_list) if x is not None]
        p.close()
        p.join()

        results_buy = pd.DataFrame(buy_rows, columns=["Session", "Code", "Volume", "EPS", "EPS_MEAN4", 'Price', 'Changed'])
        results_buy.to_excel("outputs/amark.xlsx")
    cached = datetime.now().strftime("%b%d")

    await ctx.send(file=discord.File('outputs/amark.xlsx'))


@bot.group()
async def info(ctx, *args):
    print(args)
    msg = ''' Nguyên tắc tối ưu hóa là tối đa lợi nhuận kỳ vọng dựa trên cùng một mức độ rủi ro kỳ vọng như nhau. Tuy nhiên, tôi lưu ý rằng: (i) danh mục tối ưu giả định bạn muốn nắm giữ cổ phiếu bạn chọn trên cở sở lâu dài. Tỷ lệ % chính là sự phân bổ số tiền nhiều/ít cho các cổ phiếu bạn chọn; (ii) định kỳ bạn có thể kiểm tra xem tỷ lệ % thay đổi thế nào để hiểu là nên rebalancing thế nào (mua thêm/bán bớt như các quỹ đầu tư vẫn làm); (iii) nếu dùng ngắn hạn thì tối ưu hóa kiểu này phù hợp nhất với kiểu giao dịch theo xu hướng (momentum) là kiểu giao dịch khá phổ biến. Và đặc biệt là (iv) theo kinh nghiệm của tôi cách dùng hữu ích nhất chính là nếu bạn đang phân vân giữa các cổ phiếu cùng nhóm ngành, cùng kiểu (ví dụ đánh đấm) thì dùng máy này sẽ gợi ý bạn chọn cổ phiếu tốt hơn trong số đó. Dữ liệu dùng để tính toán hiện tại là giá 120 ngày gần nhất và giả định lãi suất ngân hàng 12 tháng 7%. Giai đoạn này các bạn tham khảo là chính. Sau đó, cái máy này sẽ cần phải nâng cấp để nó phân biệt được 3 giai đoạn tăng/giảm/sideways để từ đó mới có gợi ích phân bổ tốt hơn'''
    if PYTHON_ENVIRONMENT == 'production':
        await ctx.send('```%s```' % msg)
    else:
        print(msg)


@bot.group()
async def mtp(ctx, *args):
    symbols = args
    if len(args) == 1 and ',' in args[0]:
        symbols = args[0].replace(' ', '').split(',')

    if PYTHON_ENVIRONMENT == 'production':
        await ctx.send('`User #{} đang kiểm tra {}` mã : `{}`'.format(ctx.message.author, len(symbols), ', '.join(symbols)), delete_after=180.0)
        symbols, mean, corr = calc_correlation(symbols, 120)
        result = optimize_profit(symbols, mean, corr)
        await ctx.send(result.to_string(), delete_after=180.0)
        await ctx.message.delete()
    else:
        msg = '`User #{} đang kiểm tra {}` mã : `{}`'.format(ctx.message.author, len(symbols), ', '.join(symbols))
        print(msg)
        # await ctx.send('`User #{} đang kiểm tra {}` mã : `{}`'.format(ctx.message.author, len(symbols), ', '.join(symbols)), delete_after=180.0)
        # symbols, mean, corr = calc_correlation(symbols, 120)
        # result = optimize_profit(symbols, mean, corr)
        # await ctx.send(result.to_string(), delete_after=180.0)
        # await ctx.message.delete()


slow.start()
# refresh_data.start()
bot.run(TOKEN)
