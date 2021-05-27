# This example requires the 'members' privileged intents
import os
from datetime import datetime, timedelta
from os import path
import re

import discord
import feedparser
import pandas as pd
from dateutil.parser import parse
from discord.ext import commands, tasks
from dotenv import load_dotenv
import queue
import multiprocessing as mp
import aiocron
from itertools import repeat
from cacheout import Cache

from mpt.mpt import calc_correlation, optimize_profit
from mpt.mpx import mpx, mpx_info
from social.trending import social_counting, insert_mentioned_code

from stock import Stock
from util import volume_break_load_cached, volume_break_save_cached, resample_trending_interval
from db.database import get_connection, close_connection

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

bot.channel_list = [815900646419071000, 818029515028168714]
bot.channel_black_list = [843790719877775380, 826636905475080293]  # room coding --> not check code.
bot.bl_words = [
    'doanh thu',
    'Doanh thu',
    'DOANH THU',
    'VNI'
]
bot.message_to_delete = queue.Queue()
bot.stock_objects = {}

f319x = Cache(maxsize=300, ttl=3 * 60 * 60)

bot.allowed_commands = [
    '?MTP', '?MPT',
    '?INFO',
    '?TICKER',
    '?AMARK',
    '?GTLT',
    '?VB',
    '?TRENDING'
]
bot.good_code = []
bot.company_list = []
bot.company_list_all = []


def reload_company_list():
    # Update company list
    conn, cursor = get_connection()
    sql_query = pd.read_sql_query('''select distinct code from tbl_price_board_day where v > 150000 and t > (unix_timestamp() - (86400 * 7))''', conn)
    bot.company_list = list(pd.DataFrame(sql_query)['code'])
    close_connection(conn)


def gtlt_worker(code, window):
    try:
        s = Stock(code=code, length=2 * window)
        if s.price_increase(window=window):
            return code
        del s
    except Exception as ex:
        print('Exception', ex)


def volume_break_worker(code, window, breakout):
    try:
        s = Stock(code=code, length=window * 2)
        if s.volume_break(window=window, breakout=breakout):
            # return code
            return [
                code,
                s.last_volume(),
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
        if s.f_check_7_conditions() and s.last_volume() > 100000:
            return [
                s.LAST_SESSION,
                code,
                s.last_volume(),
                s.EPS,
                s.EPS_MEAN4,
                s.f_get_current_price(),
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
@aiocron.crontab('30 9-15 * * 1-5')
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
    else:
        await bot.default_channel.send('Dellphic h1 empty', delete_after=10.0)


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


# Every 10 minutes, read RSS from F319 and parsing stock code.
@tasks.loop(minutes=5)
async def f319():
    await social_counting(bot.company_list_all, bot.bl_words, f319x)


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

    print(message.channel.id, message.author.id, message.author, message.content)
    msg = message.content.upper()
    ctx = message.channel

    # Only allow run bot in bot.channel_list
    if not msg.startswith('?') and message.channel.id not in bot.channel_black_list:
        msg_x = message.content.upper()
        for x in bot.bl_words:
            msg_x = msg_x.replace(x, '')

        msg_a = re.split(r'[`\-=~!@#$%^&*()_+\[\]{};\'\\:"|<,./<> ]', msg_x)  # msg_x.split(' ')
        # msg_a = re.split('; |, |\\*|\n | |\\? |! |_ |- |. | |\\# |$', msg_x)  # msg_x.split(' ')
        # msg_a = re.split('; |, |\\* |\n | ', msg_x)  # msg_x.split(' ')

        # Process message and insert to db for STAT here
        matches = [x for x in bot.company_list_all if x in msg_a]
        if len(matches) > 0:
            # Insert into database
            print('### MATCH %s' % matches)
            insert_mentioned_code(matches, message.created_at, message.author.id, message.author, 'discord', message.content)
        else:
            print('### NO MATCHES', msg_a)

    if msg.startswith('?'):
        if message.channel.id not in bot.channel_list:
            xh = "```Chỉ chạy bot trong phòng 'gọi-bot'!```"
            if PYTHON_ENVIRONMENT == 'production':
                await message.channel.send(xh, delete_after=10.0)
            else:
                print(xh)
        elif message.channel.id in bot.channel_list:
            cmd = msg.split(' ')[:1][0]
            print('Command: %s' % cmd)
            if cmd in bot.allowed_commands:
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
    p = mp.Pool(mp.cpu_count())
    window = int(args[0]) if (len(args) > 0) else 3
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
async def trending(ctx, *args):
    conn, cursor = get_connection()
    symbol = args[0].upper() if (len(args) > 0) else ''

    print('%s' % ctx.invoked_subcommand)

    if symbol in bot.company_list_all:
        window = float(args[1]) if (len(args) > 1) else 7.0  # default 7hour
        limit = int(args[1]) if (len(args) > 2) else 10  # default limit 10 top
        query_string = '''select mentioned_at as date, symbol, 1 as count from tbl_mentions where symbol=\"''' + symbol + '''\" and mentioned_at > date_sub(now(), interval ''' + str(
            window) + ''' day) order by date desc'''
        sql_query = pd.read_sql_query(query_string, conn)
        trending_list = pd.DataFrame(sql_query)
        close_connection(conn)
        # Re-sample
        x_trend = resample_trending_interval(trending_list, 1440)
        x_trend = x_trend.reindex(index=x_trend.index[::-1])
        x_trend.reset_index(inplace=True, drop=True)
        x_trend = x_trend.head(int(window))

        await ctx.send('%s in trend last %s days ```%s```' % (symbol, window, x_trend.to_string()), delete_after=300.0)
        await ctx.message.delete()

    else:
        window = float(args[0]) if (len(args) > 0) else 24.0  # default 24hour
        limit = int(args[1]) if (len(args) > 1) else 10  # default limit 10 top
        query_string = '''select symbol, count(symbol) as count from tbl_mentions where mentioned_at > date_sub(now(), interval ''' + str(
            window) + ''' hour) group by symbol order by count desc limit ''' + str(limit)
        sql_query = pd.read_sql_query(query_string, conn)
        trending_list = pd.DataFrame(sql_query)
        close_connection(conn)

        await ctx.send('Top %s trending last %s hours ```%s```' % (limit, window, trending_list.to_string()), delete_after=180.0)
        await ctx.message.delete()


@bot.group()
async def amark(ctx, *args):
    if not path.exists('../outputs/amark.xlsx'):
        p = mp.Pool(5)
        buy_rows = [x for x in p.map(stock_worker, bot.company_list) if x is not None]
        p.close()
        p.join()

        results_buy = pd.DataFrame(buy_rows, columns=["Session", "Code", "Volume", "EPS", "EPS_MEAN4", 'Price'])
        results_buy.to_excel("outputs/amark.xlsx")
    cached = datetime.now().strftime("%b%d")

    await ctx.send(file=discord.File('outputs/amark.xlsx'))


#  --------------------------------------------------------------------------------------  #
#  MPT
#  --------------------------------------------------------------------------------------  #
@bot.group()
async def info(ctx, *args):
    """
    :param ctx:
    :param args:
    :return:
    """
    await mpx_info(ctx, *args)


@bot.command()
async def mpt(ctx, *args):
    """
    :param ctx:
    :param args:
    :return:
    """
    await mpx(ctx, *args)


@bot.group()
async def mtp(ctx, *args):
    """
    :param ctx:
    :param args:
    :return:
    """
    await mpx(ctx, *args)


slow.start()
f319.start()
# refresh_data.start()
bot.run(TOKEN)
