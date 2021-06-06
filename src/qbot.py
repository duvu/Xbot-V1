import os
import queue
import re
from datetime import datetime, timedelta
from itertools import repeat
from os import path

import aiocron
import discord
import pandas as pd
from discord.ext import commands, tasks
from dotenv import load_dotenv
from lru_redis_cache import LRUCache

import util
from db.database import get_connection, close_connection
from delphic.dellphic import dellphic
from evaluate.evaluate import evaluate_price
from info.infox import infoX
from mpt.mpx import mpx
from stock.stock import Stock
from trending.trending import trendingX
from util import volume_break_load_cached, volume_break_save_cached, get_pool

description = '''An example bot to showcase the discord.ext.commands extension
module.

There are a number of utility commands being showcased here.'''

intents = discord.Intents.default()
intents.members = True

bot = commands.Bot(command_prefix='?', description=description, intents=intents, case_insensitive=True)

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
CHANNEL_ID = os.getenv('DISCORD_CHANNEL')
PYTHON_ENVIRONMENT = os.getenv('PYTHON_ENVIRONMENT')

bot.channel_list = [815900646419071000, 818029515028168714, 843790719877775380]
bot.channel_black_list = [843790719877775380, 826636905475080293]  # room coding --> not check code.
bot.bl_words = [  # blacklist word
    'doanh thu',
    'Doanh thu',
    'DOANH THU',
    'VNI'
]
bot.message_to_delete = queue.Queue()
bot.stock_objects = {}

# f319x = Cache(maxsize=300, ttl=3 * 60 * 60)
bot.cacheX = LRUCache(cache_size=300, ttl=3 * 60 * 60)

bot.allowed_commands = [
    '?MTP', '?MPT',
    '?INFO',
    '?TICKER',
    '?AMARK',
    '?GTLT',
    '?VB',
    '?TRENDING',
    '?EVALUATE',
    '?DINHGIA',
]
bot.good_code = []
bot.company_short_list = []
bot.company_full_list = []


def reload_company_list():
    # Update company list
    conn, cursor = get_connection()
    sql_query = pd.read_sql_query('''select distinct code from tbl_price_board_day where v > 150000 and t > (NOW() - INTERVAL '7 DAYS')''', conn)
    bot.company_short_list = list(pd.DataFrame(sql_query)['code'])
    close_connection(conn)


def gtlt_worker(code, window):
    try:
        s = Stock(code=code, length=2 * window)
        s.load_price_board_day()
        if s.price_increase(window=window):
            return code
        del s
    except Exception as ex:
        print('Exception', ex)


def volume_break_worker(code, window, breakout):
    try:
        s = Stock(code=code, length=window * 2)
        s.load_price_board_day()
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
        if timeframe == 'h1':
            s.load_price_board_minute(length=7500)  # 150h1
            s.re_sample_h()
            dp = dellphic(s.df_h1)

            if dp.tail(1).all():
                return [
                    s.df_h1['date'].iloc[-1],
                    code,
                    s.df_h1['volume'].iloc[-1],
                    s.df_h1['close'].iloc[-1]
                ]
        else:
            s.load_price_board_day(length=150)  # 150 d1
            dp = dellphic(s.df_day)
            if dp.tail(1).all():
                return [
                    s.df_day['date'].iloc[-1],
                    code,
                    s.df_day['volume'].iloc[-1],
                    s.df_day['close'].iloc[-1]
                ]

        del s
    except Exception as ex:
        print('Exception', ex)


def stock_worker(code):
    try:
        s = Stock(code=code)
        s.load_price_board_day()
        s.calculate_indicators()
        if s.f_check_7_conditions() and s.last_volume() > 100000:
            return [
                s.dff,
                code,
                s.last_volume(),
                s.EPS,
                s.EPS_MEAN4,
                s.f_get_current_price(),
            ]

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
    sql_query = pd.read_sql_query('''select * from tbl_company where Exchange='HOSE' or Exchange='HNX' or Exchange='Upcom' order by code ASC''', conn)
    bot.company_full_list = list(pd.DataFrame(sql_query)['code'])
    close_connection(conn)


# At 16:00 on every day-of-week from Monday through Friday.
@aiocron.crontab('1 16 * * 1-5')
async def dellphic_daily():
    if PYTHON_ENVIRONMENT == 'development':
        bot.default_channel = bot.get_channel(815900646419071000)
    else:
        bot.default_channel = bot.get_channel(818029515028168714)
    # Reload company list daily
    reload_company_list()

    print('... dellphic')
    p = get_pool()
    good_codes = [x for x in p.starmap(dellphic_worker, zip(bot.company_short_list, repeat('d1'))) if x is not None]
    p.close()
    p.join()

    if len(good_codes) > 0:
        gc = pd.DataFrame(good_codes, columns=["Session", "Code", "Volume", 'Close'])
        await bot.default_channel.send('Dellphic Daily: ```%s```' % gc.to_string(), delete_after=180.0)


# every 30 minutes from 9 through 15 on every day-of-week from Monday through Friday.
@aiocron.crontab('*/30 9-15 * * 1-5')
async def dellphic_hourly():
    if PYTHON_ENVIRONMENT == 'development':
        bot.default_channel = bot.get_channel(815900646419071000)
    else:
        # bot.default_channel = bot.get_channel(818029515028168714)
        bot.default_channel = bot.get_channel(815900646419071000)

    print('... dellphic')
    p = get_pool()
    good_codes = [x for x in p.starmap(dellphic_worker, zip(bot.company_short_list, repeat('h1'))) if x is not None]
    p.close()
    p.join()

    if len(good_codes) > 0:
        gc = pd.DataFrame(good_codes, columns=["Session", "Code", "Volume", 'Close'])
        await bot.default_channel.send('Dellphic Hourly: ```%s```' % gc.to_string())
    else:
        await bot.default_channel.send('Tôi vẫn đang chạy')

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

        # Process message and insert to db for STAT here
        matches = [x for x in bot.company_full_list if x in msg_a]
        if len(matches) > 0:
            # Insert into database
            print('### MATCH %s' % matches)
            util.insert_mentioned_code(matches, message.created_at, message.author.id, message.author, 'discord', message.id, message.content)
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
                await util.send_info(ctx)

    elif msg.endswith('X3') or msg.endswith('X3.') or msg.endswith('.X3') or msg.endswith(':X'):
        bot.message_to_delete.put(message)


@bot.group()
async def ticker(ctx, *args):
    await ctx.send(bot.company_short_list[:100])


# Gia tang lien tuc x phien
@bot.group()
async def gtlt(ctx, *args):
    p = get_pool()
    window = int(args[0]) if (len(args) > 0) else 3
    codes = [x for x in p.starmap(gtlt_worker, zip(bot.company_short_list, repeat(window))) if x is not None]
    p.close()
    p.join()
    await ctx.send(codes)


@bot.group(pass_context=True, aliases=['dotbien', 'volumebreak'])
async def vb(ctx, *args):
    w = volume_break_load_cached()
    if w is not None:

        if len(w) > 1900:
            await ctx.send('Danh sách khối lượng tăng đột biến đáng chú ý.\n ```%s```' % w[:1900])
            await ctx.send('```%s```' % w[1900:])
        else:
            await ctx.send('Danh sách khối lượng tăng đột biến đáng chú ý.\n ```%s```' % w)
    else:
        p = get_pool()
        window = int(args[0]) if (len(args) > 0) else 20
        breakout = int(args[1]) if (len(args) > 1) else 50
        codes = [x for x in p.starmap(volume_break_worker, zip(bot.company_short_list, repeat(window), repeat(breakout))) if x is not None]
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


@bot.group(pass_context=True, aliases=['xuhuong'])
async def trending(ctx, *args):
    ctx.company_full_list = bot.company_full_list
    await trendingX(ctx, *args)


@bot.group()
async def amark(ctx, *args):
    if not path.exists('../outputs/amark.xlsx'):
        p = get_pool()
        buy_rows = [x for x in p.map(stock_worker, bot.company_short_list) if x is not None]
        p.close()
        p.join()

        results_buy = pd.DataFrame(buy_rows, columns=["Session", "Code", "Volume", "EPS", "EPS_MEAN4", 'Price'])
        results_buy.to_excel("outputs/amark.xlsx")
    cached = datetime.now().strftime("%b%d")

    await ctx.send(file=discord.File('outputs/amark.xlsx'))


#  --------------------------------------------------------------------------------------  #
#  Price EVALUATE
#  --------------------------------------------------------------------------------------  #
@bot.group(pass_context=True, aliases=['dinhgia'])
async def evaluate(ctx, *args):
    """
    Dinh gia - TA
    :param ctx:
    :param args:
    :return:
    """
    await evaluate_price(ctx, *args)


#  --------------------------------------------------------------------------------------  #
#  MPT
#  --------------------------------------------------------------------------------------  #
@bot.group(pass_context=True, aliases=['thongtin', 'huongdan', 'infor'])
async def info(ctx, *args):
    """
    :param ctx:
    :param args:
    :return:
    """
    await infoX(ctx, *args)
    # await mpx_info(ctx, *args)


@bot.command(pass_context=True, aliases=['mtp', 'maiphuongthuy', 'toiuudanhmuc'])
async def mpt(ctx, *args, **kwargs):
    """
    :param ctx:
    :param args:
    :return:
    """
    await mpx(ctx, *args, **kwargs)


# @mpt.error()
# async def error_handler(ctx):
#     await ctx.send('Something went wrong!')

slow.start()
bot.run(TOKEN)
