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

from mpt import calc_correlation, optimize_profit
from stock import Stock

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

channel_list = [815900646419071000, 818029515028168714]
bot.message_to_delete = queue.Queue()

bot.conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
bot.cursor = bot.conn.cursor()
bot.allowed_commands = ['?MTP', '?MPT', '?INFO', '?TICKER', '?AMARK']
bot.good_code = []


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


@bot.event
async def on_ready():
    print('Logged in as')
    print(bot.user.name)
    print(bot.user.id)
    print('------')
    if PYTHON_ENVIRONMENT == 'development':
        sql_query = pd.read_sql_query('''select * from tbl_company where Exchange='HOSE' or Exchange='HNX' or Exchange='Upcom' order by Code ASC limit 50''', bot.conn)
        bot.company_list = list(pd.DataFrame(sql_query)['Code'])
        bot.conn.close()
    else:
        sql_query = pd.read_sql_query('''select * from tbl_company where Exchange='HOSE' or Exchange='HNX' or Exchange='Upcom' order by Code ASC''', bot.conn)
        bot.company_list = list(pd.DataFrame(sql_query)['Code'])
        bot.conn.close()


@tasks.loop(seconds=15)
async def slow():
    print('slowing ...', bot.message_to_delete.qsize())
    msg = bot.message_to_delete.get() if bot.message_to_delete.qsize() > 0 else None
    if msg is not None:
        print('slowing ...', msg.created_at, datetime.utcnow() + timedelta(minutes=-3))

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
    if message.author.id == bot.user.id:
        return
    # Not process if message is from the bot
    print(message.channel.id, message.author.id, message.author, message.content)

    # Only allow run bot in channel_list
    msg = message.content.upper()
    ctx = message.channel
    cmd = msg.split(' ')[:1][0]

    if msg.startswith('?') and message.channel.id not in channel_list:
        xh = "```Chỉ chạy bot trong phòng 'gọi-bot'!```"
        if PYTHON_ENVIRONMENT == 'production':
            await message.channel.send(xh, delete_after=10.0)
        else:
            print(xh)
    elif cmd in bot.allowed_commands:
        print('Come here')
        #     symbols = msg.split(' ')[1:]
        #     await ctx.send('`{}` mã : `{}`'.format(len(symbols), ', '.join(symbols)))
        #     symbols, mean, corr = calc_correlation(symbols, 120)
        #     result = optimize_profit(symbols, mean, corr)
        #     await ctx.send(result.to_string())
        await bot.process_commands(message)
    elif msg.endswith('X3') or msg.endswith('X3.') or msg.endswith('.X3') or msg.endswith(':X'):
        bot.message_to_delete.put(message)
    elif msg.startswith('?') and message.channel.id in channel_list:
        xh = "Hãy dùng cú pháp: ```?mtp mã_ck1 mã_ck2 ....```"
        if PYTHON_ENVIRONMENT == 'production':
            await ctx.send(xh, delete_after=10.0)
        else:
            print(xh)


@bot.group()
async def ticker(ctx, *args):
    await ctx.send(bot.company_list[:100])


@bot.group()
async def amark(ctx, *args):
    if not path.exists('outputs/amark.xlsx'):
        p = mp.Pool(5)
        buy_rows = [x for x in p.map(stock_worker, bot.company_list) if x is not None]
        p.close()
        p.join()

        results_buy = pd.DataFrame(buy_rows, columns=["Session", "Code", "Volume", "EPS", "EPS_MEAN4", 'Price', 'Changed'])
        results_buy.to_excel("outputs/amark.xlsx")

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
bot.run(TOKEN)

