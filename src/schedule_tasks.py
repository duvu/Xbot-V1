# # At 16:00 on every day-of-week from Monday through Friday.
# import os
#
# import aiocron
# from dotenv import load_dotenv
#
# load_dotenv()
#
#
# PYTHON_ENVIRONMENT = os.getenv('PYTHON_ENVIRONMENT')
#
# @aiocron.crontab('1 16 * * 1-5')
# async def dellphic_daily():
#     if PYTHON_ENVIRONMENT == 'development':
#         bot.default_channel = bot.get_channel(815900646419071000)
#     else:
#         bot.default_channel = bot.get_channel(818029515028168714)
#     # Reload company list daily
#     reload_company_list()
#
#     print('... dellphic')
#     p = get_pool()
#     good_codes = [x for x in p.starmap(dellphic_worker, zip(bot.company_short_list, repeat('d1'))) if x is not None]
#     p.close()
#     p.join()
#
#     if len(good_codes) > 0:
#         gc = pd.DataFrame(good_codes, columns=["Session", "Code", "Volume", 'Close'])
#         await bot.default_channel.send('Dellphic Daily: ```%s```' % gc.to_string(), delete_after=180.0)
#
#
# # every 30 minutes from 9 through 15 on every day-of-week from Monday through Friday.
# @aiocron.crontab('*/30 9-15 * * 1-5')
# async def dellphic_hourly():
#     if PYTHON_ENVIRONMENT == 'development':
#         bot.default_channel = bot.get_channel(815900646419071000)
#     else:
#         # bot.default_channel = bot.get_channel(818029515028168714)
#         bot.default_channel = bot.get_channel(815900646419071000)
#
#     print('... dellphic')
#     p = get_pool()
#     good_codes = [x for x in p.starmap(dellphic_worker, zip(bot.company_short_list, repeat('h1'))) if x is not None]
#     p.close()
#     p.join()
#
#     if len(good_codes) > 0:
#         gc = pd.DataFrame(good_codes, columns=["Session", "Code", "Volume", 'Close'])
#         await bot.default_channel.send('Dellphic Hourly: ```%s```' % gc.to_string())
#     else:
#         await bot.default_channel.send('Tôi vẫn đang chạy')
