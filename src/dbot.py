# dbot.py
import asyncio
import os
from datetime import datetime

import discord
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
CHANNEL_ID = os.getenv('DISCORD_CHANNEL')


# https://discord.com/api/webhooks/835476667752906772/ran0btZ-JctcqRLCVZ4Mf8Jfiqs7ru6qfZ3B0Gsu4GcndRjdKyQmGArxSd39vgq9Wobm

class MyClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.my_background_task())

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')

    async def my_background_task(self):
        await self.wait_until_ready()
        counter = 0
        channel = self.get_channel(int(CHANNEL_ID))  # channel ID goes here
        print(f'preparing to send a message ...')
        while not self.is_closed():
            self.buy_list = []
            self.my_filter()
            pd_buy = pd.DataFrame(self.buy_list, columns=["Session", "Code", "Volume", "EPS", "EPS_MEAN4", 'Price', 'Changed', 'Agree', 'Disagree'])
            message = f''' ```%s``` ''' % (pd_buy['code'].to_markdown(tablefmt='grid'))
            counter += 1

            sheet_name = datetime.now().strftime("%b%d")
            pd_buy.to_excel("outputs/outputs_buy" + sheet_name + ".xlsx", sheet_name=sheet_name)

            # await channel.send(str(counter) + message)
            await channel.send(file=discord.File("outputs/outputs_buy" + sheet_name + ".xlsx"))
            await asyncio.sleep(1 * 60 * 60)  # task runs every 1 hour

    def my_filter(self):
        for idx, code in self.df_company_list['code'].iteritems():
            self.my_worker(code)

    # @multitasking.task  # <== this is all it takes :-)
    def my_worker(self, code):
        try:
            s = Stock(code=code)
            s.consensus_day.evaluate_ichimoku()
            s_score = s.consensus_day.score()
            buy_agreement = s_score['buy_agreement'].iloc[-1]
            buy_disagreement = s_score['buy_disagreement'].iloc[-1]
            if buy_agreement > buy_disagreement:
                print("BUY BUY BUY %s" % code)
                self.buy_list.append(
                    [
                        s.LAST_SESSION,
                        code,
                        s.last_volume(),
                        s.EPS,
                        s.EPS_MEAN4,
                        s.f_get_current_price(),
                        s.f_last_changed() * 100,
                        buy_agreement,
                        buy_disagreement
                    ])
            del s  # dispose s
        except Exception as ex:
            print('Exception', ex)


client = MyClient()
client.run(TOKEN)
