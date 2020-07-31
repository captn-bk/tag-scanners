'''
TO DOs and Enhancement Ideas:
- Look at evaluating the size of the move compared to the max size of the move in the Trend - ex DNJR - would have caught the first pop up on 7/29
- Add a "scanner attribute" that tells us why it's showing up...curling vs. price action
- Add Heiken Ashi attributes to the dataframe and use this to scan for 4 consecutive higher bars (aka curling up) - higher percent than the last move 
    - this could use a percent increase threshold for each of the 4 bars (.25% for example) -
    examples: MNK from 7/29
    examples: MOGU from 7/30
- Run this scanner with a paper trade account once I have it dialed in
- Add other ticker info to the output (example: Industry)
'''

import alpaca_trade_api as tradeapi
import requests
import logging
import time
from tabulate import tabulate
import talib as ta
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
import pandas as pd
import asyncio
import discord
from dotenv import load_dotenv
from discord.ext import tasks, commands
import os

load_dotenv() 
api = tradeapi.REST()

# OS variables
TOKEN = 'NzM3NTE5NzgyNjI2MzI4NjI3.Xx-i0g.ow-DdKb_mhkEB528AQoWdKZoD5c'
GUILD = 'TAG Alerts'
CHANNEL = os.getenv('DISCORD_CHANNEL')

# Discord 
postToDiscord = False
client = discord.Client()

# We only consider stocks with per-share prices inside this range
min_share_price = .50
max_share_price = 20

# Minimum previous-day volume for a stock we might consider
minimum_daily_volume = 100000

# minimum bar volume to show up on scanner
minimum_bar_volume = 2000

# Previous Bars to consider for trending
trend_bar_count = 10

# Number of bars to evaluate against the past trend
eval_bar_count = 1

# The average price of the eval period needs to be greater than the trend period by this factor
price_percentage_threshold = 5

# The average size of the eval period bars to be greater than the average bar size of the trend period by this factor
bar_size_factor_threshold = 5

# minimum size of the move on the eval bars in order to be included
bar_size_threshold = .05

# The volume of the eval period needs to be greater than the trend period by this factor
# volume_percentage_threshold = 

# time variables for loading historical ticks
time_now = datetime.now().strftime('%Y-%m-%d')
time_before = (datetime.now()-(timedelta(minutes=30))).strftime('%Y-%m-%d')

# returns the minute historical data for each ticker
def get_min_history_data(symbols):
    print('Getting historical data...')
    minute_history = {}
    c = 0
    for symbol in symbols:
        minute_history[symbol] = api.polygon.historic_agg_v2(
            symbol=symbol, multiplier=1, timespan="minute", _from=time_before, to=time_now
        ).df
        c += 1
        print('{}/{}'.format(c, len(symbols)))
    print('Success.')
    return minute_history

# gets a list of equities to evaluate
def get_tickers():
    print('Getting current ticker data...')
    tickers = api.polygon.all_tickers()
    print('Success.')
    assets = api.list_assets()
    symbols = [asset.symbol for asset in assets if asset.tradable]
    return [ticker for ticker in tickers if (
        ticker.ticker in symbols and
        ticker.lastTrade['p'] >= min_share_price and
        ticker.lastTrade['p'] <= max_share_price and
        ticker.prevDay['v'] > minimum_daily_volume
    )]

def run(tickers):
    # Establish streaming connection
    conn = tradeapi.StreamConn()

    symbols = [ticker.ticker for ticker in tickers]
    # symbols = ["SNOA"]

    print('Tracking {} symbols.'.format(len(symbols)))
    minute_history = get_min_history_data(symbols)

    # Connect to Minute Bars Data via Polygon
    @conn.on(r'AM$')
    async def handle_minute_bar(conn, channel, data):

        # add the new bar data to the minute history dataframe
        ts = data.start
        ts -= timedelta(microseconds=ts.microsecond)
        minute_history[data.symbol].loc[ts] = [
            data.open,
            data.high,
            data.low,
            data.close,
            data.volume
        ]

        alert = False

        # strip out only the bars we need
        totalBarsToEval = trend_bar_count + eval_bar_count
        history_in_scope = minute_history[data.symbol].tail(totalBarsToEval)
        df = history_in_scope.copy()

        # add the Heiken Ashi bar data:
        df = addHeikenAshi(df)
        
        # print('symbol = ' , data.symbol)
        # print('df =', df)

        # add some extra data to the frame
        df['symbol'] = data.symbol

        # price change
        df['prev_close'] = df['close'].shift()
        df['price_change'] = df['close']-df['prev_close']
        df['%_price_change'] = ((df['close']-df['prev_close']) / df['prev_close'])*100
        
        # volume changes
        df['prev_volume'] = df['volume'].shift()
        df['volume_change'] = df['volume']-df['prev_volume']
        df['%_volume_change'] = ((df['volume']-df['prev_volume']) / df['prev_volume'])*100

        # bar sizes absolute
        df['bar_size_abs'] = (df['close']-df['open']).abs()
        
        # df.index = df.index.strftime("%x %I %p")
        # df = df.tail(1)
        # print(df)

        # trend values:
        trend_bars = df.head(trend_bar_count)
        trend_volume    = trend_bars["volume"].mean()
        trend_price_avg = trend_bars["close"].mean()
        trend_price_max = trend_bars["close"].max()
        trend_price_min = trend_bars["close"].min()
        trend_bar_size_avg = trend_bars["bar_size_abs"].mean()
        
        # evaluation values:
        eval_bars = df.tail(eval_bar_count)
        eval_volume     = eval_bars["volume"].mean()
        eval_price_avg  = eval_bars["close"].mean()
        eval_%_price_change_avg = eval_bars["%_price_change"].mean()
        eval_bar_size_avg = eval_bars["bar_size_abs"].mean()

        # calculated variables
        # FIXME: invalid value encountered in double_scalars
        # bar_size_factor = eval_bar_size_avg / trend_bar_size_avg

        # determine if it should be alerted:
        # if( eval_price_avg > trend_price_max and eval_volume > minimum_bar_volume):
        #         if(eval_%_price_change_avg > price_percentage_threshold or 
        #            (bar_size_factor > bar_size_factor_threshold and eval_bar_size_avg > bar_size_threshold)):
        #             alert = True

        if( eval_price_avg > trend_price_max and eval_volume > minimum_bar_volume):
            if(eval_%_price_change_avg > price_percentage_threshold):
                alert = True
                    
        
        # return if the alert signal is flase
        if(alert == False):
            return

        # drop some unecesarry columns
        df = df.drop(columns=['open','high','low','prev_close','prev_volume','volume_change','%_volume_change'])

        # trim to the last 5 items in the frame
        alert_df = df.tail(5)

        # message = 'Price Momentum Alert:\n' + tabulate(alert_df, headers='keys', tablefmt='github', showindex=False, floatfmt=(",.2f",",.2f",",.2f",",.2f",",.2f",",.2f",",.0f"))
        message = 'Price Momentum Alert:\n' + tabulate(alert_df, headers='keys', tablefmt='github', showindex=True)
        print(message)

        if(postToDiscord):
            # retrieve the channel
            channel = client.get_channel(721931969138786364)
            print('Sending Results to Discord Channel - ',channel)

            # format the message as a block for discord
            message = '```' + message + '```'

            await channel.send(message)
        
    # define channels and run the scanner for each
    channels = []
    for symbol in symbols:
        symbol_channels = ['A.{}'.format(symbol), 'AM.{}'.format(symbol)]
        channels += symbol_channels
    print('Watching {} symbols.'.format(len(symbols)))
    run_ws(conn, channels)


# add Heiken Ashi values:
def addHeikenAshi(df):
    df['HA_Close']=(df['open']+ df['high']+ df['low']+df['close'])/4

    idx = df.index.name
    df.reset_index(inplace=True)

    for i in range(0, len(df)):
        if i == 0:
            df.set_value(i, 'HA_Open', ((df.get_value(i, 'open') + df.get_value(i, 'close')) / 2))
        else:
            df.set_value(i, 'HA_Open', ((df.get_value(i - 1, 'HA_Open') + df.get_value(i - 1, 'HA_Close')) / 2))

    if idx:
        df.set_index(idx, inplace=True)

    df['HA_High']=df[['HA_Open','HA_Close','High']].max(axis=1)
    df['HA_Low']=df[['HA_Open','HA_Close','Low']].min(axis=1)
    return df

# Handle failed websocket connections by reconnecting
def run_ws(conn, channels):
    try:
        conn.run(channels)
    except Exception as e:
        print(e)
        conn.close()
        run_ws(conn, channels)

# @client.event
# async def on_ready():
#     print('Bot is ready and scanning...')
#     run_scanner.start()
#     return

# @tasks.loop(count=1)
# async def run_scanner():
#     await run(get_tickers())

if __name__ == "__main__":
    # client.run(TOKEN)
    run(get_tickers())
    

