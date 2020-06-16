import alpaca_trade_api as tradeapi
import requests
import logging
import time
from tabulate import tabulate
from pytz import timezone
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
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
CHANNEL = os.getenv('DISCORD_CHANNEL')

# Discord 
postToDiscord = True
client = discord.Client()

# We only consider stocks with per-share prices inside this range
min_share_price = 5.0
max_share_price = 500.0

# moving average variables
sma_slow = 30
sma_fast = 13

# Minimum previous-day volume for a stock we might consider
min_volume = 2000000

# time variables for loading historical ticks
time_now = datetime.now().strftime('%Y-%m-%d')
time_before = (datetime.now()-(timedelta(days=7))).strftime('%Y-%m-%d')

# timetracking variables when first run
nyc = timezone('America/New_York')
trackedHour = datetime.now().astimezone(nyc).hour
minuteToRunOn = 1

@client.event
async def on_ready():
    # this starts the scanner task in a loop
    run_scanner.start()
    print('Bot is ready and scanning...')

@tasks.loop(seconds=30)
async def run_scanner():

    if((trackedHour !=  datetime.now().astimezone(nyc).hour) &
       (datetime.now().astimezone(nyc).minute == minuteToRunOn) &
       (datetime.now().astimezone(nyc).hour >= 9) & 
       (datetime.now().astimezone(nyc).hour <= 16)):

        df_counter = 1
        max_df_rows_in_message = 10
        
        results_df_dict = {1 : pd.DataFrame()}

        # gets a list of equities to evaluate
        print('Getting current ticker data...')
        tickers = api.polygon.all_tickers()
        assets = api.list_assets()
        symbols = [asset.symbol for asset in assets if asset.tradable]
        filtered_tickers = [ticker for ticker in tickers if (
            ticker.ticker in symbols and
            ticker.lastTrade['p'] >= min_share_price and
            ticker.lastTrade['p'] <= max_share_price and
            ticker.prevDay['v'] > min_volume
        )]

        filtered_symbols = [ticker.ticker for ticker in filtered_tickers]
        # filtered_symbols = ['ENPH']

        print('Filtered_symbols length = ',len(filtered_symbols))
        print(filtered_symbols)

        print('Getting historical data...')
        hour_history = {}
        c = 0
        for symbol in filtered_symbols:
            hour_history[symbol] = api.polygon.historic_agg_v2(
                symbol=symbol, multiplier=60, timespan="minute", _from=time_before, to=time_now
            ).df
            c += 1
            print('{}/{}'.format(c, len(filtered_symbols)))
        print('Scanning data...')

        for symbol in filtered_symbols:

            df = hour_history.get(symbol).copy()

            # first drop any items where the timestamp is outide of 9 - 16 (regular trading hours)
            df = df[(df.index.hour >= 9) & (df.index.hour < 16)]
            
            # print(df)

            # add to the data frame the fast and slow moving averages as well as the previous values
            df['fast_sma'] = df['close'].rolling(window=sma_fast).mean()
            df['slow_sma'] = df['close'].rolling(window=sma_slow).mean()
            df['prev_close'] = df['close'].shift()
            df['prev_fast_sma'] = df['fast_sma'].shift()
            df['prev_slow_sma'] = df['slow_sma'].shift()
            df['symbol'] = symbol

            df = df.tail(1)
            print(df)
            
            # # extract dataframes for advancing / declining 13/30 crosses
            df_advancing_crosses = df.loc[(df['fast_sma'] > df['slow_sma']) & (df['prev_fast_sma'] < df['prev_slow_sma'])]
            df_advancing_crosses['dir'] = 'Up'
            # # print(df_advancing_crosses)
            df_declining_crosses = df.loc[(df['fast_sma'] < df['slow_sma']) & (df['prev_fast_sma'] > df['prev_slow_sma'])]
            df_declining_crosses['dir'] = 'Down'
            # # print(df_declining_crosses)

            # # combine into results data frame
            new_results_df = pd.concat([df_advancing_crosses, df_declining_crosses])

            # calculate the difference between the moving averages as a "score"
            new_results_df['price_change'] = new_results_df['close'] - new_results_df['prev_close']
            
            # drop the unecessary columns
            new_results_df = new_results_df.drop(columns=['open', 'high','close' 'low','fast_sma','slow_sma','prev_fast_sma','prev_slow_sma','prev_close'])
            new_results_df = new_results_df[['symbol','dir','price_change','volume']]
            # print(new_results_df)

            # add the dataframe to the dictionary of dfs if there's room
            if(len(results_df_dict[df_counter]) == max_df_rows_in_message):
                df_counter += 1
                results_df_dict[df_counter] = new_results_df
            else:
                results_df_dict[df_counter] = results_df_dict[df_counter].append(new_results_df)
        
        # reset the tracked hour
        trackedHour = datetime.now().astimezone(nyc).hour

        if(results_df_dict):
            for key in results_df_dict:
                split_df = results_df_dict[key]
                if(split_df.empty == False):
                    message = '13/30 Moving Average Crossover - ALERT:\n' + tabulate(split_df, headers=['symbol','dir','price_change','volume'], tablefmt='github' )
                    print(message)

                    if(postToDiscord):
                        # retrieve the channel
                        channel = client.get_channel(721931969138786364)
                        print('Sending Results to Discord Channel - ',channel)

                        # format the message as a block
                        message = '```' + message + '```'
                    
                        await channel.send(message)

    client.run(TOKEN)