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

# OS vars
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
CHANNEL = os.getenv('DISCORD_CHANNEL')

# Discord vars
postToDiscord = False
client = discord.Client()

# Pandas options
# pd.options.display.float_format = "{:,.2f}".format

# We only consider stocks with per-share prices inside this range
min_share_price = 5.0

# moving average variables
sma_slow = 30
sma_fast = 13

# Minimum previous-day volume for a stock we might consider
min_volume = 2000000

# Price change threshold - absolute val
price_change_threshold = .10

# time variables for loading historical ticks
time_now = datetime.now().strftime('%Y-%m-%d')
time_before = (datetime.now()-(timedelta(days=7))).strftime('%Y-%m-%d')

# function to return the RSI tag
def apply_rsi_tag(row):
    if row['rsi'] < 30:
        val = '*OVERSOLD*'
    elif row['rsi'] > 70:
        val = '*OVERBOUGHT*'
    else:
        val = ''
    return val

@client.event
async def on_ready():
    # this starts the scanner task in a loop
    run_scanner.start()
    print('Bot is ready and scanning...')

@tasks.loop(count=1)
async def run_scanner():

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
        ticker.prevDay['v'] > min_volume
    )]

    filtered_symbols = [ticker.ticker for ticker in filtered_tickers]
    filtered_symbols = ['HLT']

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

        # add some extra data to the frame
        df['fast_sma'] = df['close'].rolling(window=sma_fast).mean()
        df['slow_sma'] = df['close'].rolling(window=sma_slow).mean()
        df['prev_close'] = df['close'].shift()
        df['prev_fast_sma'] = df['fast_sma'].shift()
        df['prev_slow_sma'] = df['slow_sma'].shift()
        df['symbol'] = symbol
        df['price_change'] = df['close']-df['prev_close']
        df['perc_change'] = ((df['close']-df['prev_close']) / df['prev_close'])*100
        df['rsi'] = ta.RSI(np.array(df['close']))
        # df['rsi_rating'] = df.apply(applyRSI, axis=1)
        df.index = df.index.strftime("%x %I %p")

        # print(df)

        ### NOTE this can be removed / modified if we want to evaluate ALL the crosses for a given time period and not just the last one
        df = df.tail(1)
        
        # # extract dataframes for advancing / declining 13/30 crosses
        df_advancing_crosses = df.loc[(df['fast_sma'] > df['slow_sma']) & (df['prev_fast_sma'] < df['prev_slow_sma'])]
        df_advancing_crosses['dir'] = 'Up'
        df_declining_crosses = df.loc[(df['fast_sma'] < df['slow_sma']) & (df['prev_fast_sma'] > df['prev_slow_sma'])]
        df_declining_crosses['dir'] = 'Down'

        # # combine into results data frame
        new_results_df = pd.concat([df_advancing_crosses, df_declining_crosses])

        # calculate the if the price change is over the threshold
        over_threshold =  abs(new_results_df['price_change']) > price_change_threshold
        new_results_df = new_results_df[over_threshold]
        
        # drop the unecessary columns
        new_results_df = new_results_df.drop(columns=['open', 'high','close','low','fast_sma','slow_sma','prev_fast_sma','prev_slow_sma','prev_close'])
        new_results_df = new_results_df[['symbol','dir','price_change','perc_change','volume','rsi']]

        # print(new_results_df)

        # add the dataframe to the dictionary of dfs if there's room
        if(len(results_df_dict[df_counter]) == max_df_rows_in_message):
            df_counter += 1
            results_df_dict[df_counter] = new_results_df
        else:
            results_df_dict[df_counter] = results_df_dict[df_counter].append(new_results_df)
        # print(results_df_dict)
        
    if(results_df_dict):
        for key in results_df_dict:
            split_df = results_df_dict[key]
            split_df = split_df.reset_index()
            
            if(split_df.empty == False):
                message = '13/30 Moving Average Crossover - ALERT:\n' + tabulate(split_df, headers='keys', tablefmt='github', showindex=False, floatfmt=(",.2f",",.2f",",.2f",",.2f",",.2f",",.2f",",.0f"))
                print(message)

                if(postToDiscord):
                    # retrieve the channel
                    channel = client.get_channel(721931969138786364)
                    print('Sending Results to Discord Channel - ',channel)

                    # format the message as a block
                    message = '```' + message + '```'
                
                    await channel.send(message)
    else:
        print('No Crossovers detected')
    
client.run(TOKEN)