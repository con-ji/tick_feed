"""
Scripts to:
    1. Load deribit BTC USD perpetual quotes into a sqlite DB
    2. Stream and load derebit BTC USD perpetual quotes into the same sqlite DB
TODO:
    1. Use normalized replay data
    2. Pass in arguments for exchanges, symbols
"""
import asyncio
import aiohttp
import datetime
import json
import urllib.parse
import sqlite3
import time

import ciso8601
import pandas as pd
from tardis_client import TardisClient, Channel


API_KEY = "TD.bJc2KGlCAGSsssq5.C2vovEEvbOrMct5.M1L1DoachKERl-D.Ao1zF1RSX9yaDqM.v0aqZH57d3qTKmE.7HCI"
COLS = ['timestamp', 'instrument_name', 'bid_price', 'bid_amount', 'ask_price', 'ask_amount']
DATE_FMT = '%Y-%m-%d'

INSERT_QUERY = "INSERT OR IGNORE INTO deribit_perp_quotes VALUES (?, ?, ?, ?, ?, ?)"


async def replay_full_load():
    """
    Pulls historical data for BTCUSD perpetual quotes from deribit.
    Loads the messages into a sqlite DB from a single dataframe.
    """
    tardis_client = TardisClient(api_key=API_KEY)
    today = datetime.date.today() + datetime.timedelta(days=1)
    week_ago = today - datetime.timedelta(days=1)

    messages = tardis_client.replay(
        exchange='deribit',
        from_date=week_ago.strftime(DATE_FMT),
        to_date=today.strftime(DATE_FMT),
        filters=[Channel(name="quote", symbols=["BTC-PERPETUAL"])],
    )

    conn = sqlite3.connect('tick_feed.db')

    msgs = []
    async for local_timestamp, message in messages:
        data = message['params']['data']
        data_dict = {
                'timestamp': data['timestamp'],
                'instrument_name': data['instrument_name'],
                'bid_price': data['best_bid_price'],
                'bid_amount': data['best_bid_amount'],
                'ask_price': data['best_ask_price'],
                'ask_amount': data['best_ask_amount'],
        }
        msgs.append(data_dict)
    msgs_df = pd.DataFrame(msgs, columns=cols)
    msgs_df.to_sql('deribit_perp_quotes', conn, if_exists='replace', index=False)
    conn.close()


async def replay():
    """
    Pulls historical data for BTCUSD perpetual quotes from deribit.
    Loads the messages into a sqlite DB from a single dataframe.
    """
    tardis_client = TardisClient(api_key=API_KEY)
    today = datetime.date.today() + datetime.timedelta(days=1)
    week_ago = today - datetime.timedelta(days=8)

    messages = tardis_client.replay(
        exchange='deribit',
        from_date=week_ago.strftime(DATE_FMT),
        to_date=today.strftime(DATE_FMT),
        filters=[Channel(name="quote", symbols=["BTC-PERPETUAL"])],
    )

    conn = sqlite3.connect('tick_feed.db')
    cur = conn.cursor()

    # Load each historical message into sqlite DB
    last_msg = {}
    last_minute = 0
    async for local_timestamp, message in messages:
        # We only really care about the message here
        data = message['params']['data']
        curr_minute = data['timestamp'] - data['timestamp'] % 60000
        if curr_minute > last_minute:
            if last_msg:
                cur.execute(INSERT_QUERY,
                        (last_minute, last_msg['instrument_name'],
                         last_msg['best_bid_price'], last_msg['best_bid_price'],
                         last_msg['best_ask_price'], last_msg['best_ask_amount']))
                conn.commit()
            last_minute = curr_minute
        last_msg = data
    conn.close()



async def live_feed():
    """
    Streams live feed ticks from deribit for BTC USD perpetual quotes.
    Loads the messages into a sqlite DB.
    """
    stream_options = [
        {
            "exchange": "deribit",
            "symbols": ["BTC-PERPETUAL"],
            "dataTypes": ["quote"],
        },
    ]

    options = urllib.parse.quote_plus(json.dumps(stream_options))

    URL = f"ws://localhost:8001/ws-stream-normalized?options={options}"

    conn = sqlite3.connect('tick_feed.db')
    cur = conn.cursor()

    last_msg = {}
    last_minute = 0
    # Real time quotes from deribit
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(URL) as websocket:
            async for msg in websocket:
                data = json.loads(msg.data)
                ts = ciso8601.parse_datetime(data['timestamp'])
                timestamp = int(ts.timestamp() * 1000)
                curr_minute = timestamp - timestamp % 60000
                if curr_minute > last_minute:
                    if last_msg:
                        cur.execute(
                            INSERT_QUERY,
                            (timestamp, data['symbol'],
                             data['bids'][0]['price'], data['bids'][0]['amount'],
                             data['asks'][0]['price'], data['asks'][0]['amount']))
                        conn.commit()
                    last_minute = curr_minute
                last_msg = data
    conn.close()


async def main():
    live_task = asyncio.create_task(live_feed())
    replay_task = asyncio.create_task(replay())
    await live_feed()
    await replay()

asyncio.run(main())
