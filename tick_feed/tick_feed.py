"""
Scripts to:
    1. Load deribit BTC USD perpetual quotes into a sqlite DB
    2. Stream and load derebit BTC USD perpetual quotes into the same sqlite DB
TODO:
    1. Pass in arguments for exchanges, symbols (requires generating tables, or normalizing things)
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


async def replay_normalized_via_tardis_machine(replay_options):
    timeout = aiohttp.ClientTimeout(total=0)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # url encode as json object options
        encoded_options = urllib.parse.quote_plus(json.dumps(replay_options))

        # assumes tardis-machine HTTP API running on localhost:8000
        url = f"http://localhost:8000/replay-normalized?options={encoded_options}"

        async with session.get(url) as response:
            # otherwise we may get line too long errors
            response.content._high_water = 100_000_000

            async for line in response.content:
                yield line


async def replay_normalized():
    today = datetime.datetime.now()
    week_ago = datetime.date.today() - datetime.timedelta(days=7)

    messages = replay_normalized_via_tardis_machine({
        'exchange': 'deribit',
        'from': week_ago.isoformat(),
        'to': today.isoformat(),
        'symbols': ["BTC-PERPETUAL"],
        'dataTypes': ['quote'],
    })
    conn = sqlite3.connect('tick_feed.db')
    cur = conn.cursor()

    # Load each historical message into sqlite DB
    last_msg = {}
    last_minute = 0
    async for message in messages:
        # We only really care about the message here
        data = json.loads(message)
        ts = ciso8601.parse_datetime(data['timestamp'])
        timestamp = int(ts.timestamp() * 1000)
        curr_minute = timestamp - timestamp % 60000
        if curr_minute > last_minute:
            if last_msg:
                print('inserting historical data')
                cur.execute(INSERT_QUERY,
                        (last_minute, last_msg['symbol'],
                         last_msg['bids'][0]['price'], last_msg['bids'][0]['amount'],
                         last_msg['asks'][0]['price'], last_msg['asks'][0]['amount']))
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
                        print('inserting live data')
                        cur.execute(
                            INSERT_QUERY,
                            (last_minute, last_msg['symbol'],
                             last_msg['bids'][0]['price'], last_msg['bids'][0]['amount'],
                             last_msg['asks'][0]['price'], last_msg['asks'][0]['amount']))
                        conn.commit()
                    last_minute = curr_minute
                last_msg = data
    conn.close()


async def main():
    live_task = asyncio.create_task(live_feed())
    replay_task = asyncio.create_task(replay_normalized())
    await live_task
    await replay_task

asyncio.run(main())
