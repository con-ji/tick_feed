"""
Scripts to:
    1. Load exchange/symbol/data type level ticks into a sqlite DB
    2. Stream and load live ticks into the same sqlite DB
"""
import argparse
import asyncio
import datetime
import json
import urllib.parse
import sqlite3

import aiohttp
import ciso8601


INSERT_QUERY = "INSERT OR IGNORE INTO {exchange}_{data_type}_ticks VALUES (?, ?, ?, ?, ?, ?, ?)"


async def replay_normalized_via_tardis_machine(replay_options):
    """
    Pulls normalized messages from the Tardis Machine image via HTTP API.
    """
    timeout = aiohttp.ClientTimeout(total=0)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        encoded_options = urllib.parse.quote_plus(json.dumps(replay_options))

        # assumes tardis-machine HTTP API running on localhost:8000
        url = f"http://localhost:8000/replay-normalized?options={encoded_options}"

        async with session.get(url) as response:
            async for line in response.content:
                yield line


async def replay_normalized(exchange, symbols, data_type, dry_run):
    """
    Loads historical minute-by-minute ticks into a sqlite DB for BTC USD perpetual quotes.

    :param exchange (str): exchange to pull data from
    :param symbols (List[str]): list of symbols to pull data for
    :param data_type (str): Tardis data type to pull for
    :param dry_run (bool): if true, doesn't load to DB - prints to stdout
    """
    today = datetime.datetime.now()
    week_ago = datetime.date.today() - datetime.timedelta(days=7)

    messages = replay_normalized_via_tardis_machine({
        'exchange': exchange,
        'from': week_ago.isoformat(),
        'to': today.isoformat(),
        'symbols': symbols,
        'dataTypes': [data_type],
    })
    conn = sqlite3.connect('tick_feed.db')
    cur = conn.cursor()

    # Load each historical message into sqlite DB
    async for message in messages:
        data = json.loads(message)
        timestamp = int(ciso8601.parse_datetime(data['timestamp']).timestamp() * 1000)
        if not dry_run:
            cur.execute(INSERT_QUERY.format(exchange=exchange, data_type=data_type),
                    (timestamp, data['symbol'],
                     data['bids'][0]['price'], data['bids'][0]['amount'],
                     data['asks'][0]['price'], data['asks'][0]['amount'],
                     json.dumps(data)))
            conn.commit()
        else:
            print(timestamp, data)
    conn.close()



async def live_feed(exchange, symbols, data_type, dry_run):
    """
    Streams live feed ticks from deribit for BTC USD perpetual quotes.
    Loads the messages into a sqlite DB.

    :param exchange (str): exchange to pull data from
    :param symbols (List[str]): list of symbols to pull data for
    :param data_type (str): Tardis data type to pull for
    :param dry_run (bool): if true, doesn't load to DB - prints to stdout
    """
    stream_options = [
        {
            "exchange": exchange,
            "symbols": symbols,
            "dataTypes": [data_type],
        },
    ]

    options = urllib.parse.quote_plus(json.dumps(stream_options))

    url = f"ws://localhost:8001/ws-stream-normalized?options={options}"

    conn = sqlite3.connect('tick_feed.db')
    cur = conn.cursor()

    # Real time quotes from deribit
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url) as websocket:
            async for msg in websocket:
                data = json.loads(msg.data)
                timestamp = int(ciso8601.parse_datetime(data['timestamp']).timestamp() * 1000)
                if not dry_run:
                    cur.execute(INSERT_QUERY.format(exchange=exchange, data_type=data_type),
                        (timestamp, data['symbol'],
                         data['bids'][0]['price'], data['bids'][0]['amount'],
                         data['asks'][0]['price'], data['asks'][0]['amount'],
                         json.dumps(data)))
                    conn.commit()
                else:
                    print(timestamp, data)
    conn.close()


async def main():
    """
    Spawns a coroutine for each task (live and replay), and runs them concurrently.
    """
    parser = argparse.ArgumentParser(
        description='Load historical and live crypto exchange minute tick data')
    parser.add_argument('--exchange', required=True,
        help='exchange to pull ticks from')
    parser.add_argument('--symbols', required=True, nargs='+',
        help='symbol(s) to pull ticks for, e.g. BTC-PERPETUAL or ETHUSD')
    parser.add_argument('--data-type', required=True,
        help='data type: https://docs.tardis.dev/api/tardis-machine#normalized-data-types')
    parser.add_argument('--dry-run',
        action='store_true',
        help='if true, print ticks to stdout instead of loading into DB')
    args = parser.parse_args()

    live_task = asyncio.create_task(live_feed(
        args.exchange, args.symbols, args.data_type, args.dry_run))
    replay_task = asyncio.create_task(replay_normalized(
        args.exchange, args.symbols, args.data_type, args.dry_run))
    await live_task
    await replay_task

asyncio.run(main())
