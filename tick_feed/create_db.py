"""
Small script to set up the sqlite DB for tick loading
"""
import argparse
import sqlite3


def create_db_table(exchange, data_type):
    """
    Creates a local sqlite DB file with a table for deribit perpetual quotes.

    :param exchange (str): exchange to build a table for
    :param data_type (str): Tardis data type to pull for
    """
    conn = sqlite3.connect('tick_feed.db')
    cur = conn.cursor()

    table = '{exchange}_{data_type}_ticks'.format(exchange=exchange, data_type=data_type)

    # Create table for the tardis messages
    # Primary key will be timestamp since these should always be unique
    cur.execute("""
        CREATE TABLE {table} (
        timestamp INTEGER NOT NULL,
        instrument_name TEXT NOT NULL,
        bid_price REAL NOT NULL,
        bid_amount REAL NOT NULL,
        ask_price REAL NOT NULL,
        ask_amount REAL NOT NULL,
        message TEXT NOT NULL,
        PRIMARY KEY(timestamp, instrument_name, bid_price, bid_amount, ask_price, ask_amount)
        );
        """.format(table=table))
    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(
        description='Load historical and live crypto exchange minute tick data')
    parser.add_argument('--exchange', required=True,
        help='exchange to pull ticks from')
    parser.add_argument('--data-type', required=True,
        help='data type: https://docs.tardis.dev/api/tardis-machine#normalized-data-types')
    args = parser.parse_args()

    create_db_table(args.exchange, args.data_type)

main()
