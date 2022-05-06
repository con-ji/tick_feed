"""
Small script to set up the sqlite DB for tick loading
"""
import argparse
import sqlite3


def create_db_table(exchange):
    """
    Creates a local sqlite DB file with a table for deribit perpetual quotes.

    :param exchange (str): exchange to build a table for
    """
    conn = sqlite3.connect('tick_feed.db')
    cur = conn.cursor()

    table = '{exchange}_ticks'.format(exchange=exchange)

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
    args = parser.parse_args()

    create_db_table(args.exchange)

main()
