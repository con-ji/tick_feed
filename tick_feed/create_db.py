"""
Small script to set up the sqlite DB for tick loading
"""
import sqlite3


def create_db():
    """
    Creates a local sqlite DB file with a table for deribit perpetual quotes.
    """
    conn = sqlite3.connect('tick_feed.db')
    cur = conn.cursor()

    # Create table for the tardis messages
    # Primary key will be timestamp since these should always be unique
    cur.execute(
        '''
        CREATE TABLE deribit_perp_quotes (
        timestamp INTEGER NOT NULL,
        instrument_name TEXT NOT NULL,
        bid_price REAL NOT NULL,
        bid_amount REAL NOT NULL,
        ask_price REAL NOT NULL,
        ask_amount REAL NOT NULL,
        PRIMARY KEY(timestamp, instrument_name, bid_price, bid_amount, ask_price, ask_amount)
        );
        ''')
    conn.commit()
    conn.close()

create_db()
