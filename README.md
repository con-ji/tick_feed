### Tick feed
This is a tick feed that pulls in minute data for crypto exchanges using [Tardis](https://tardis.dev/).
Currently, the scripts:
1. Spin up a `sqlite` DB locally
2. Pull historical data (1 week) and load this into the DB
3. Maintains an open connection and loads live ticks into the DB

### Instructions 
1. Run the following script with Docker running:
	```
	docker run -p 8000:8000 -p 8001:8001 -e "TM_API_KEY=YOUR_API_KEY" -d tardisdev/tardis-machine
	```
2. Run the following to install packages:
	```
	pip install -r requirements.txt
	```
3. Set up the `sqlite` DB:
	```
	python3 tick_feed/create_db.py --exchange EXCHANGE
	```
4. Start the tick feed loading (exchange must match the exchange created by `create_db.py`):
	```
	python3 tick_feed/tick_feed.py --exchange EXCHANGE --symbols SYMBOL_1 SYMBOL_2 --data-type DATA_TYPE [--dry-run]
	```
For example, to get minute level BTCUSD perpetual quotes on deribit:
```
python3 tick_feed/create_db.py --exchange deribit --data-type book_snapshot_1_60s
python3 tick_feed/tick_feed.py --exchange deribit --symbols BTC-PERPETUAL --data-type book_snapshot_1_60s
```

### TODO
- [x] Use normalized historical data feed
- [x] Parameterize exchange, query fields. This might get weird due to different symbols and ["normalized data types"](https://docs.tardis.dev/api/tardis-machine#normalized-data-types)
- [x] Add `setup.py` and easier setup scripts
