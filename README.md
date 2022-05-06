### Tick feed
This is a tick feed that pulls in minute data for crypto exchanges using [Tardis](https://tardis.dev/).
Currently, the scripts:
1. Spin up a `sqlite` DB locally
2. Pull historical data (1 week) and load this into the DB
3. Maintains an open connection and loads live ticks into the DB

### Setup
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
	python3 create_db.py
	```
4. Start the tick feed loading:
	```
	python3 tick_feed.py
	```

### TODO
- [ ] Use normalized historical data feed
- [ ] Parameterize exchange, query fields
- [x] Add `setup.py` and easier setup scripts
