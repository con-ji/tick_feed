### Tick feed
This is a tick feed that pulls in minute data for crypto exchanges using [Tardis](https://tardis.dev/).
Currently, the scripts:
1. Spin up a `sqlite` DB locally
2. Pull historical data (1 week) and load this into the DB
3. Maintains an open connection and loads live ticks into the DB

### TODO
- [] Use normalized historical data feed
- [] Parameterize exchange, query fields
- [] Add `setup.py` and easier setup scripts
