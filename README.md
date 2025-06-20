# CEX Price/Volume Aggregator

This project contains an asynchronous worker that aggregates price and volume data from centralized exchanges and uploads normalized results to Google BigQuery.

## Setup

1. Create a `.env` file based on `.env.example` and provide your Google Cloud credentials and worker parameters.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the worker:

```bash
python workers/worker_cex_aggregator.py
```

## Files

- `clients/` – exchange clients implementing a common asynchronous interface.
- `gcp_utils.py` – helper functions to upload data to BigQuery.
- `workers/worker_cex_aggregator.py` – main worker fetching and uploading OHLCV data.

## FAQ

### How do I add another exchange?
Implement a new client in the `clients/` package inheriting from `AbstractCEXClient` and instantiate it in the worker.
