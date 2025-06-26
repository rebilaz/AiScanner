# CEX Data Aggregation Worker

This project provides an asynchronous worker to collect OHLCV data from centralized exchanges (Binance and Kraken) and upload the results to BigQuery.

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file based on `.env.example` and provide your Google Cloud project information, optional rate limits, and any proxy configuration required by your environment. You can also control which exchange clients are enabled via the `ENABLED_CLIENTS` variable. If you need to route requests through a remote browser (e.g. to bypass geo‑restrictions), set `PLAYWRIGHT_WS` to the WebSocket URL of your Playwright server.

For the CoinGecko worker you may adjust `COINGECKO_RATE_LIMIT` to specify the number of requests allowed per minute (defaults to 50 if unset).

## Usage

Run the main script:

```bash
python main.py
```

The script fetches the latest market data for the pairs defined in your `.env` file and stores them in the configured BigQuery table.

If your environment requires a proxy with a custom SSL certificate, set `HTTPS_PROXY` and `SSL_CERT_FILE` in the `.env` file. The worker will use these values when creating the HTTP session.

## Running Tests

To execute the unit tests, run:

```bash
pytest
```
