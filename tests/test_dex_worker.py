import os
import pandas as pd
import pytest

from worker_dex import run_dex_worker
from dex_clients.thegraph import TheGraphClient
from gcp_utils import BigQueryClient
from google.cloud import bigquery


@pytest.mark.asyncio
async def test_run_dex_worker_uploads_dataframe(mocker):
    sample_swaps = [
        {
            "transaction": {"id": "0xabc"},
            "timestamp": "1625097600",
            "pool": {
                "token0": {"symbol": "ETH"},
                "token1": {"symbol": "USDT"},
            },
            "amount0": "1",
            "amount1": "-2500",
            "amountUSD": "2500",
            "sqrtPriceX96": "0",
        }
    ]

    mocker.patch.object(TheGraphClient, "fetch_swaps", mocker.AsyncMock(return_value=sample_swaps))
    mocker.patch.object(BigQueryClient, "ensure_dataset_exists")
    upload_mock = mocker.patch.object(BigQueryClient, "upload_dataframe")
    mocker.patch.object(bigquery, "Client")

    os.environ.update(
        {
            "GCP_PROJECT_ID": "proj",
            "BQ_DATASET": "dataset",
            "DEX_BIGQUERY_TABLE": "table",
            "THEGRAPH_UNISWAPV3_ENDPOINT": "http://example.com",
        }
    )

    await run_dex_worker()

    upload_mock.assert_called_once()
    df = upload_mock.call_args.args[0]
    assert isinstance(df, pd.DataFrame)
    assert set(
        [
            "ingestion_timestamp",
            "event_timestamp",
            "transaction_hash",
            "dex_source",
            "pair",
            "price_usd",
            "volume_usd",
            "token0_symbol",
            "token1_symbol",
            "amount0",
            "amount1",
        ]
    ).issubset(df.columns)

