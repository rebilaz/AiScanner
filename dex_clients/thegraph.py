import logging
from typing import List, Dict

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from dotenv import load_dotenv


class TheGraphClient:
    """Asynchronous client to query The Graph subgraphs."""

    def __init__(self, endpoint: str) -> None:
        load_dotenv()
        self.endpoint = endpoint
        self.transport = AIOHTTPTransport(url=self.endpoint)
        self.logger = logging.getLogger(self.__class__.__name__)

    async def fetch_swaps(
        self,
        start_timestamp: int,
        end_timestamp: int,
        limit: int = 1000,
    ) -> List[Dict]:
        """Fetch swap events from Uniswap V3 between two timestamps."""
        query = gql(
            """
            query($start:Int!, $end:Int!, $first:Int!, $skip:Int!) {
              swaps(
                first: $first
                skip: $skip
                orderBy: timestamp
                orderDirection: desc
                where: {timestamp_gte: $start, timestamp_lt: $end}
              ) {
                transaction { id }
                timestamp
                pool { id token0 { symbol } token1 { symbol } }
                amount0
                amount1
                amountUSD
                sqrtPriceX96
              }
            }
            """
        )

        all_swaps: List[Dict] = []
        skip = 0
        async with Client(transport=self.transport, fetch_schema_from_transport=False) as session:
            while True:
                variables = {
                    "start": start_timestamp,
                    "end": end_timestamp,
                    "first": min(limit, 1000),
                    "skip": skip,
                }
                result = await session.execute(query, variable_values=variables)
                swaps = result.get("swaps", [])
                if not swaps:
                    break
                all_swaps.extend(swaps)
                if len(swaps) < min(limit, 1000):
                    break
                skip += len(swaps)
                if skip >= limit:
                    break
        self.logger.info("Fetched %d swaps from %s", len(all_swaps), self.endpoint)
        return all_swaps
