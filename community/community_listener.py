"""Asynchronous Discord and Telegram message listener storing data in BigQuery."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, List

import pandas as pd
import yaml
from discord import Intents, Client, Message
from telethon import TelegramClient, events

from gcp_utils import BigQueryClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_spam(text: str) -> bool:
    stripped = text.strip().lower()
    if len(stripped) < 3:
        return True
    if "http://" in stripped or "https://" in stripped:
        return True
    return False


async def upload_batch(
    bq_client: BigQueryClient,
    dataset: str,
    table: str,
    batch: List[Dict[str, Any]],
) -> None:
    df = pd.DataFrame(batch)
    await asyncio.to_thread(bq_client.upload_dataframe, df, dataset, table)


# ---------------------------------------------------------------------------
# Discord client
# ---------------------------------------------------------------------------

class DiscordListener(Client):
    def __init__(self, queue: asyncio.Queue, channel_ids: List[int]) -> None:
        intents = Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.queue = queue
        self.channel_ids = set(channel_ids)

    async def on_ready(self) -> None:
        logging.info("Discord connected as %s", self.user)

    async def on_message(self, message: Message) -> None:
        if message.author.bot:
            return
        if message.channel.id not in self.channel_ids:
            return
        if is_spam(message.content):
            return
        await self.queue.put(
            {
                "source": "discord",
                "author": str(message.author),
                "channel_id": str(message.channel.id),
                "text": message.content,
                "timestamp": message.created_at.isoformat(),
            }
        )


# ---------------------------------------------------------------------------
# Telegram client
# ---------------------------------------------------------------------------

class TelegramMonitor:
    def __init__(
        self,
        queue: asyncio.Queue,
        api_id: int,
        api_hash: str,
        bot_token: str,
        channels: List[str],
    ) -> None:
        self.queue = queue
        self.client = TelegramClient("bot", api_id, api_hash)
        self.bot_token = bot_token
        self.channels = channels

    async def start(self) -> None:
        await self.client.start(bot_token=self.bot_token)
        logging.info("Telegram client connected")
        self.client.add_event_handler(self._handler, events.NewMessage(chats=self.channels))
        await self.client.run_until_disconnected()

    async def _handler(self, event: events.NewMessage.Event) -> None:
        message = event.message
        if not message.message or is_spam(message.message):
            return
        await self.queue.put(
            {
                "source": "telegram",
                "author": str(message.sender_id),
                "channel_id": str(event.chat_id),
                "text": message.message,
                "timestamp": message.date.isoformat(),
            }
        )


# ---------------------------------------------------------------------------
# BigQuery worker
# ---------------------------------------------------------------------------

async def bq_worker(
    queue: asyncio.Queue,
    bq_client: BigQueryClient,
    dataset: str,
    table: str,
    batch_size: int,
) -> None:
    batch: List[Dict[str, Any]] = []
    while True:
        item = await queue.get()
        if item is None:
            break
        batch.append(item)
        queue.task_done()
        if len(batch) >= batch_size:
            await upload_batch(bq_client, dataset, table, batch)
            batch.clear()
    if batch:
        await upload_batch(bq_client, dataset, table, batch)


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

async def main() -> None:
    config_path = os.getenv("COMMUNITY_CONFIG_FILE", os.path.join(os.path.dirname(__file__), "config.yaml"))
    config = load_config(config_path)

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("BQ_COMMUNITY_TABLE", "raw_community_messages")
    if not project_id or not dataset or not table:
        logging.error("Missing BigQuery configuration")
        return

    bq_client = BigQueryClient(project_id)
    bq_client.ensure_dataset_exists(dataset)

    queue: asyncio.Queue = asyncio.Queue()
    batch_size = int(config.get("batch_size", 50))

    tasks = []

    discord_token = os.getenv("DISCORD_BOT_TOKEN")
    discord_channels = config.get("discord", {}).get("channels", [])
    if discord_token and discord_channels:
        discord_client = DiscordListener(queue, discord_channels)
        tasks.append(discord_client.start(discord_token))
    else:
        logging.warning("Discord client not configured")

    tg_api_id = os.getenv("TELEGRAM_API_ID")
    tg_api_hash = os.getenv("TELEGRAM_API_HASH")
    tg_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    telegram_channels = config.get("telegram", {}).get("channels", [])
    if tg_api_id and tg_api_hash and tg_bot_token and telegram_channels:
        telegram_monitor = TelegramMonitor(
            queue, int(tg_api_id), tg_api_hash, tg_bot_token, telegram_channels
        )
        tasks.append(telegram_monitor.start())
    else:
        logging.warning("Telegram client not configured")

    if not tasks:
        logging.error("No clients configured, exiting")
        return

    worker_task = asyncio.create_task(bq_worker(queue, bq_client, dataset, table, batch_size))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await queue.join()
        queue.put_nowait(None)
        await worker_task


if __name__ == "__main__":
    asyncio.run(main())
