"""Generic rule-based alerting engine using BigQuery."""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, Iterable, List

import pandas as pd
from google.cloud import bigquery
import yaml
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SENTIMENT_TABLE = os.getenv("BQ_SENTIMENT_TABLE", "social_sentiment_timeseries")
GITHUB_TABLE = os.getenv("BQ_GITHUB_TABLE", "github_score")
LABLED_ADDR_TABLE = os.getenv("BQ_LABELED_ADDRESSES_TABLE", "labeled_addresses")
CONFIG_FILE = os.getenv("ALERT_CONFIG_FILE", os.path.join(os.path.dirname(__file__), "alert_rules.yaml"))

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def load_config(path: str) -> Dict[str, Any]:
    """Load YAML configuration with alert rules."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logging.error("Alert config file %s not found", path)
        return {}


def send_alert(message: Dict[str, Any], channels: Dict[str, Any]) -> None:
    """Send alert message via configured channels."""
    slack_url = channels.get("slack_webhook_url") or os.getenv("SLACK_WEBHOOK_URL")
    if slack_url:
        try:
            resp = requests.post(slack_url, json=message, timeout=10)
            logging.info("Slack response: %s", resp.status_code)
        except Exception:  # pragma: no cover - network
            logging.exception("Failed to send Slack alert")

    webhook = channels.get("webhook_url")
    if webhook:
        try:
            requests.post(webhook, json=message, timeout=10)
        except Exception:  # pragma: no cover - network
            logging.exception("Failed to send webhook alert")

# ---------------------------------------------------------------------------
# Alert rules
# ---------------------------------------------------------------------------

def check_hype_vs_substance(client: bigquery.Client, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Detect projects with surging hype but stale GitHub activity."""
    hype_z = float(params.get("hype_z_threshold", 2.0))
    commit_days = int(params.get("commit_days_threshold", 14))

    query = f"""
        WITH daily AS (
            SELECT project,
                   DATE(timestamp) AS dt,
                   SUM(hype_score) AS hype_score
            FROM `{client.project}.{DATASET}.{SENTIMENT_TABLE}`
            GROUP BY project, dt
        ),
        stats AS (
            SELECT
                project,
                dt,
                LAST_VALUE(hype_score) OVER w AS current_hype,
                AVG(hype_score) OVER w AS avg_hype,
                STDDEV(hype_score) OVER w AS std_hype
            FROM daily
            WINDOW w AS (
                PARTITION BY project
                ORDER BY dt
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )
        ),
        latest_commit AS (
            SELECT id_projet AS project,
                   MAX(date_analyse) AS last_commit
            FROM `{client.project}.{DATASET}.{GITHUB_TABLE}`
            GROUP BY project
        ),
        latest_stats AS (
            SELECT s.project, s.current_hype, s.avg_hype, s.std_hype, l.last_commit
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY project ORDER BY dt DESC) AS rn
                FROM stats
            ) s
            JOIN latest_commit l ON s.project = l.project
            WHERE s.rn = 1
        )
        SELECT *
        FROM latest_stats
        WHERE std_hype IS NOT NULL
          AND current_hype > avg_hype + {hype_z} * std_hype
          AND last_commit < DATE_SUB(CURRENT_DATE(), INTERVAL {commit_days} DAY)
    """

    logging.info("Executing hype_vs_substance query")
    df = client.query(query).to_dataframe()
    alerts: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        alerts.append(
            {
                "project": row["project"],
                "rule": "hype_vs_substance",
                "current_hype": row["current_hype"],
                "avg_hype": row["avg_hype"],
                "std_hype": row["std_hype"],
                "last_commit": str(row["last_commit"]),
            }
        )
    return alerts


RULE_MAP: Dict[str, Callable[[bigquery.Client, Dict[str, Any]], List[Dict[str, Any]]]] = {
    "hype_vs_substance": check_hype_vs_substance,
}

# ---------------------------------------------------------------------------
# Main worker
# ---------------------------------------------------------------------------

def run_alert_engine() -> None:
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    config = load_config(CONFIG_FILE)
    if not config:
        logging.error("Empty alert configuration")
        return

    client = bigquery.Client(project=PROJECT_ID)
    rules = config.get("rules", [])
    for rule_conf in rules:
        name = rule_conf.get("name")
        func = RULE_MAP.get(name)
        if not func:
            logging.warning("Unknown rule %s", name)
            continue
        params = rule_conf.get("params", {})
        alerts = func(client, params)
        if alerts:
            logging.info("%d alerts for %s", len(alerts), name)
            channels = rule_conf.get("notifications", {})
            for alert in alerts:
                send_alert(alert, channels)
        else:
            logging.info("No alerts for %s", name)


if __name__ == "__main__":
    run_alert_engine()
