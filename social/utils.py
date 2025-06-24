from __future__ import annotations

import json
import logging
from typing import Dict, Any

import yaml


def load_config(path: str) -> Dict[str, Any]:
    """Load YAML or JSON configuration file."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            if path.endswith(('.yaml', '.yml')):
                return yaml.safe_load(fh)
            return json.load(fh)
    except FileNotFoundError:
        logging.error("Config file %s not found", path)
    except Exception as exc:  # pragma: no cover - parsing errors
        logging.exception("Failed to load config %s: %s", path, exc)
    return {}
