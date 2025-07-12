import types
import pytest
from unittest import mock

import module_3_1


def test_alert_from_bigquery_summary(mocker):
    mock_client = mocker.MagicMock()
    mocker.patch("module_3_1.create_bq_client", return_value=mock_client)
    mocker.patch("module_3_1._get_row_count", side_effect=[10, 3])
    mock_query = mocker.MagicMock()
    mock_query.result.return_value = [types.SimpleNamespace(c=1)]
    mock_client.query.return_value = mock_query

    send_mock = mocker.patch("module_3_1.send_telegram_message")

    module_3_1.PROJECT_ID = "proj"
    module_3_1.DATASET = "dataset"
    module_3_1.TELEGRAM_BOT_TOKEN = "token"
    module_3_1.TELEGRAM_CHAT_ID = "chat"

    module_3_1.alert_from_bigquery()

    send_mock.assert_called_once()
    text = send_mock.call_args.args[2]
    assert "10 signaux" in text
    assert "3 anomalies" in text
    assert "1 incident" in text


