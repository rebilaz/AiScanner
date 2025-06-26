# tests/test_coingecko_client.py

import os
import sys
import json
import asyncio
import aiohttp
import pytest

# Permet d'importer les modules du répertoire parent (ex: `clients`)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from clients.coingecko import CoinGeckoClient


@pytest.mark.asyncio
async def test_token_details_success(mocker):
    """
    Vérifie le cas de succès où l'API retourne une réponse 200 OK.
    """
    # Préparation des mocks
    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    mock_resp = mocker.MagicMock()
    mock_resp.status = 200
    mock_resp.json = mocker.AsyncMock(return_value={"id": "bitcoin", "name": "Bitcoin"})
    
    # Mock du contexte asynchrone (async with)
    cm = mocker.MagicMock()
    cm.__aenter__ = mocker.AsyncMock(return_value=mock_resp)
    cm.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_session.get.return_value = cm
    
    client = CoinGeckoClient(mock_session, rate_limit=100)

    # Appel et assertion
    data = await client.token_details("bitcoin")
    assert data == {"id": "bitcoin", "name": "Bitcoin"}
    mock_session.get.assert_called_once()


@pytest.mark.asyncio
async def test_token_details_rate_limit_retry_success(mocker):
    """
    Vérifie que le client réessaye après un rate limit (429) et réussit au second appel.
    """
    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    
    # Mock de la réponse 429 (rate limit)
    resp_429 = mocker.MagicMock()
    resp_429.status = 429
    # On simule raise_for_status() pour déclencher la logique de retry
    resp_429.raise_for_status = mocker.MagicMock(side_effect=aiohttp.ClientResponseError(mocker.MagicMock(), ()))
    
    # Mock de la réponse 200 OK
    resp_ok = mocker.MagicMock()
    resp_ok.status = 200
    resp_ok.json = mocker.AsyncMock(return_value={"id": "bitcoin"})

    # Création des context managers pour chaque réponse
    cm_429 = mocker.MagicMock()
    cm_429.__aenter__ = mocker.AsyncMock(return_value=resp_429)
    cm_429.__aexit__ = mocker.AsyncMock(return_value=None)
    cm_ok = mocker.MagicMock()
    cm_ok.__aenter__ = mocker.AsyncMock(return_value=resp_ok)
    cm_ok.__aexit__ = mocker.AsyncMock(return_value=None)

    # Le premier appel retourne 429, le second retourne 200
    mock_session.get.side_effect = [cm_429, cm_ok]
    
    client = CoinGeckoClient(mock_session, rate_limit=100)

    # On mock asyncio.sleep pour que le test soit instantané
    mocker.patch("asyncio.sleep", mocker.AsyncMock())
    
    data = await client.token_details("bitcoin")
    
    assert data == {"id": "bitcoin"}
    assert mock_session.get.call_count == 2 # Vérifie qu'il y a bien eu deux appels


@pytest.mark.asyncio
async def test_token_details_server_error_fails_after_retries(mocker):
    """
    Vérifie que le client lève une RuntimeError après avoir échoué toutes les tentatives sur une erreur 503.
    """
    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    
    resp_503 = mocker.MagicMock()
    resp_503.status = 503
    resp_503.raise_for_status = mocker.MagicMock(side_effect=aiohttp.ClientResponseError(mocker.MagicMock(), ()))

    cm = mocker.MagicMock()
    cm.__aenter__ = mocker.AsyncMock(return_value=resp_503)
    cm.__aexit__ = mocker.AsyncMock(return_value=None)
    
    # La méthode get retournera toujours une erreur 503
    mock_session.get.return_value = cm

    # Le client est configuré pour 3 tentatives par défaut dans la méthode `get`
    client = CoinGeckoClient(mock_session, rate_limit=100)
    mocker.patch("asyncio.sleep", mocker.AsyncMock())

    # On vérifie qu'une RuntimeError est bien levée
    with pytest.raises(RuntimeError):
        await client.token_details("bitcoin")
    
    assert mock_session.get.call_count == 3 # Vérifie que les 3 tentatives ont eu lieu


@pytest.mark.asyncio
async def test_token_details_invalid_json_raises_exception(mocker):
    """
    Vérifie que le client lève une exception si la réponse est 200 OK mais que le corps n'est pas du JSON valide.
    """
    mock_session = mocker.MagicMock(spec=aiohttp.ClientSession)
    
    resp = mocker.MagicMock()
    resp.status = 200
    # On simule l'erreur que aiohttp lèverait en cas de JSON invalide
    resp.json = mocker.AsyncMock(side_effect=json.JSONDecodeError("Invalid JSON", "doc", 0))

    cm = mocker.MagicMock()
    cm.__aenter__ = mocker.AsyncMock(return_value=resp)
    cm.__aexit__ = mocker.AsyncMock(return_value=None)
    mock_session.get.return_value = cm

    client = CoinGeckoClient(mock_session, rate_limit=100)
    
    # On vérifie qu'une exception de décodage JSON est bien levée.
    # aiohttp peut aussi lever ContentTypeError si les headers sont incorrects.
    with pytest.raises((json.JSONDecodeError, aiohttp.ContentTypeError)):
        await client.token_details("bitcoin")