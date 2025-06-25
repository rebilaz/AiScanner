import aiohttp
import asyncio
import socket

async def main():
    print("Test de la connexion en forçant l'IPv4...")
    try:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            # On teste avec l'API de CoinGecko
            async with session.get('https://api.coingecko.com/api/v3/ping') as resp:
                print("--> Connexion réussie !")
                print("--> Réponse de l'API :", await resp.json())
    except Exception as e:
        print(f"--> Le test a échoué avec l'erreur : {e}")

asyncio.run(main())