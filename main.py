"""Application orchestrator."""
import time
import asyncio

from workers.worker_coingecko import run_coingecko_pipeline
from workers.worker_cex_aggregator import run as run_cex_worker


def main() -> None:
    """Entry point of the application."""
    print("==============================================")
    print("=      DÉMARRAGE DE L'ORCHESTRATEUR          =")
    print("==============================================")

    try:
        print("\n[ORCHESTRATEUR] Lancement du worker CEX...")
        asyncio.run(run_cex_worker())
        print("[ORCHESTRATEUR] Worker CEX terminé.")

        print("\n[ORCHESTRATEUR] Lancement du worker CoinGecko...")
        start_time = time.time()
        run_coingecko_pipeline()
        duration = time.time() - start_time
        print(f"[ORCHESTRATEUR] Worker CoinGecko terminé en {duration:.2f} secondes.")
    except Exception as exc:
        print(f"\n[ORCHESTRATEUR] Une erreur critique est survenue : {exc}")
    finally:
        print("\n==============================================")
        print("=       FIN DE L'EXÉCUTION                   =")
        print("==============================================")


if __name__ == "__main__":
    main()
