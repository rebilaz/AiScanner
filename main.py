# main.py

import time
from worker_coingecko import run_coingecko_pipeline

def main():
    """
    Point d'entrée principal de l'application.
    Ce script orchestre l'exécution des différents workers.
    """
    print("==============================================")
    print("=      DÉMARRAGE DE L'ORCHESTRATEUR          =")
    print("==============================================")
    
    try:
        # Lancement du pipeline pour CoinGecko
        print("\n[ORCHESTRATEUR] Lancement du worker CoinGecko...")
        start_time = time.time()
        
        run_coingecko_pipeline() # Appel de la fonction importée
        
        end_time = time.time()
        duration = end_time - start_time
        print(f"[ORCHESTRATEUR] Worker CoinGecko terminé en {duration:.2f} secondes.")

        # --- Vous pourriez ajouter ici d'autres workers ---
        # print("\n[ORCHESTRATEUR] Lancement d'un autre worker...")
        # run_another_worker()
        
    except Exception as e:
        print(f"\n[ORCHESTRATEUR] Une erreur critique est survenue : {e}")
    finally:
        print("\n==============================================")
        print("=       FIN DE L'EXÉCUTION                   =")
        print("==============================================")


if __name__ == "__main__":
    main()