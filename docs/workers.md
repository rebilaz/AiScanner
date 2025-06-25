# Aperçu des Workers

Ce dépôt contient plusieurs *workers* chargés de collecter ou d'analyser des données avant de les stocker dans BigQuery. Le tableau ci‑dessous résume la tâche principale de chaque worker.

| Fichier | Description |
| ------- | ----------- |
| `worker_eth_sync.py` | Synchronise les tables publiques Ethereum de Google Cloud vers des tables BigQuery privées. |
| `worker_dex.py` | Récupère les swaps Uniswap V3 via TheGraph et les charge dans BigQuery. |
| `workers/worker_2_1.py` | Collecte les informations fondamentales des tokens depuis CoinGecko. |
| `workers/worker_2_2.py` | Analyse l'activité des dépôts GitHub liés aux projets et produit divers scores. |
| `workers/worker_2_3.py` | Calcule des métriques on‑chain (TVL, activité des baleines…) à partir des API *scan*. |
| `workers/worker_3_1.py` | Décode les logs Ethereum en événements nommés à l'aide des ABI et les stocke. |
| `workers/worker_3_2.py` | Identifie les « whales » et les adresses « smart money » en fonction des transferts de tokens. |
| `workers/worker_4_1.py` | Télécharge le code source vérifié des contrats depuis les explorateurs compatibles Etherscan. |
| `workers/worker_4_2.py` | Exécute l'outil Slither pour réaliser une analyse statique des contrats. |
| `workers/worker_4_3.py` | Applique un modèle de machine learning sur les opcodes des contrats pour détecter d'éventuelles failles. |
| `workers/worker_5_1.py` | Agrège les flux de liquidité des bridges entre chaînes et en calcule les montants en USD. |
| `workers/worker_5_2.py` | Évalue la TVL et les revenus des protocoles DeFi et les compare aux chiffres de DeFiLlama. |
| `workers/worker_6_3.py` | Produit un score global de classement des actifs à partir de métriques normalisées. |
| `workers/worker_7_1.py` | Mesure le sentiment social en analysant les messages publics et communautaires. |
| `workers/worker_7_2.py` | Génère des sujets récurrents dans les articles grâce au modèle BERTopic et suit leur prévalence. |
| `workers/worker_7_3.py` | Crée des résumés prudents d'articles via un LLM (GPT‑4) et les stocke pour validation humaine. |
| `workers/worker_7_4.py` | Suit l'évolution des collections NFT (volumes, hype, prix plancher) et déclenche au besoin des analyses de sécurité. |

Chaque worker peut être exécuté indépendamment selon les besoins, en fournissant les variables d'environnement adéquates (projet GCP, dataset BigQuery, clés API, etc.).
