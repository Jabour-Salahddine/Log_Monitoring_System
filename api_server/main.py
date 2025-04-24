# api_server/main.py
import os
import traceback # Pour les erreurs détaillées
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import importlib.metadata # Pour vérifier la version

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
# Assurez-vous d'utiliser la bonne version alignée !
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError, ApiError

# Charger les variables d'environnement (.env)
load_dotenv()

# Lire la configuration depuis l'environnement ou utiliser des valeurs par défaut
ES_HOST_CONFIG = os.getenv("ELASTICSEARCH_HOST", "localhost:9200")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "app-logs-index-xampp")

# --- Initialisation du Client Elasticsearch (Module Level) ---
# Cette tentative initiale est conservée
es_client: Optional[Elasticsearch] = None # Annotation de type optionnelle
try:
    # Vérifier la version de la bibliothèque installée
    try:
        es_lib_version = importlib.metadata.version("elasticsearch")
        print(f"Elasticsearch library version: {es_lib_version}")
        # Idéalement, vérifier si elle correspond à ES_VERSION_TARGET = "8.7.0"
    except importlib.metadata.PackageNotFoundError:
        print("WARNING: Could not determine elasticsearch library version.")

    # Construire l'URL complète
    if not ES_HOST_CONFIG.startswith(('http://', 'https://')):
        connection_url = f"http://{ES_HOST_CONFIG}"
    else:
        connection_url = ES_HOST_CONFIG

    print(f"FastAPI attempting initial connection to Elasticsearch at {connection_url}...")

    # Utiliser la version alignée (ex: 8.7.0)
    temp_es_client = Elasticsearch(
        [connection_url],
        request_timeout=10 # Timeout plus court pour l'API
        # Si vous aviez ajouté headers=SIMPLE_HEADERS, assurez-vous qu'il est là si nécessaire
    )

    # Vérification avec ping() - Devrait fonctionner avec les versions alignées
    if temp_es_client.ping():
        es_client = temp_es_client # Assigner si succès
        print(">>> FastAPI initial Elasticsearch connection successful.")
    else:
        # Ne pas lever d'erreur ici, laisser startup_event réessayer
        print("--- FastAPI initial Elasticsearch ping failed. Will retry on startup.")
        es_client = None

# Gérer les erreurs de connexion spécifiques
except ESConnectionError as ce:
    print(f"!!! FastAPI initial connection failed (Connection Error): {ce}")
    es_client = None
# Gérer les erreurs API (comme le media_type si les versions ne sont pas alignées)
except ApiError as ae:
     print(f"!!! FastAPI initial connection failed (API Error): {ae}")
     es_client = None
except Exception as e:
    print(f"!!! FastAPI initial connection failed (Unexpected Error): {e}")
    traceback.print_exc()
    es_client = None
# --- Fin Initialisation Client Elasticsearch ---


# Créer l'application FastAPI
app = FastAPI(
    title="Log Monitoring API (XAMPP Source)",
    description="API for querying and analyzing application logs stored in Elasticsearch."
)

# Variable pour suivre l'état de la connexion ES
es_connection_status = "Unavailable (Initial check)"

# Événement au démarrage de l'application
@app.on_event("startup")
async def startup_event():
    # Déclarer les globales qui seront MODIFIÉES au début de la fonction
    global es_client
    global es_connection_status

    print("FastAPI startup event triggered...")

    # Vérifier si la connexion initiale (au niveau module) a réussi
    if es_client:
        # Vérifier à nouveau au cas où ES serait tombé entre temps
        try:
            if es_client.ping():
                es_connection_status = "Connected (from initial check)"
                print("Elasticsearch connection confirmed at FastAPI startup.")
                return # Pas besoin de réessayer
            else:
                print("Elasticsearch client existed but ping failed at startup. Retrying...")
                es_client = None # Forcer la tentative de reconnexion
        except Exception as ping_err:
             print(f"Error pinging existing Elasticsearch client at startup: {ping_err}. Retrying...")
             es_client = None # Forcer la tentative de reconnexion

    # Si la connexion initiale a échoué ou si le ping ci-dessus a échoué, on tente ici
    if not es_client:
        print("Attempting Elasticsearch connection retry during startup...")
        try:
            if not ES_HOST_CONFIG.startswith(('http://', 'https://')):
                connection_url = f"http://{ES_HOST_CONFIG}"
            else:
                connection_url = ES_HOST_CONFIG

            # Nouvelle tentative de connexion
            temp_es_client = Elasticsearch(
                [connection_url],
                request_timeout=10 # Peut être un peu plus long ici
                 # Ajoutez headers=SIMPLE_HEADERS ici aussi si vous l'aviez utilisé
                )

            if temp_es_client.ping():
                 # Modifier la variable globale es_client
                 es_client = temp_es_client
                 es_connection_status = "Connected (on startup retry)"
                 print(">>> Elasticsearch connection established successfully on startup retry.")
            else:
                 es_connection_status = "Failed (ping failed on retry)"
                 print("WARNING: Elasticsearch ping failed on startup retry.")
                 es_client = None # Assurer que c'est None si le retry échoue

        # Gérer les erreurs pendant la tentative de retry
        except ESConnectionError as ce_retry:
             es_connection_status = f"Failed (Connection Error on retry: {ce_retry})"
             print(f"WARNING: Elasticsearch connection retry failed: {ce_retry}")
             es_client = None
        except ApiError as ae_retry:
             es_connection_status = f"Failed (API Error on retry: {ae_retry})"
             print(f"WARNING: Elasticsearch connection retry failed: {ae_retry}")
             es_client = None
        except Exception as e_retry:
            es_connection_status = f"Failed (Error on retry: {e_retry})"
            print(f"WARNING: Elasticsearch connection retry failed: {e_retry}")
            traceback.print_exc()
            es_client = None


# Route racine pour vérifier l'état
@app.get("/", summary="API Root and Status Check")
async def read_root():
    # Vérifier l'état actuel de la connexion à chaque requête (plus fiable)
    current_es_status = "Unknown"
    if es_client:
        try:
            # Faire un ping rapide pour vérifier l'état actuel
            if es_client.ping():
                current_es_status = "Connected (Live Ping OK)"
            else:
                current_es_status = "Disconnected (Live Ping Failed)"
        except Exception as e:
            # print(f"Live ping check failed: {e}") # Optionnel : logger l'erreur de ping
            current_es_status = f"Disconnected (Live Ping Error: {type(e).__name__})"
    else:
        current_es_status = "Disconnected (Client unavailable)"

    return {
        "message": "Log Monitoring API is running.",
        "elasticsearch_connection_status_at_startup": es_connection_status, # Statut enregistré au démarrage
        "elasticsearch_current_status": current_es_status, # Statut vérifié maintenant
        "api_docs": "/docs"
        }

# Helper function pour obtenir le client ES (vérifie s'il existe)
# On ne fait plus le ping ici pour éviter la latence sur chaque requête API
# Les routes individuelles peuvent gérer les erreurs si ES tombe entre temps.
def get_es_client_checked():
    if not es_client:
        # Lever une erreur 503 Service Unavailable si le client n'a jamais été initialisé
        raise HTTPException(status_code=503, detail="Elasticsearch client not available. Check API startup logs.")
    return es_client

# Route pour rechercher des logs
@app.get("/logs", response_model=List[Dict[str, Any]], summary="Search Logs")
async def search_logs(
    term: Optional[str] = Query(None, description="Search term in 'message' field (case-insensitive)."),
    level: Optional[str] = Query(None, description="Filter by log level (e.g., ERROR, INFO). Case-insensitive.", regex="^(?i)(INFO|WARNING|ERROR|DEBUG|CRITICAL)$"),
    last_minutes: Optional[int] = Query(60, description="Search within the last N minutes.", ge=1),
    size: int = Query(100, description="Maximum number of logs to return.", ge=1, le=1000)
):
    """
    Search for logs in Elasticsearch based on specified criteria.
    Returns the most recent logs first.
    """
    try:
        es = get_es_client_checked() # Récupère le client s'il existe
    except HTTPException as http_exc:
         # Propager l'erreur 503 si le client n'est pas dispo
         raise http_exc

    query_body = {
        "size": size,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": {
            "bool": {
                "must": [],
                "filter": [
                     # Filtre de temps (obligatoire)
                    {"range": { "@timestamp": {
                        "gte": (datetime.utcnow() - timedelta(minutes=last_minutes)).isoformat(),
                        "lte": datetime.utcnow().isoformat(),
                        "format": "strict_date_optional_time||epoch_millis"
                    }}}
                ]
            }
        },
        "_source": True
    }

    # --- Ajout dynamique des filtres / requêtes ---
    if level:
        query_body["query"]["bool"]["filter"].append({
            "term": {"level.keyword": level.upper()} # Assumer mapping avec .keyword
        })
    if term:
        query_body["query"]["bool"]["must"].append({
            "match": {"message": {"query": term, "operator": "AND"}}
        })
    # Assurer une requête valide même sans terme/niveau spécifique
    if not query_body["query"]["bool"]["must"]:
        query_body["query"]["bool"]["must"].append({"match_all": {}})

    # --- Exécution de la requête ---
    try:
        response = es.search(
            index=ES_INDEX,
            body=query_body,
            ignore=[404] # Ignorer si l'index n'existe pas (retournera 0 hits)
            # Ne pas ignorer 400 ici, car cela cache des erreurs de requête
        )
        # Extraire les hits de manière plus sûre
        hits = response.get("hits", {}).get("hits", [])
        return [hit.get("_source") for hit in hits if hit.get("_source")]

    # Gérer les erreurs spécifiques d'Elasticsearch pendant la recherche
    except ESConnectionError as search_conn_err:
         print(f"Search failed due to ES connection error: {search_conn_err}")
         raise HTTPException(status_code=503, detail=f"Elasticsearch connection error during search: {search_conn_err}")
    except ApiError as search_api_err:
         print(f"Search failed due to ES API error: {search_api_err}")
         # Renvoyer une erreur 400 si c'est la faute du client, 500 sinon
         status_code = 400 if 400 <= search_api_err.status_code < 500 else 500
         raise HTTPException(status_code=status_code, detail=f"Elasticsearch API error during search: {search_api_err.message}")
    except Exception as e:
        print(f"Unexpected error during log search: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected error querying Elasticsearch: {str(e)}")


# Route pour obtenir des statistiques
@app.get("/stats/errors_per_hour", response_model=Dict[str, int], summary="Get Hourly Error Count")
async def get_error_stats(
    last_hours: int = Query(24, description="Analyze the last N hours.", ge=1)
):
    """
    Calculates the number of ERROR logs per hour for the specified duration.
    """
    try:
        es = get_es_client_checked()
    except HTTPException as http_exc:
         raise http_exc

    now = datetime.utcnow()
    start_time = now - timedelta(hours=last_hours)

    query_body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"level.keyword": "ERROR"}},
                    {"range": {"@timestamp": {
                        "gte": start_time.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time||epoch_millis"
                        }}}
                ]
            }
        },
        "aggs": {
            "errors_per_hour": {
                "date_histogram": {
                    "field": "@timestamp",
                    "calendar_interval": "hour",
                    "min_doc_count": 0,
                    "extended_bounds": {
                      "min": start_time.isoformat(),
                      "max": now.isoformat()
                    },
                    "format": "yyyy-MM-dd'T'HH:00:00Z" # Format de clé plus standard
                }
            }
        }
    }

    try:
        response = es.search(index=ES_INDEX, body=query_body, ignore=[404])

        # Extraction plus sûre des buckets d'agrégation
        buckets = response.get("aggregations", {}).get("errors_per_hour", {}).get("buckets", [])
        stats = { bucket["key_as_string"]: bucket["doc_count"] for bucket in buckets }
        return stats

    # Gestion des erreurs pour les statistiques
    except ESConnectionError as stats_conn_err:
         print(f"Stats query failed due to ES connection error: {stats_conn_err}")
         raise HTTPException(status_code=503, detail=f"Elasticsearch connection error during stats query: {stats_conn_err}")
    except ApiError as stats_api_err:
         print(f"Stats query failed due to ES API error: {stats_api_err}")
         status_code = 400 if 400 <= stats_api_err.status_code < 500 else 500
         raise HTTPException(status_code=status_code, detail=f"Elasticsearch API error during stats query: {stats_api_err.message}")
    except Exception as e:
        print(f"Unexpected error querying Elasticsearch statistics: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected error querying Elasticsearch statistics: {str(e)}")

# Note: Lancer avec uvicorn main:app --reload --port 8000
# Pensez à utiliser un environnement virtuel !