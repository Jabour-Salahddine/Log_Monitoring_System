'''
# api_server/main.py  : version1 : une seul table fonctionnelle : log (ERROR,WARNING, INFO)
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
    title="Log Monitoring API : Besmilah",
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
        "_source": ["@timestamp", "level", "message", "debezium_op"] #True
    }

    # --- Ajout dynamique des filtres / requêtes ---
    if level:
        query_body["query"]["bool"]["filter"].append({
            "term": {"level": level.upper()} # Assumer mapping avec .keyword
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
'''




# version 2 : deux tables : logs warning, error, info + action utilisateur au niveau de panier

# api_server/main.py
import os
import traceback # Pour les erreurs détaillées
from datetime import datetime, timedelta, timezone # Ajout timezone
from typing import List, Optional, Dict, Any
import importlib.metadata # Pour vérifier la version

from dotenv import load_dotenv # type: ignore
from fastapi import FastAPI, HTTPException, Query, Path # type: ignore # Ajout Path
from pydantic import BaseModel, Field # type: ignore # Ajout Pydantic pour les modèles de réponse

# Assurez-vous d'utiliser la bonne version alignée !
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError, ApiError # type: ignore

# Charger les variables d'environnement (.env)
# Assurez-vous que ce fichier existe dans le dossier api_server
load_dotenv()

# --- Configuration ---
# Lire la configuration depuis l'environnement ou utiliser des valeurs par défaut
ES_HOST_CONFIG = os.getenv("ELASTICSEARCH_HOST", "localhost:9200")
# Index pour les logs (existant)
ES_LOG_INDEX = os.getenv("ELASTICSEARCH_LOG_INDEX", "app-logs-index-xampp")
# NOUVEAU: Index pour les événements panier - Assurez-vous de l'ajouter à votre .env !
ES_CART_INDEX = os.getenv("ELASTICSEARCH_CART_INDEX", "cart-events-index")

# --- Initialisation du Client Elasticsearch (Module Level) ---
es_client: Optional[Elasticsearch] = None
es_lib_version = "N/A"
try:
    try:
        es_lib_version = importlib.metadata.version("elasticsearch")
        print(f"Elasticsearch library version: {es_lib_version}")
    except importlib.metadata.PackageNotFoundError:
        print("WARNING: Could not determine elasticsearch library version.")

    if not ES_HOST_CONFIG.startswith(('http://', 'https://')):
        connection_url = f"http://{ES_HOST_CONFIG}"
    else:
        connection_url = ES_HOST_CONFIG

    print(f"FastAPI attempting initial connection to Elasticsearch at {connection_url}...")
    temp_es_client = Elasticsearch([connection_url], request_timeout=10)

    if temp_es_client.ping():
        es_client = temp_es_client
        print(">>> FastAPI initial Elasticsearch connection successful.")
    else:
        print("--- FastAPI initial Elasticsearch ping failed. Will retry on startup.")
        es_client = None

except ESConnectionError as ce:
    print(f"!!! FastAPI initial connection failed (Connection Error): {ce}")
    es_client = None
except ApiError as ae:
    print(f"!!! FastAPI initial connection failed (API Error - check version alignment?): {ae}")
    es_client = None
except Exception as e:
    print(f"!!! FastAPI initial connection failed (Unexpected Error): {e}")
    traceback.print_exc()
    es_client = None
# --- Fin Initialisation Client Elasticsearch ---

# Créer l'application FastAPI
app = FastAPI(
    title="Log & Cart Event Monitoring API : Besmilah",
    description="API for querying application logs and cart events stored in Elasticsearch."
)

# Variable pour suivre l'état de la connexion ES au démarrage
es_connection_status_startup = "Unavailable (Initial check)"

# Événement au démarrage de l'application (Retry Logic)
@app.on_event("startup")
async def startup_event():
    global es_client
    global es_connection_status_startup # Utiliser la variable spécifique au démarrage

    print("FastAPI startup event triggered...")

    if es_client:
        try:
            if es_client.ping():
                es_connection_status_startup = "Connected (from initial check)"
                print("Elasticsearch connection confirmed at FastAPI startup.")
                return
            else:
                print("Elasticsearch client existed but ping failed at startup. Retrying...")
                es_client = None
        except Exception as ping_err:
            print(f"Error pinging existing Elasticsearch client at startup: {ping_err}. Retrying...")
            es_client = None

    if not es_client:
        print("Attempting Elasticsearch connection retry during startup...")
        try:
            if not ES_HOST_CONFIG.startswith(('http://', 'https://')):
                connection_url = f"http://{ES_HOST_CONFIG}"
            else:
                connection_url = ES_HOST_CONFIG

            temp_es_client = Elasticsearch([connection_url], request_timeout=15)

            if temp_es_client.ping():
                es_client = temp_es_client
                es_connection_status_startup = "Connected (on startup retry)"
                print(">>> Elasticsearch connection established successfully on startup retry.")
            else:
                es_connection_status_startup = "Failed (ping failed on retry)"
                print("WARNING: Elasticsearch ping failed on startup retry.")
                es_client = None
        except Exception as e_retry:
            error_type = type(e_retry).__name__
            es_connection_status_startup = f"Failed ({error_type} on retry)"
            print(f"WARNING: Elasticsearch connection retry failed: {e_retry}")
            if not isinstance(e_retry, (ESConnectionError, ApiError)):
                 traceback.print_exc()
            es_client = None

# Helper function pour obtenir le client ES (vérifie s'il existe à chaque appel)
# Lève une HTTPException 503 si le client n'est pas disponible
def get_es_client_checked():
    if not es_client:
        raise HTTPException(status_code=503, detail="Elasticsearch client not available. Check API startup logs and ES status.")
    # Faire un ping rapide avant de retourner le client pour vérifier la connexion live
    try:
        if not es_client.ping():
             raise HTTPException(status_code=503, detail="Elasticsearch client available but failed live ping check.")
    except ESConnectionError as ping_err:
         raise HTTPException(status_code=503, detail=f"Elasticsearch connection error during live ping check: {ping_err}")
    except Exception as ping_e: # Autres erreurs potentielles de ping
         raise HTTPException(status_code=500, detail=f"Unexpected error during Elasticsearch live ping check: {ping_e}")
    return es_client


# --- Modèles Pydantic pour les Événements Panier (NOUVEAU) ---
# Définir des modèles pour structurer la réponse de l'API pour les événements panier

class CartEventNestedEvent(BaseModel):
    id: Optional[int] = None
    type: Optional[str] = None
    source: Optional[str] = None
    debezium_op: Optional[str] = Field(None, alias="debezium_op")

class CartEventNestedUser(BaseModel):
    id: Optional[str] = None

class CartEventNestedSession(BaseModel):
    id: Optional[str] = None

class CartEventNestedProduct(BaseModel):
    id: Optional[str] = None
    unit_price: Optional[float] = Field(None, alias="unit_price")

class CartEventNestedCart(BaseModel):
    quantity: Optional[int] = None

class CartEventDetails(BaseModel):
    # Ce modèle peut être adapté si vous avez des champs spécifiques dans 'details'
    raw: Optional[Any] = None # Pour les cas où 'details' n'est pas un JSON valide
    coupon_code: Optional[str] = Field(None, alias="coupon_code") # Exemple

class CartEventResponse(BaseModel):
    timestamp: datetime = Field(..., alias="@timestamp")
    event: CartEventNestedEvent
    user: CartEventNestedUser
    session: CartEventNestedSession
    product: CartEventNestedProduct
    cart: CartEventNestedCart
    details: Optional[Dict[str, Any]] = None # Accepter un dict flexible pour 'details'

class ProductSummary(BaseModel):
     doc_count: int
     # Ajoutez d'autres métriques si vous avez des sous-agrégations
     # event_type_counts: Optional[Dict[str, int]] = None

class ProductAggregationResponse(BaseModel):
     products: Dict[str, ProductSummary] = Field(..., description="Summary statistics per product ID")


# --- Routes API ---

# Route racine pour vérifier l'état
@app.get("/", summary="API Root and Status Check", tags=["Status"])
async def read_root():
    # Vérifier l'état live pour la réponse
    current_es_status = "Unknown"
    if es_client:
        try:
            if es_client.ping():
                current_es_status = "Connected (Live Ping OK)"
            else:
                current_es_status = "Disconnected (Live Ping Failed)"
        except Exception:
            current_es_status = "Disconnected (Live Ping Error)"
    else:
        current_es_status = "Disconnected (Client unavailable)"

    return {
        "message": "Log & Cart Event Monitoring API is running.",
        "elasticsearch_connection_status_at_startup": es_connection_status_startup,
        "elasticsearch_current_status": current_es_status,
        "elasticsearch_library_version": es_lib_version,
        "target_log_index": ES_LOG_INDEX,
        "target_cart_event_index": ES_CART_INDEX, # Indiquer l'index panier cible
        "api_docs": "/docs"
    }

# --- Endpoints pour les LOGS (EXISTANTS, légèrement adaptés) ---

@app.get("/logs", response_model=List[Dict[str, Any]], summary="Search Logs", tags=["Logs"])
async def search_logs(
    term: Optional[str] = Query(None, description="Search term in 'message' field (case-insensitive)."),
    level: Optional[str] = Query(None, description="Filter by log level (e.g., ERROR, INFO). Case-insensitive.", regex="^(?i)(INFO|WARNING|ERROR|DEBUG|CRITICAL)$"),
    last_minutes: Optional[int] = Query(60, description="Search within the last N minutes.", ge=1),
    size: int = Query(10, description="Maximum number of logs to return.", ge=1, le=1000), # Réduit la taille par défaut
    from_: int = Query(0, description="Offset for pagination.", ge=0, alias="from") # Ajout pagination
):
    """ Search for logs in Elasticsearch based on specified criteria. """
    es = get_es_client_checked() # Vérifie la connexion à chaque appel

    filters = []
    must_queries = []

    # Filtre de temps obligatoire
    time_filter = {"range": { "@timestamp": {
        "gte": f"now-{last_minutes}m/m", # Utiliser date math
        "lte": "now/m",
        "format": "strict_date_optional_time||epoch_millis"
    }}}
    filters.append(time_filter)

    if level:
        filters.append({"term": {"level": level.upper()}}) # Utilise le champ keyword implicite
    if term:
        must_queries.append({"match": {"message": {"query": term, "operator": "AND"}}})

    # Si aucun terme de recherche, utiliser match_all
    if not must_queries:
        must_queries.append({"match_all": {}})

    query_body = {
        "size": size,
        "from": from_,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": { "bool": { "must": must_queries, "filter": filters } },
        "_source": ["@timestamp", "level", "message", "source_db_id", "debezium_op"] # Adapter les champs si besoin
    }

    try:
        response = es.search(index=ES_LOG_INDEX, body=query_body, ignore=[404])
        hits = response.get("hits", {}).get("hits", [])
        # Retourner le _source directement
        return [hit.get("_source") for hit in hits if hit.get("_source")]
    except (ESConnectionError, ApiError, HTTPException) as e: # Attraper aussi HTTPException de get_es_client_checked
        # Log l'erreur serveur
        print(f"Error during log search: {e}")
        if isinstance(e, HTTPException): raise e # Renvoyer l'erreur HTTP déjà formatée
        status_code = 503 if isinstance(e, ESConnectionError) else 500
        raise HTTPException(status_code=status_code, detail=f"Error querying Elasticsearch for logs: {str(e)}")
    except Exception as e:
        print(f"Unexpected error during log search: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error during log search.")


@app.get("/stats/logs/errors_per_hour", response_model=Dict[str, int], summary="Get Hourly Error Log Count", tags=["Logs"])
async def get_log_error_stats(
    last_hours: int = Query(24, description="Analyze the last N hours.", ge=1)
):
    """ Calculates the number of ERROR logs per hour for the specified duration. """
    es = get_es_client_checked()
    now_utc = datetime.now(timezone.utc)
    start_time_utc = now_utc - timedelta(hours=last_hours)

    query_body = {
        "size": 0,
        "query": { "bool": { "filter": [
            {"term": {"level": "ERROR"}}, # Utiliser 'level' directement si c'est keyword
            {"range": {"@timestamp": {
                "gte": start_time_utc.isoformat(timespec='seconds'),
                "lte": now_utc.isoformat(timespec='seconds'),
                "format": "strict_date_optional_time||epoch_millis"
            }}}
        ]}},
        "aggs": { "errors_per_hour": { "date_histogram": {
            "field": "@timestamp",
            "calendar_interval": "hour",
            "min_doc_count": 0, # Inclure les heures sans erreur
            "extended_bounds": {
                "min": start_time_utc.isoformat(timespec='seconds'),
                "max": now_utc.isoformat(timespec='seconds')
            },
            "format": "yyyy-MM-dd'T'HH:mm:ss'Z'" # Format ISO8601 UTC
        }}}
    }

    try:
        response = es.search(index=ES_LOG_INDEX, body=query_body, ignore=[404])
        buckets = response.get("aggregations", {}).get("errors_per_hour", {}).get("buckets", [])
        # Retourner { "timestamp_heure": count }
        return { bucket["key_as_string"]: bucket["doc_count"] for bucket in buckets }
    except (ESConnectionError, ApiError, HTTPException) as e:
        print(f"Error during log stats query: {e}")
        if isinstance(e, HTTPException): raise e
        status_code = 503 if isinstance(e, ESConnectionError) else 500
        raise HTTPException(status_code=status_code, detail=f"Error querying Elasticsearch for log stats: {str(e)}")
    except Exception as e:
        print(f"Unexpected error during log stats query: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error during log stats query.")


# --- Endpoints pour les ÉVÉNEMENTS PANIER (NOUVEAU) ---

@app.get("/cart-events", response_model=List[CartEventResponse], summary="Search Cart Events", tags=["Cart Events"])
async def search_cart_events(
    user_id: Optional[str] = Query(None, description="Filter by user ID."),
    product_id: Optional[str] = Query(None, description="Filter by product ID."),
    event_type: Optional[str] = Query(None, description="Filter by event type (e.g., ADD_TO_CART). Case-insensitive."),
    last_minutes: Optional[int] = Query(60, description="Search within the last N minutes.", ge=1),
    start_date: Optional[datetime] = Query(None, description="Start date/time (ISO format). Overrides last_minutes if set."),
    end_date: Optional[datetime] = Query(None, description="End date/time (ISO format). Defaults to now if start_date is set."),
    size: int = Query(10, description="Maximum number of events to return.", ge=1, le=1000),
    from_: int = Query(0, description="Offset for pagination.", ge=0, alias="from")
):
    """
    Search for cart events in Elasticsearch based on specified criteria.
    Returns the most recent events first by default.
    """
    es = get_es_client_checked() # Vérifie la connexion

    filters = []

    # Gestion du filtre de temps
    now_utc = datetime.now(timezone.utc)
    if start_date:
        gte_time = start_date.astimezone(timezone.utc) # Assurer UTC
        lte_time = (end_date or now_utc).astimezone(timezone.utc)
        time_filter = {"range": {"@timestamp": {
            "gte": gte_time.isoformat(timespec='milliseconds'),
            "lte": lte_time.isoformat(timespec='milliseconds'),
            "format": "strict_date_optional_time_nanos||strict_date_optional_time||epoch_millis" # Format plus flexible
        }}}
    else:
        # Utiliser last_minutes si start_date n'est pas fourni
        time_filter = {"range": {"@timestamp": {
            "gte": f"now-{last_minutes}m/m",
            "lte": "now/m"
        }}}
    filters.append(time_filter)

    # Ajouter les filtres optionnels (basés sur le mapping défini dans tasks.py)
    if user_id:
        filters.append({"term": {"user.id": user_id}}) # Chemin exact du champ keyword
    if product_id:
        filters.append({"term": {"product.id": product_id}}) # Chemin exact
    if event_type:
        # Assumer que event.type est un champ keyword
        filters.append({"term": {"event.type": event_type.upper()}}) # Normaliser en majuscules comme dans la tâche Celery

    query_body = {
        "size": size,
        "from": from_,
        "sort": [{"@timestamp": {"order": "desc"}}], # Trier par défaut par date décroissante
        "query": {
            "bool": {
                "filter": filters # Utiliser 'filter' pour les recherches exactes (plus performant)
            }
        }
        # Pas besoin de _source car on utilise le modèle Pydantic qui sélectionne
    }

    try:
        response = es.search(
            index=ES_CART_INDEX, # Utiliser l'index des événements panier !
            body=query_body,
            ignore=[404] # Gérer si l'index n'existe pas encore
        )
        hits = response.get("hits", {}).get("hits", [])
        # Valider et retourner les données via le modèle Pydantic
        # Note: Pydantic gère le renommage via alias et la validation de type
        return [CartEventResponse(**hit.get("_source", {})) for hit in hits if hit.get("_source")]
    except (ESConnectionError, ApiError, HTTPException) as e:
        print(f"Error during cart event search: {e}")
        if isinstance(e, HTTPException): raise e
        status_code = 503 if isinstance(e, ESConnectionError) else 500
        raise HTTPException(status_code=status_code, detail=f"Error querying Elasticsearch for cart events: {str(e)}")
    except Exception as e:
        print(f"Unexpected error during cart event search: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error during cart event search.")


@app.get("/stats/cart-events/by_product", response_model=ProductAggregationResponse, summary="Get Cart Event Summary per Product", tags=["Cart Events"])
async def get_cart_event_stats_by_product(
    event_type: Optional[str] = Query(None, description="Filter by event type (e.g., ADD_TO_CART). Case-insensitive."),
    last_hours: int = Query(24, description="Analyze the last N hours.", ge=1)
):
    """
    Calculates the number of cart events per product ID for the specified duration.
    Optionally filters by event type.
    """
    es = get_es_client_checked()
    now_utc = datetime.now(timezone.utc)
    start_time_utc = now_utc - timedelta(hours=last_hours)

    filters = []
    # Filtre de temps obligatoire
    filters.append({"range": {"@timestamp": {
        "gte": start_time_utc.isoformat(timespec='seconds'),
        "lte": now_utc.isoformat(timespec='seconds'),
        "format": "strict_date_optional_time||epoch_millis"
    }}})

    # Filtre optionnel sur le type d'événement
    if event_type:
        filters.append({"term": {"event.type": event_type.upper()}})

    query_body = {
        "size": 0, # On ne veut pas les documents, seulement les agrégations
        "query": { "bool": { "filter": filters } },
        "aggs": {
            "products": { # Nom de l'agrégation
                "terms": {
                    "field": "product.id", # Agréger sur le champ keyword product.id
                    "size": 100 # Nombre maximum de produits à retourner dans le sommaire
                }
                # Ajoutez ici des sous-agrégations si nécessaire :
                # ,"aggs": {
                #     "total_quantity": { "sum": { "field": "cart.quantity" } }
                # }
            }
        }
    }

    try:
        response = es.search(
             index=ES_CART_INDEX, # Utiliser l'index des événements panier !
             body=query_body,
             ignore=[404]
        )
        buckets = response.get("aggregations", {}).get("products", {}).get("buckets", [])
        # Formater la réponse selon le modèle Pydantic
        product_summaries = {
             bucket["key"]: ProductSummary(doc_count=bucket["doc_count"])
             for bucket in buckets
        }
        return ProductAggregationResponse(products=product_summaries)

    except (ESConnectionError, ApiError, HTTPException) as e:
        print(f"Error during cart stats query: {e}")
        if isinstance(e, HTTPException): raise e
        status_code = 503 if isinstance(e, ESConnectionError) else 500
        raise HTTPException(status_code=status_code, detail=f"Error querying Elasticsearch for cart stats: {str(e)}")
    except Exception as e:
        print(f"Unexpected error during cart stats query: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error during cart stats query.")

# --- Lancement (pour développement local) ---
# Pour lancer: uvicorn api_server.main:app --reload --port 8000
# Assurez-vous d'avoir un fichier .env dans le même dossier que main.py
# avec ELASTICSEARCH_HOST, ELASTICSEARCH_LOG_INDEX, ELASTICSEARCH_CART_INDEX

