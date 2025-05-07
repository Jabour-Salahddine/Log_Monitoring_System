
# log_processor/tasks.py    version 2
import json
import datetime
import time
import traceback
import logging

# Importer Celery, clients et exceptions
from .celery_app import app # Importez l'instance Celery depuis celery_app.py
from celery import shared_task # type: ignore
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError, ApiError # type: ignore
import redis # type: ignore

# Importer la configuration locale
from . import config # Contient les noms d'index, hôtes, etc.

# Configuration du logging (devrait être centralisée idéalement, mais ok ici pour l'instant)
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# Utiliser getLogger pour éviter la réinitialisation par basicConfig si déjà configuré ailleurs
task_logger = logging.getLogger('celery.tasks') # Logger parent pour les tâches
if not task_logger.handlers: # Appliquer la config seulement si pas déjà fait
    logging.basicConfig(level=logging.INFO, format=log_format)


# --- Initialisation des Clients (au niveau module, partagé par les workers) ---
es_client: Elasticsearch | None = None
redis_client_anomaly: redis.Redis | None = None # Pour la détection d'anomalie LOGS

# --- Constantes pour la tentative de connexion ES ---
MAX_ES_CONNECTION_ATTEMPTS = 5
ES_CONNECTION_RETRY_DELAY_SECONDS = 5

# --- Bloc de Connexion Elasticsearch (AVEC RETRIES et test INFO()) ---
connection_url_es = f"http://{config.ELASTICSEARCH_HOST}:9200"
task_logger.info(f"Worker initializing ES connection to {connection_url_es}...")
for attempt_es in range(1, MAX_ES_CONNECTION_ATTEMPTS + 1):
    try:
        task_logger.info(f"ES Connection attempt {attempt_es}/{MAX_ES_CONNECTION_ATTEMPTS}...")
        # Augmenter légèrement le timeout pour l'initialisation
        temp_es_client = Elasticsearch([connection_url_es], request_timeout=30, retry_on_timeout=True)
        task_logger.info(f"Attempt {attempt_es}: Checking ES cluster info...")
        cluster_info = temp_es_client.info()
        if cluster_info and 'version' in cluster_info:
            es_client = temp_es_client
            task_logger.info(f">>> Worker successfully connected to Elasticsearch. Version: {cluster_info['version']['number']}")
            break
        else:
            task_logger.warning(f"Attempt {attempt_es}: Received unexpected response from ES info(): {cluster_info}")
    except Exception as e_es:
        task_logger.error(f"!!! Attempt {attempt_es}: Error during ES connection/info check: {e_es}", exc_info=False) # exc_info=False pour moins de bruit initial

    if attempt_es < MAX_ES_CONNECTION_ATTEMPTS:
        task_logger.info(f"    Retrying ES connection in {ES_CONNECTION_RETRY_DELAY_SECONDS} seconds...")
        time.sleep(ES_CONNECTION_RETRY_DELAY_SECONDS)
    else:
        task_logger.critical(f"!!! CRITICAL: Worker failed to connect to Elasticsearch after {MAX_ES_CONNECTION_ATTEMPTS} attempts.")
# --- Fin Bloc Connexion Elasticsearch ---

# --- Bloc de Connexion Redis pour la détection d'anomalie LOGS ---
# Note: Le client Redis Anomaly (DB 2) n'est PAS utilisé par la tâche cart_events
try:
    redis_host_anomaly = config.REDIS_HOST
    redis_port_anomaly = config.REDIS_PORT
    task_logger.info(f"Worker connecting to Redis (anomaly detection - DB 2) at {redis_host_anomaly}:{redis_port_anomaly}...")
    redis_client_anomaly = redis.Redis(host=redis_host_anomaly, port=redis_port_anomaly, db=2, decode_responses=True, socket_connect_timeout=5)
    redis_client_anomaly.ping()
    task_logger.info(">>> Worker successfully connected to Redis (anomaly detection - DB 2)")
except Exception as e_redis:
    task_logger.error(f"!!! Worker error connecting to Redis (anomaly detection - DB 2): {e_redis}", exc_info=True)
    redis_client_anomaly = None # Important de le mettre à None si échec
# --- Fin Bloc Connexion Redis Anomaly ---

# --- Vérification/Création des Index Elasticsearch ---
if es_client:
    # -- 1. Index des LOGS --
    try:
        log_index_name = config.ELASTICSEARCH_LOG_INDEX # <- Assurez-vous que cette variable existe dans config.py
        task_logger.info(f"Worker checking/creating LOG index '{log_index_name}'...")
        if not es_client.indices.exists(index=log_index_name):
            task_logger.info(f"LOG Index '{log_index_name}' not found. Attempting creation...")
            # Mapping simple pour les logs
            log_mapping = {
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "level": {"type": "keyword"},
                        "message": {"type": "text"},
                        #"source_db_id": {"type": "keyword"}, # Utiliser keyword pour les IDs
                        #"pipeline_step": {"type": "keyword"},
                        "debezium_op": {"type": "keyword"},
                        #"anomaly_detected": {"type": "boolean"}
                    }
                }
            }
            # ignore=[400] pour 'resource_already_exists_exception' si un autre worker le crée en même temps
            es_client.indices.create(index=log_index_name, body=log_mapping, ignore=[400])
            task_logger.info(f"LOG Index '{log_index_name}' should now exist.")
        else:
            task_logger.info(f"LOG Index '{log_index_name}' already exists.")
    except Exception as index_error:
        task_logger.error(f"!!! Worker error checking or creating LOG index '{config.ELASTICSEARCH_LOG_INDEX}': {index_error}", exc_info=True)

    # -- 2. Index des ÉVÉNEMENTS PANIER -- (NOUVEAU BLOC) --
    try:
        # ===> ASSUREZ-VOUS QUE config.ELASTICSEARCH_CART_INDEX EST DÉFINI DANS config.py <===
        cart_index_name = config.ELASTICSEARCH_CART_INDEX
        task_logger.info(f"Worker checking/creating CART index '{cart_index_name}'...")
        if not es_client.indices.exists(index=cart_index_name):
            task_logger.info(f"CART Index '{cart_index_name}' not found. Attempting creation...")
            # Définir un mapping pertinent pour les événements panier
            cart_mapping = {
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "event": {
                            "properties": {
                                "id": {"type": "integer"}, # ID de l'événement dans la DB
                                "type": {"type": "keyword"}, # ADD_TO_CART, REMOVE_FROM_CART, etc.
                                "source": {"type": "keyword", "index": False}, # Ex: 'database', pas besoin d'indexer si toujours pareil
                                "debezium_op": {"type": "keyword"}
                            }
                        },
                        "user": {
                            "properties": { "id": {"type": "keyword"} } # ID utilisateur
                        },
                        "session": {
                             "properties": { "id": {"type": "keyword"} } # ID de session
                        },
                        "product": {
                             "properties": {
                                 "id": {"type": "keyword"}, # ID/SKU produit
                                 "unit_price": {"type": "float"} # Prix
                             }
                        },
                        "cart": {
                             "properties": { "quantity": {"type": "integer"} } # Quantité modifiée/ajoutée
                        },
                        # Si vous avez une colonne 'details' JSON, vous pouvez la mapper
                        # "details": {"type": "object", "enabled": False} # Désactiver l'indexation du contenu JSON brut
                        # Ou mapper des champs spécifiques si connus :
                        "details": {
                            "properties": {
                                "coupon_code": {"type": "keyword"} # Exemple si 'details' contient un coupon
                            }
                        }
                    }
                }
            }
            es_client.indices.create(index=cart_index_name, body=cart_mapping, ignore=[400])
            task_logger.info(f"CART Index '{cart_index_name}' should now exist.")
        else:
            task_logger.info(f"CART Index '{cart_index_name}' already exists.")
    except AttributeError:
         task_logger.error(f"!!! Worker error: 'config.ELASTICSEARCH_CART_INDEX' is likely MISSING in config.py!")
    except Exception as index_error:
        task_logger.error(f"!!! Worker error checking or creating CART index '{config.ELASTICSEARCH_CART_INDEX}': {index_error}", exc_info=True)

else:
    task_logger.warning("!!! Worker skipping ALL index checks because Elasticsearch client is not available.")
# --- Fin Vérification/Création Index ---


# --- Tâche Celery pour les LOGS (EXISTANTE) ---
@app.task(bind=True, name='log_processor.process_log_event', max_retries=3, default_retry_delay=60, queue='logs')
def process_log_event(self, event_data_bytes):
    """Traite un événement de log brut reçu de Kafka (en bytes)."""
    task_logger.info(f"Task {self.request.id} [LOG]: Received event data ({len(event_data_bytes)} bytes) on queue 'logs'.")

    # Vérifier les clients requis pour CETTE tâche au début. Essentiel !
    if not es_client or not redis_client_anomaly: # <- Note: redis_client_anomaly est spécifique aux logs ici
        current_es_status = "OK" if es_client else "Unavailable"
        current_redis_status = "OK" if redis_client_anomaly else "Unavailable"
        task_logger.error(f"Task {self.request.id} [LOG]: Prerequisite client missing (ES: {current_es_status}, Redis Anomaly: {current_redis_status}). Retrying task...")
        # Lever une exception pour que Celery gère la relance
        raise self.retry(exc=ConnectionRefusedError("LOG Task: ES or Redis Anomaly client not available in worker"), countdown=60 * (self.request.retries + 1))

    try:
        # --- Décodage et Parsing JSON ---
        event_data_str = event_data_bytes.decode('utf-8')
        message = json.loads(event_data_str)
        payload = message.get('payload')

        # --- Validation Payload et Opération ---
        if not payload:
            task_logger.warning(f"Task {self.request.id} [LOG]: Skipping event without payload")
            return "Skipped: No payload"
        op = payload.get('op')
        if op == 'd':
            task_logger.info(f"Task {self.request.id} [LOG]: Skipping delete operation")
            return "Skipped: Delete operation"
        log_data = payload.get('after') if op in ['c', 'u', 'r'] else payload # 'r' pour read (snapshots?)
        if not log_data:
            task_logger.warning(f"Task {self.request.id} [LOG]: Skipping event without log data in 'after' (op='{op}')")
            return "Skipped: No log data in 'after'"

        # --- Formatage Timestamp (Logique précédente) ---
        # (Votre logique de timestamp existante ici...)
        ts_raw = log_data.get('log_time')
        debezium_ts_ms = payload.get('ts_ms')
        timestamp_iso = None
        try:
            if ts_raw:
                if isinstance(ts_raw, str):
                    # Essayer format ISO ou format spécifique
                     try: dt_obj = datetime.datetime.fromisoformat(ts_raw.replace(' ', 'T'))
                     except ValueError: dt_obj = datetime.datetime.strptime(ts_raw, '%Y-%m-%d %H:%M:%S')
                elif isinstance(ts_raw, (int, float)): dt_obj = datetime.datetime.fromtimestamp(ts_raw, tz=datetime.timezone.utc)
                else: dt_obj = None

                if dt_obj:
                     if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc)
                     timestamp_iso = dt_obj.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

            if not timestamp_iso and isinstance(debezium_ts_ms, int):
                 timestamp_iso = datetime.datetime.fromtimestamp(debezium_ts_ms / 1000, tz=datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        except Exception as ts_e:
            task_logger.warning(f"Task {self.request.id} [LOG]: Error parsing timestamp '{ts_raw}' or '{debezium_ts_ms}': {ts_e}. Using fallback.")

        if not timestamp_iso:
            timestamp_iso = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            task_logger.warning(f"Task {self.request.id} [LOG]: Using current time as timestamp fallback.")
        # --- Fin Formatage Timestamp ---

        # --- Création Document ES pour LOGS ---
        es_doc = {
            '@timestamp': timestamp_iso,
            'level': str(log_data.get('level', 'UNKNOWN')).upper(), 
            'message': str(log_data.get('message', '')),
            'debezium_op': op,
        }

        # --- Détection d'Anomalie (Logique précédente avec vérif client) ---
        if es_doc['level'] == 'ERROR':
            if redis_client_anomaly: # Vérifier si le client est dispo DANS la tâche
                 current_minute_key = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M')
                 redis_key_error_count = f"error_count:{current_minute_key}"
                 redis_key_alert_sent = f"alert_sent:{current_minute_key}"
                 # Utiliser des constantes de config si possible pour threshold/window
                 redis_key_expiry = config.ANOMALY_TIME_WINDOW_SECONDS + 60
                 try:
                     count = redis_client_anomaly.incr(redis_key_error_count)
                     # Mettre l'expiration à chaque incrémentation est plus sûr
                     redis_client_anomaly.expire(redis_key_error_count, redis_key_expiry)
                     task_logger.debug(f"Task {self.request.id} [LOG]: ERROR detected. Count in minute {current_minute_key}: {count}")
                     # Utiliser ANOMALY_ERROR_THRESHOLD de config.py
                     if count >= config.ANOMALY_ERROR_THRESHOLD:
                         # setnx retourne 1 si la clé a été créée (alerte non envoyée), 0 sinon
                         if redis_client_anomaly.set(redis_key_alert_sent, "1", ex=redis_key_expiry, nx=True):
                             task_logger.warning(f"!!! ANOMALY DETECTED (Task {self.request.id} [LOG]) !!! Threshold ({config.ANOMALY_ERROR_THRESHOLD}) reached in minute {current_minute_key}. Count: {count}")
                             es_doc['anomaly_detected'] = True # Marquer le log
                         else:
                              task_logger.info(f"Task {self.request.id} [LOG]: Anomaly threshold reached ({count}), but alert already sent for minute {current_minute_key}.")
                 except redis.RedisError as redis_err:
                     task_logger.error(f"Task {self.request.id} [LOG]: Redis (anomaly) error during anomaly check: {redis_err}")
                     # Ne pas retenter la tâche pour une erreur Redis ici, juste logguer
            else:
                 task_logger.warning(f"Task {self.request.id} [LOG]: Cannot perform anomaly check because Redis Anomaly client is unavailable.")
        # --- Fin Détection d'Anomalie ---

        # --- Envoyer à Elasticsearch (LOGS) ---
        try:
            # Utiliser l'ID de la DB comme ID ES pour potentielle idempotence
            doc_id = f"log_db_{log_data.get('id', f'op_{op}_{self.request.id}')}"
            task_logger.debug(f"Task {self.request.id} [LOG]: Indexing document to ES index '{config.ELASTICSEARCH_LOG_INDEX}' with id='{doc_id}'")
            response = es_client.index(
                index=config.ELASTICSEARCH_LOG_INDEX, # Utiliser l'index des logs
                id=doc_id,
                document=es_doc
            )
            task_logger.info(f"Task {self.request.id} [LOG]: Log indexed to ES: {response['_id']} (Result: {response['result']})")
            return f"Processed and indexed log: {response['_id']}"

        except (ESConnectionError, ApiError) as es_err: # Attraper spécifiquement les erreurs ES pour relance
            task_logger.error(f"Task {self.request.id} [LOG]: Error indexing log to Elasticsearch: {es_err}. Retrying...", exc_info=True)
            # Laisser Celery gérer la relance en levant l'exception originale
            raise self.retry(exc=es_err, countdown=(self.request.retries + 1) * 30) # Augmenter le délai peut aider

    except (json.JSONDecodeError, UnicodeDecodeError) as decode_err:
        task_logger.error(f"Task {self.request.id} [LOG]: Failed to decode JSON/UTF-8: {event_data_bytes[:200]}... Error: {decode_err}")
        # Ne pas retenter ce type d'erreur
        return f"Decode Error (Not Retrying): {type(decode_err).__name__}"
    except Exception as e:
        task_logger.critical(f"Task {self.request.id} [LOG]: An critical unexpected error occurred: {e}", exc_info=True)
        # Tenter une relance pour les erreurs inconnues
        try:
            raise self.retry(exc=e, countdown=(self.request.retries + 1) * 60)
        except self.MaxRetriesExceededError:
            task_logger.critical(f"Task {self.request.id} [LOG]: Max retries exceeded for critical error. Giving up.")
            return "Failed after max retries (Critical Error)"
# --- Fin Tâche Logs ---


# --- NOUVELLE TÂCHE CELERY pour les ÉVÉNEMENTS PANIER ---
@app.task(bind=True, name='log_processor.process_cart_event', max_retries=3, default_retry_delay=60, queue='cart')
def process_cart_event(self, event_data_bytes):
    """
    Traite un événement de panier d'achat reçu de Kafka (via Debezium)
    et l'indexe dans l'index Elasticsearch dédié aux paniers.
    """
    task_logger.info(f"Task {self.request.id} [CART]: Received event data ({len(event_data_bytes)} bytes) on queue 'cart'.")

    # Vérifier le client ES au début. Essentiel !
    # Note: Pas besoin de redis_client_anomaly ici.
    if not es_client:
        task_logger.error(f"Task {self.request.id} [CART]: Prerequisite client missing (ES: Unavailable). Retrying task...")
        raise self.retry(exc=ConnectionRefusedError("CART Task: ES client not available in worker"), countdown=60 * (self.request.retries + 1))

    try:
        # 1. Décoder et Parser le JSON de Debezium
        event_data_str = event_data_bytes.decode('utf-8')
        message = json.loads(event_data_str)
        payload = message.get('payload')

        # 2. Vérifier le contenu et l'opération Debezium
        if not payload or payload.get('op') not in ('c', 'u', 'r') or not payload.get('after'):
            op = payload.get('op') if payload else 'N/A'
            reason = "no payload" if not payload else f"op={op} not in c,u,r" if payload.get('op') not in ('c', 'u', 'r') else "no 'after' data"
            task_logger.warning(f"Task {self.request.id} [CART]: Skipping cart event ({reason}).")
            return f"Skipped: Cart event ({reason})"

        cart_data = payload.get('after') # Les données de la ligne de la table cart_events
        op = payload.get('op') # Récupérer l'opération (c, u, r)

        # 3. Extraire les champs pertinents de cart_data
        # Les noms ici doivent correspondre aux noms de colonnes de `cart_events`
        event_id = cart_data.get('event_id') # Clé primaire de l'événement
        event_type = cart_data.get('event_type')
        user_id = cart_data.get('user_id')
        session_id = cart_data.get('session_id')
        product_id = cart_data.get('product_id')
        quantity = cart_data.get('quantity')
        unit_price = cart_data.get('unit_price')
        details = cart_data.get('details') # Champ JSON optionnel
        event_timestamp_db = cart_data.get('event_timestamp') # Timestamp de la DB

        # --- Formatage Timestamp pour l'événement panier ---
        debezium_ts_ms = payload.get('ts_ms')
        timestamp_iso = None
        try:
            # Priorité 1: Timestamp de la base de données
            if event_timestamp_db:
                # Tenter de parser comme ISO (avec ou sans T) ou format spécifique
                if isinstance(event_timestamp_db, str):
                     try: dt_obj = datetime.datetime.fromisoformat(str(event_timestamp_db).replace(' ', 'T'))
                     except ValueError: dt_obj = datetime.datetime.strptime(str(event_timestamp_db), '%Y-%m-%d %H:%M:%S') # Fallback format
                # Si c'est déjà un objet datetime (peu probable depuis JSON mais sait-on jamais)
                elif isinstance(event_timestamp_db, datetime.datetime): dt_obj = event_timestamp_db
                # Si c'est un nombre (timestamp epoch)
                elif isinstance(event_timestamp_db, (int, float)): dt_obj = datetime.datetime.fromtimestamp(event_timestamp_db, tz=datetime.timezone.utc)
                else: dt_obj = None

                if dt_obj:
                    if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc) # Assumer UTC si non précisé
                    timestamp_iso = dt_obj.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

            # Priorité 2: Timestamp de Debezium
            if not timestamp_iso and isinstance(debezium_ts_ms, int):
                 timestamp_iso = datetime.datetime.fromtimestamp(debezium_ts_ms / 1000, tz=datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        except Exception as ts_e:
             task_logger.warning(f"Task {self.request.id} [CART]: Error parsing cart timestamp '{event_timestamp_db}' or '{debezium_ts_ms}': {ts_e}. Using fallback.")

        # Priorité 3: Heure actuelle
        if not timestamp_iso:
            timestamp_iso = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            task_logger.warning(f"Task {self.request.id} [CART]: Using current time as timestamp fallback.")
        # --- Fin Formatage Timestamp ---


        # 4. Préparer le document pour Elasticsearch (CART EVENTS)
        # Utiliser une structure imbriquée pour plus de clarté dans Kibana
        doc_to_index = {
            '@timestamp': timestamp_iso,
            'event': {
                'id': event_id,
                'type': str(event_type).upper() if event_type else 'UNKNOWN', # Normaliser en majuscules
                'source': 'database',
                'debezium_op': op
            },
            'user': { 'id': str(user_id) if user_id is not None else None },
            'session': { 'id': str(session_id) if session_id is not None else None },
            'product': {
                'id': str(product_id) if product_id is not None else None,
                # Essayer de convertir en float, sinon None
                'unit_price': float(unit_price) if unit_price is not None else None
            },
            'cart': {
                # Essayer de convertir en int, sinon None
                'quantity': int(quantity) if quantity is not None else None
            },
            # Gérer le champ 'details' s'il existe (il pourrait être une string JSON)
            'details': {} # Initialiser vide
        }
        if details:
            if isinstance(details, str):
                try: doc_to_index['details'] = json.loads(details)
                except json.JSONDecodeError:
                    task_logger.warning(f"Task {self.request.id} [CART]: Field 'details' is not valid JSON: {details[:100]}...")
                    doc_to_index['details'] = {'raw': details} # Stocker brut si invalide
            elif isinstance(details, dict):
                 doc_to_index['details'] = details
            else:
                 doc_to_index['details'] = {'raw': str(details)}

        # Nettoyer les clés de niveau supérieur avec valeur None (optionnel)
        # doc_to_index = {k: v for k, v in doc_to_index.items() if v is not None}

        # 5. Indexer dans le NOUVEL index Elasticsearch (CART)
        try:
            target_index = config.ELASTICSEARCH_CART_INDEX # Utiliser l'index panier
            # Utiliser l'ID de l'événement DB comme ID ES pour l'idempotence
            doc_id = f"cart_db_{event_id}" if event_id else f"cart_op_{op}_{self.request.id}"

            task_logger.debug(f"Task {self.request.id} [CART]: Indexing document to ES index '{target_index}' with id='{doc_id}'...")
            response = es_client.index(
                index=target_index,
                id=doc_id,
                document=doc_to_index
            )
            task_logger.info(f"Task {self.request.id} [CART]: Cart event indexed to ES: {response['_id']} (Result: {response['result']})")
            return f"Processed and indexed cart event: {response['_id']}"

        except (ESConnectionError, ApiError) as es_err: # Attraper erreurs ES pour relance
            task_logger.error(f"Task {self.request.id} [CART]: Error indexing cart event to Elasticsearch index '{config.ELASTICSEARCH_CART_INDEX}': {es_err}. Retrying...", exc_info=True)
            raise self.retry(exc=es_err, countdown=(self.request.retries + 1) * 30)

    except (json.JSONDecodeError, UnicodeDecodeError) as decode_err:
        task_logger.error(f"Task {self.request.id} [CART]: Failed to decode JSON/UTF-8: {event_data_bytes[:200]}... Error: {decode_err}")
        return f"Decode Error (Not Retrying): {type(decode_err).__name__}" # Pas de relance
    except Exception as e:
        task_logger.critical(f"Task {self.request.id} [CART]: An critical unexpected error occurred: {e}", exc_info=True)
        # Tenter une relance pour les erreurs inconnues
        try:
            raise self.retry(exc=e, countdown=(self.request.retries + 1) * 60)
        except self.MaxRetriesExceededError:
            task_logger.critical(f"Task {self.request.id} [CART]: Max retries exceeded for critical error. Giving up.")
            return "Failed after max retries (Critical Error)"
# --- Fin Nouvelle Tâche Cart Events ---


# --- Fin du fichier tasks.py ---


