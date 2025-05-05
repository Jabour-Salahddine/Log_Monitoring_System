# log_processor/tasks.py
import json
import datetime
import time
import traceback
import logging

# --- Imports spécifiques au projet ---
from .celery_app import app     # Importe l'instance 'app' de Celery définie dans celery_app.py
from . import config            # Importe les configurations (adresses, noms, seuils)

# --- Imports des bibliothèques externes ---
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError, ApiError # Pour interagir avec Elasticsearch
import redis                    # Pour interagir avec Redis (pour la détection d'anomalie)

# Configuration du logging (Identique à la version précédente)
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
task_logger = logging.getLogger('celery.tasks.process_log')

# --- Initialisation des Clients (Identique à la version précédente) ---
es_client: Elasticsearch | None = None
redis_client_anomaly: redis.Redis | None = None
MAX_ES_CONNECTION_ATTEMPTS = 5
ES_CONNECTION_RETRY_DELAY_SECONDS = 5

# --- Bloc de Connexion Elasticsearch (Identique à la version précédente) ---
connection_url_es = f"http://{config.ELASTICSEARCH_HOST}:9200"
task_logger.info(f"Worker initializing ES connection to {connection_url_es}...")
for attempt_es in range(1, MAX_ES_CONNECTION_ATTEMPTS + 1):
    try:
        task_logger.info(f"ES Connection attempt {attempt_es}/{MAX_ES_CONNECTION_ATTEMPTS}...")
        temp_es_client = Elasticsearch([connection_url_es], request_timeout=20)
        task_logger.info(f"Attempt {attempt_es}: Checking ES cluster info...")
        cluster_info = temp_es_client.info()
        if cluster_info and 'version' in cluster_info:
            es_client = temp_es_client
            task_logger.info(f">>> Worker successfully connected to Elasticsearch. Version: {cluster_info['version']['number']}")
            break
        else:
             task_logger.warning(f"Attempt {attempt_es}: Received unexpected response from ES info(): {cluster_info}")
    except Exception as e_es:
        task_logger.error(f"!!! Attempt {attempt_es}: Error during ES connection/info check: {e_es}", exc_info=True)
    if attempt_es < MAX_ES_CONNECTION_ATTEMPTS:
        task_logger.info(f"    Retrying ES connection in {ES_CONNECTION_RETRY_DELAY_SECONDS} seconds...")
        time.sleep(ES_CONNECTION_RETRY_DELAY_SECONDS)
    else:
        task_logger.critical(f"!!! CRITICAL: Worker failed to connect to Elasticsearch after {MAX_ES_CONNECTION_ATTEMPTS} attempts.")

# --- Bloc de Connexion Redis pour la détection d'anomalie (Identique à la version précédente) ---
try:
    redis_host_anomaly = config.REDIS_HOST
    redis_port_anomaly = config.REDIS_PORT
    task_logger.info(f"Worker connecting to Redis (anomaly detection) at {redis_host_anomaly}:{redis_port_anomaly} (DB 2)...")
    redis_client_anomaly = redis.Redis(host=redis_host_anomaly, port=redis_port_anomaly, db=2, decode_responses=True, socket_connect_timeout=5)
    redis_client_anomaly.ping()
    task_logger.info(">>> Worker successfully connected to Redis (anomaly detection)")
except Exception as e_redis:
    task_logger.error(f"!!! Worker error connecting to Redis (anomaly detection): {e_redis}", exc_info=True)
    redis_client_anomaly = None

# --- Vérification/Création de l'Index Elasticsearch (Identique à la version précédente) ---
if es_client:
    try:
        task_logger.info(f"Worker checking/creating index '{config.ELASTICSEARCH_INDEX}'...")
        if not es_client.indices.exists(index=config.ELASTICSEARCH_INDEX):
            task_logger.info(f"Index '{config.ELASTICSEARCH_INDEX}' not found, attempting creation...")
            # Assurez-vous que le mapping inclut tous les champs que vous prévoyez d'indexer
            # et qu'ils ont le bon type (ex: level: keyword)
            index_body = {
                 "mappings": {
                     "properties": {
                         "@timestamp": {"type": "date"},
                         "level": {"type": "keyword"}, # Important pour les filtres exacts
                         "message": {"type": "text"}
                        
                        # les autres champ comme operation et anomalie mapping auto
                        # "source_info.db_log_id": {"type": "long"}, # Si vous décidez de le remettre
                        # "source_info.debezium_op": {"type": "keyword"} # Opération Debezium
                         # Ajoutez d'autres champs et leurs types si nécessaire
                     }
                 }
            }
            es_client.indices.create(index=config.ELASTICSEARCH_INDEX, body=index_body, ignore=[400])
            task_logger.info(f"Index '{config.ELASTICSEARCH_INDEX}' should now exist.")
        else:
             task_logger.info(f"Index '{config.ELASTICSEARCH_INDEX}' already exists.")
    except Exception as index_error:
        task_logger.error(f"!!! Worker error checking or creating index '{config.ELASTICSEARCH_INDEX}': {index_error}", exc_info=True)
else:
    task_logger.warning("!!! Worker skipping index check because Elasticsearch client is not available.")

# --- Tâche Celery ---
# Le décorateur reste le même
@app.task(bind=True, name='log_processor.process_log_event', max_retries=3, default_retry_delay=30, queue='logs')
def process_log_event(self, event_data_bytes):
    """Traite un événement brut Debezium reçu de Kafka (en bytes) et l'indexe dans Elasticsearch."""
    task_id = self.request.id # Utilise self.request.id via bind=True
    task_logger.info(f"----> TASK {task_id}: Received event data ({len(event_data_bytes)} bytes).")

    # === Vérifications Préliminaires (Identique) ===
    if not es_client:
        task_logger.error(f"TASK {task_id}: Elasticsearch client not available. Retrying...")
        raise self.retry(exc=ESConnectionError("ES client not initialized"), countdown=60)
    # if not redis_client_anomaly: ... (vérif redis si besoin)

    # === Traitement du Message ===
    try:
        # 1. Décoder et Parser (Identique)
        event_data_str = event_data_bytes.decode('utf-8')
        message = json.loads(event_data_str)
        task_logger.debug(f"TASK {task_id}: Decoded and parsed JSON.")

        # 2. Extraire Payload (Identique)
        payload = message.get('payload')
        if not payload:
            task_logger.warning(f"TASK {task_id}: Skipping - No 'payload'.")
            return "Skipped: No payload"

        # 3. Extraire Opération et Données (Identique)
        op = payload.get('op')
        log_data = None
        if op == 'd':
            log_data = payload.get('before')
            task_logger.info(f"TASK {task_id}: Processing delete (op='d').")
            # NOTE: La logique de suppression effective dans ES n'est pas implémentée ici.
            # Si vous voulez supprimer le document ES correspondant :
            # try:
            #     delete_doc_id = f"db_{log_data.get('id', 'no_id')}"
            #     es_client.delete(index=config.ELASTICSEARCH_INDEX, id=delete_doc_id, ignore=[404])
            #     task_logger.info(f"TASK {task_id}: Deleted document {delete_doc_id}.")
            #     return f"Processed delete for {delete_doc_id}"
            # except Exception as del_err:
            #     task_logger.error(f"TASK {task_id}: Error deleting {delete_doc_id}: {del_err}")
            return "Skipped: Delete operation" # Ou retourner après tentative de suppression
        elif op in ['c', 'u', 'r']:
            log_data = payload.get('after')
            task_logger.info(f"TASK {task_id}: Processing {op} operation.")
        else:
            task_logger.warning(f"TASK {task_id}: Skipping - Unknown op: {op}")
            return f"Skipped: Unknown op '{op}'"

        if not log_data:
             task_logger.warning(f"TASK {task_id}: Skipping - No log data found for op '{op}'.")
             return "Skipped: No log data for op"

        # --- 4. Formatage Timestamp (Identique) ---
        # ... (code identique pour déterminer timestamp_iso) ...
        # Assurez-vous que la logique de parsing et de fallback est robuste
        timestamp_iso = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds') + 'Z' # Fallback simplifié pour l'exemple
        if payload.get('ts_ms'):
            try:
                 timestamp_iso = datetime.datetime.fromtimestamp(payload['ts_ms'] / 1000, tz=datetime.timezone.utc).isoformat(timespec='milliseconds') + 'Z'
            except (ValueError, TypeError): pass
        # Ajouter ici la logique pour parser log_data.get('log_time') si vous voulez cette priorité

        # --- 5. Construire le document BRUT (Identique) ---
        # (Il est toujours bon de construire le doc complet ici pour avoir les infos pour la logique suivante)
        es_doc = {
            '@timestamp': timestamp_iso,
            'level': str(log_data.get('level', 'UNKNOWN')).upper(),
            'message': str(log_data.get('message', '')).strip(),
            'debezium_op': op,
                }

        # --- 6. Détection d'Anomalie (Identique) ---
        anomaly_detected = False
        if es_doc['level'] == 'ERROR' and redis_client_anomaly:
            try:
                current_minute_key = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M')
                redis_key_error_count = f"error_count:{current_minute_key}"
                redis_key_alert_sent = f"alert_sent:{current_minute_key}"
                redis_key_expiry = config.ANOMALY_TIME_WINDOW_SECONDS + 60

                pipe = redis_client_anomaly.pipeline()
                pipe.incr(redis_key_error_count)
                pipe.expire(redis_key_error_count, redis_key_expiry)
                results = pipe.execute()
                count = results[0]

                task_logger.debug(f"TASK {task_id}: ERROR count in minute {current_minute_key}: {count}")
                if count > config.ANOMALY_ERROR_THRESHOLD:
                    alert_already_sent = redis_client_anomaly.set(redis_key_alert_sent, "1", ex=redis_key_expiry, nx=True)
                    if alert_already_sent:
                        task_logger.warning(f"!!! ANOMALY DETECTED (Task {task_id}) !!! Threshold exceeded in {current_minute_key}. Count: {count}.")
                        anomaly_detected = True
                    else:
                         task_logger.info(f"TASK {task_id}: Anomaly threshold exceeded ({count}), but alert already sent for {current_minute_key}.")
            except redis.RedisError as redis_err:
                task_logger.error(f"TASK {task_id}: Redis error during anomaly check: {redis_err}", exc_info=False)
            except Exception as anomaly_e:
                 task_logger.error(f"TASK {task_id}: Unexpected error during anomaly check: {anomaly_e}", exc_info=True)
        elif es_doc['level'] == 'ERROR' and not redis_client_anomaly:
             task_logger.warning(f"TASK {task_id}: Cannot perform anomaly check - Redis Anomaly client unavailable.")

        # Ajouter le flag d'anomalie au document si détectée
        if anomaly_detected:
             # Note: Vous pouvez choisir de mettre ce champ dans pipeline_info ou à la racine
             es_doc['anomaly_detected'] = True # Ajout à la racine pour simplicité

        # ========== MODIFICATION POUR SOLUTION 3 - Vérif avant indexation 'r' ==========
        # --- 7. Vérification d'existence avant d'indexer un 'read' (snapshot) ---

        doc_id_base = f"db_{log_data.get('id', 'no_id')}" # ID basé sur l'ID de la DB

        # Si l'opération vient d'un snapshot ('r')
        if op == 'r':
            try:
                task_logger.debug(f"TASK {task_id}: Snapshot event (op='r'). Checking existence of doc ID: {doc_id_base}...")
                # Vérifie si le document existe déjà
                exists = es_client.exists(index=config.ELASTICSEARCH_INDEX, id=doc_id_base)

                if exists:
                    # Si oui, on ignore l'événement du snapshot pour ne pas écraser l'original
                    task_logger.info(f"TASK {task_id}: Doc {doc_id_base} exists. Skipping snapshot update (op='r').")
                    return f"Skipped snapshot update for existing doc: {doc_id_base}" # Termine la tâche
                else:
                    # Si non, on continue pour indexer ce log vu pendant le snapshot
                    task_logger.info(f"TASK {task_id}: Doc {doc_id_base} not found. Proceeding with indexing snapshot event (op='r').")
            except ESConnectionError as check_conn_err:
                 task_logger.error(f"TASK {task_id}: ES connection error during existence check for {doc_id_base}. Retrying task...", exc_info=False)
                 raise self.retry(exc=check_conn_err) # Relance la tâche si on ne peut pas vérifier
            except Exception as check_err:
                task_logger.warning(f"TASK {task_id}: Error during existence check for {doc_id_base}. Proceeding with index attempt. Error: {check_err}", exc_info=True)
                # On continue prudemment en cas d'autre erreur de vérification

        # --- 8. Envoyer à Elasticsearch ---
        # S'exécute si op != 'r' OU (op == 'r' AND le document n'existait pas ou check a échoué)
        try:
            task_logger.debug(f"TASK {task_id}: Indexing/Updating document to ES index '{config.ELASTICSEARCH_INDEX}' with id='{doc_id_base}'...")
            response = es_client.index(
                index=config.ELASTICSEARCH_INDEX,
                id=doc_id_base,      # Utilise l'ID simple
                document=es_doc      # Envoie le document complet construit plus haut
            )
            task_logger.info(f"TASK {task_id}: Log indexed/updated to ES. Result: {response.get('result')}, ES ID: {response.get('_id')}")
            return f"Processed and indexed/updated: {response.get('_id')}"

        # Gestion des erreurs d'indexation (Identique)
        except ApiError as es_index_api_err:
             task_logger.error(f"TASK {task_id}: ES API Error during indexing: {es_index_api_err}. Retrying...", exc_info=False)
             raise self.retry(exc=es_index_api_err)
        except ESConnectionError as es_index_conn_err:
             task_logger.error(f"TASK {task_id}: ES Connection Error during indexing: {es_index_conn_err}. Retrying...", exc_info=False)
             raise self.retry(exc=es_index_conn_err)
        except Exception as es_index_err:
            task_logger.error(f"TASK {task_id}: Unexpected error during indexing: {es_index_err}. Retrying...", exc_info=True)
            raise self.retry(exc=es_index_err)

    # === Gestion des Erreurs de Décodage/Parsing Initial (Identique) ===
    except json.JSONDecodeError as e_json:
        task_logger.error(f"TASK {task_id}: Failed JSON decode: {e_json}. Snippet: {event_data_bytes[:200]}...")
        return "Failed: JSON Decode Error (Not Retried)"
    except UnicodeDecodeError as e_unicode:
        task_logger.error(f"TASK {task_id}: Failed UTF-8 decode: {e_unicode}. Snippet: {event_data_bytes[:200]}...")
        return "Failed: UTF-8 Decode Error (Not Retried)"
    except Exception as e_general:
        task_logger.critical(f"TASK {task_id}: Unexpected critical error early in task: {e_general}", exc_info=True)
        try:
            raise self.retry(exc=e_general)
        except self.MaxRetriesExceededError:
             task_logger.critical(f"TASK {task_id}: Max retries exceeded for critical error. Giving up.")
             return f"Failed after max retries (Critical Error): {e_general}"

# --- Fin du fichier tasks.py ---