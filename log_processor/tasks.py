# log_processor/tasks.py
import json
import datetime
import time
import traceback
import logging

from .celery_app import app #from celery_app import app # Importez l'instance Celery depuis celery_app.py

from celery import shared_task # type: ignore
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError, ApiError # type: ignore
import redis # type: ignore

# Importer la configuration locale
from . import config #import config

# Configuration du logging simple pour la tâche
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
# Utiliser un logger spécifique pour la tâche
task_logger = logging.getLogger('celery.tasks.process_log')


# --- Initialisation des Clients (au niveau module, partagé par les workers) ---
# Note: Ces clients sont initialisés une fois par processus worker Celery.
# S'ils échouent ici, le worker aura des problèmes pour traiter les tâches.
es_client: Elasticsearch | None = None
redis_client_anomaly: redis.Redis | None = None # Pour la détection d'anomalie

# --- Constantes pour la tentative de connexion ES ---
MAX_ES_CONNECTION_ATTEMPTS = 5
ES_CONNECTION_RETRY_DELAY_SECONDS = 5

# --- Bloc de Connexion Elasticsearch (AVEC RETRIES et test INFO()) ---
# (Ce bloc est exécuté par chaque processus worker au démarrage)
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
# --- Fin Bloc Connexion Elasticsearch ---

# --- Bloc de Connexion Redis pour la détection d'anomalie ---
# (Ce bloc est aussi exécuté par chaque processus worker au démarrage)
try:
    redis_host_anomaly = config.REDIS_HOST # Doit être '127.0.0.1'
    redis_port_anomaly = config.REDIS_PORT
    task_logger.info(f"Worker connecting to Redis (anomaly detection) at {redis_host_anomaly}:{redis_port_anomaly} (DB 2)...")
    redis_client_anomaly = redis.Redis(host=redis_host_anomaly, port=redis_port_anomaly, db=2, decode_responses=True, socket_connect_timeout=5)
    redis_client_anomaly.ping()
    task_logger.info(">>> Worker successfully connected to Redis (anomaly detection)")
except Exception as e_redis:
    task_logger.error(f"!!! Worker error connecting to Redis (anomaly detection): {e_redis}", exc_info=True)
    redis_client_anomaly = None
# --- Fin Bloc Connexion Redis Anomaly ---

# --- Vérification/Création de l'Index Elasticsearch ---
# (Exécuté par chaque worker, mais la création ne se fait qu'une fois)
if es_client:
    try:
        task_logger.info(f"Worker checking/creating index '{config.ELASTICSEARCH_INDEX}'...")
        if not es_client.indices.exists(index=config.ELASTICSEARCH_INDEX):
            task_logger.info(f"Index '{config.ELASTICSEARCH_INDEX}' not found by this worker. Attempting creation (if not already created by another worker)...")
            index_body = { "mappings": { "properties": { "@timestamp": {"type": "date"}, "level": {"type": "keyword"}, "message": {"type": "text"} } } }
            # ignore=[400] est important ici car un autre worker peut créer l'index entre exists() et create()
            es_client.indices.create(index=config.ELASTICSEARCH_INDEX, body=index_body, ignore=[400])
            # On ne peut pas être sûr d'avoir *créé* l'index ici, juste qu'il existe après l'appel.
            task_logger.info(f"Index '{config.ELASTICSEARCH_INDEX}' should now exist.")
        else:
             task_logger.info(f"Index '{config.ELASTICSEARCH_INDEX}' already exists.")
    except Exception as index_error:
        task_logger.error(f"!!! Worker error checking or creating index '{config.ELASTICSEARCH_INDEX}': {index_error}", exc_info=True)
else:
    task_logger.warning("!!! Worker skipping index check because Elasticsearch client is not available.")
# --- Fin Vérification/Création Index ---


# --- Tâche Celery ---
@app.task(bind=True, name='log_processor.process_log_event', max_retries=3, queue='logs')
def process_log_event(self, event_data_bytes):
    """Traite un événement brut reçu de Kafka (en bytes)."""
    task_logger.info(f"Task {self.request.id}: Received event data ({len(event_data_bytes)} bytes).")

    # Vérifier les clients au début de chaque tâche. Essentiel !
    if not es_client or not redis_client_anomaly:
        current_es_status = "OK" if es_client else "Unavailable"
        current_redis_status = "OK" if redis_client_anomaly else "Unavailable"
        task_logger.error(f"Task {self.request.id}: Prerequisite client missing (ES: {current_es_status}, Redis Anomaly: {current_redis_status}). Retrying task...")
        raise self.retry(exc=ConnectionRefusedError("ES or Redis Anomaly client not available in worker"), countdown=60)

    # Optionnel : Re-vérifier le ping ES (peut ajouter de la latence)
    # try:
    #     if not es_client.ping():
    #         task_logger.warning(f"Task {self.request.id}: Elasticsearch ping failed at task execution. Retrying...")
    #         raise self.retry(exc=ESConnectionError("Ping failed during task execution"), countdown=30)
    # except Exception as ping_err:
    #      task_logger.error(f"Task {self.request.id}: Error during Elasticsearch ping check: {ping_err}. Retrying...")
    #      raise self.retry(exc=ping_err, countdown=30)

    try:
        event_data_str = event_data_bytes.decode('utf-8')
        message = json.loads(event_data_str)
        payload = message.get('payload')
        if not payload:
            task_logger.warning(f"Task {self.request.id}: Skipping event without payload")
            return "Skipped: No payload"

        op = payload.get('op')
        if op == 'd':
            task_logger.info(f"Task {self.request.id}: Skipping delete operation")
            return "Skipped: Delete operation"
        log_data = payload.get('after') if op in ['c', 'u', 'r'] else payload
        if not log_data:
             task_logger.warning(f"Task {self.request.id}: Skipping event without log data in 'after' or 'payload'")
             return "Skipped: No log data"

        # --- Formatage Timestamp (Logique précédente) ---
        ts_raw = log_data.get('log_time')
        debezium_ts_ms = payload.get('ts_ms')
        timestamp_iso = None
        if ts_raw:
             if isinstance(ts_raw, str):
                 try:
                    dt_obj = datetime.datetime.strptime(ts_raw, '%Y-%m-%d %H:%M:%S')
                    dt_obj_utc = dt_obj.replace(tzinfo=datetime.timezone.utc)
                    timestamp_iso = dt_obj_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                 except ValueError: pass
             elif isinstance(ts_raw, (int, float)):
                  try:
                      timestamp_iso = datetime.datetime.fromtimestamp(ts_raw, tz=datetime.timezone.utc).isoformat(timespec='milliseconds') + 'Z'
                  except ValueError: pass
        if not timestamp_iso and isinstance(debezium_ts_ms, int):
             try:
                 timestamp_iso = datetime.datetime.fromtimestamp(debezium_ts_ms / 1000, tz=datetime.timezone.utc).isoformat(timespec='milliseconds') + 'Z'
             except ValueError: pass
        if not timestamp_iso:
             timestamp_iso = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds') + 'Z'
             task_logger.warning(f"Task {self.request.id}: Using current time as timestamp fallback.")
        # --- Fin Formatage Timestamp ---

        es_doc = {
            '@timestamp': timestamp_iso,
            'log_db_timestamp_raw': ts_raw,
            'debezium_event_timestamp_ms': debezium_ts_ms,
            'level': str(log_data.get('level', 'UNKNOWN')).upper(),
            'message': str(log_data.get('message', '')),
            'source_db_id': log_data.get('id'),
            'pipeline_step': 'celery_processed',
            'debezium_op': op
        }

        # --- Détection d'Anomalie (Utilise redis_client_anomaly) ---
        if es_doc['level'] == 'ERROR':
            current_minute_key = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M')
            redis_key_error_count = f"error_count:{current_minute_key}"
            redis_key_alert_sent = f"alert_sent:{current_minute_key}"
            redis_key_expiry = config.ANOMALY_TIME_WINDOW_SECONDS + 60
            try:
                # Vérifier si le client Redis pour anomalie est disponible
                if redis_client_anomaly:
                    count = redis_client_anomaly.incr(redis_key_error_count)
                    redis_client_anomaly.expire(redis_key_error_count, redis_key_expiry)
                    task_logger.debug(f"Task {self.request.id}: ERROR detected. Count in minute {current_minute_key}: {count}")
                    if count > config.ANOMALY_ERROR_THRESHOLD:
                        if not redis_client_anomaly.exists(redis_key_alert_sent):
                            task_logger.warning(f"!!! ANOMALY DETECTED (Task {self.request.id}) !!! Threshold ({config.ANOMALY_ERROR_THRESHOLD}) exceeded in minute {current_minute_key}. Count: {count}")
                            es_doc['anomaly_detected'] = True
                            redis_client_anomaly.set(redis_key_alert_sent, "1", ex=redis_key_expiry)
                else:
                    task_logger.warning(f"Task {self.request.id}: Cannot perform anomaly check because Redis Anomaly client is unavailable.")

            except redis.RedisError as redis_err:
                task_logger.error(f"Task {self.request.id}: Redis (anomaly) error during anomaly check: {redis_err}")

        # --- Envoyer à Elasticsearch ---
        try:
            doc_id = f"db_{log_data.get('id', 'no_id')}_{op}"
            task_logger.debug(f"Task {self.request.id}: Indexing document to ES with id={doc_id}")
            response = es_client.index(
                index=config.ELASTICSEARCH_INDEX,
                id=doc_id,
                document=es_doc
                )
            task_logger.info(f"Task {self.request.id}: Log indexed to ES: {response['_id']} (Result: {response['result']})")
            return f"Processed and indexed: {response['_id']}"

        except Exception as es_err: # Attraper toutes les erreurs ES ici pour relance
            task_logger.error(f"Task {self.request.id}: Error indexing log to Elasticsearch: {es_err}. Retrying...", exc_info=True)
            # Laisser Celery gérer la relance en levant l'exception originale
            raise self.retry(exc=es_err, countdown=(self.request.retries + 1) * 15)

    except json.JSONDecodeError as e:
        task_logger.error(f"Task {self.request.id}: Failed to decode JSON: {event_data_bytes[:200]}... Error: {e}")
        return "JSON Decode Error (Not Retrying)"
    except UnicodeDecodeError as e:
        task_logger.error(f"Task {self.request.id}: Failed to decode UTF-8: {event_data_bytes[:200]}... Error: {e}")
        return "UTF-8 Decode Error (Not Retrying)"
    except Exception as e:
        task_logger.critical(f"Task {self.request.id}: An critical unexpected error occurred in task logic: {e}", exc_info=True)
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
             task_logger.critical(f"Task {self.request.id}: Max retries exceeded for critical error. Giving up.")
             return "Failed after max retries (Critical Error)"