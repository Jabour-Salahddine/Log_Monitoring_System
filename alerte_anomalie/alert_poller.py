'''
import os
import time
import datetime
import logging
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError
from dotenv import load_dotenv
from playsound import playsound # Import de la bibliothèque pour jouer le son

# --- Configuration du Logging ---
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
poller_logger = logging.getLogger('alert_poller')

# --- Configuration ---
load_dotenv() # Charger le fichier .env du répertoire courant

# Utiliser les mêmes variables d'environnement que l'API
ES_HOST_CONFIG = os.getenv("ELASTICSEARCH_HOST", "localhost:9200")
ALERT_INDEX = "alertes-kibana" # Nom de l'index où Kibana écrit les alertes
POLL_INTERVAL_SECONDS = 10     # Fréquence de vérification (en secondes)
SOUND_FILE_PATH = "alert_sound.mp3" # Chemin vers votre fichier son (à créer/placer)

# Garder en mémoire l'heure de la dernière alerte vue pour éviter les répétitions sonores
last_alert_timestamp_processed = datetime.datetime.utcnow() - datetime.timedelta(minutes=1) # Initialiser au passé

# --- Connexion Elasticsearch ---
es_client: Elasticsearch | None = None
try:
    if not ES_HOST_CONFIG.startswith(('http://', 'https://')):
        connection_url = f"http://{ES_HOST_CONFIG}"
    else:
        connection_url = ES_HOST_CONFIG
    poller_logger.info(f"Connecting to Elasticsearch at {connection_url}...")
    es_client = Elasticsearch([connection_url], request_timeout=5)
    if not es_client.ping():
        poller_logger.error("Initial Elasticsearch ping failed.")
        es_client = None
    else:
        poller_logger.info("Elasticsearch connection successful.")
except Exception as e:
    poller_logger.error(f"Failed to connect to Elasticsearch: {e}")
    es_client = None

# --- Fonction pour Jouer le Son ---
def play_alert_sound():
    """Tente de jouer le fichier son d'alerte."""
    try:
        # Vérifier si le fichier existe avant de tenter de le jouer
        if os.path.exists(SOUND_FILE_PATH):
            poller_logger.info(f"Playing sound: {SOUND_FILE_PATH}")
            playsound(SOUND_FILE_PATH)
        else:
            poller_logger.warning(f"Sound file not found at: {SOUND_FILE_PATH}")
    except Exception as e:
        # playsound peut lever des erreurs selon l'OS ou les dépendances audio
        poller_logger.error(f"Could not play sound: {e}")

# --- Boucle Principale de Polling ---
if es_client:
    poller_logger.info(f"Starting alert polling loop (Index: {ALERT_INDEX}, Interval: {POLL_INTERVAL_SECONDS}s)...")
    while True:
        try:
            # Heure actuelle pour la requête
            now = datetime.datetime.utcnow()

            # Requête pour chercher les NOUVELLES alertes depuis la dernière vérification
            query_body = {
                "size": 10, # Récupérer jusqu'à 10 nouvelles alertes par cycle
                "sort": [{"@timestamp": {"order": "asc"}}], # Traiter les plus anciennes d'abord
                "query": {
                    "range": {
                        "@timestamp": {
                            # 'gt' (greater than) pour ne prendre que celles APRES la dernière vue
                            "gt": last_alert_timestamp_processed.isoformat(timespec='milliseconds') + "Z",
                            "lte": now.isoformat(timespec='milliseconds') + "Z" # Jusqu'à maintenant
                        }
                    }
                }
            }

            poller_logger.debug(f"Polling for new alerts after: {last_alert_timestamp_processed.isoformat()}Z")
            response = es_client.search(
                index=ALERT_INDEX,
                body=query_body,
                ignore=[404] # Ignorer si l'index n'existe pas encore
            )

            hits = response.get("hits", {}).get("hits", [])
            new_alerts_found = len(hits) > 0
            latest_timestamp_in_batch = last_alert_timestamp_processed # Garder l'ancien si pas de nouveaux hits

            if new_alerts_found:
                poller_logger.info(f"Found {len(hits)} new alert(s)!")
                for hit in hits:
                    alert_doc = hit.get("_source", {})
                    alert_ts_str = alert_doc.get("@timestamp") # Ou le champ timestamp de l'alerte
                    rule_name = alert_doc.get("kibana.alert.rule.name", "N/A") # Trouver le nom de la règle

                    # Afficher/Logger l'alerte
                    poller_logger.warning(f"--- ALERT DETECTED ---")
                    poller_logger.warning(f"  Time: {alert_ts_str}")
                    poller_logger.warning(f"  Rule: {rule_name}")
                    # Vous pouvez extraire et afficher d'autres champs ici
                    # (ex: context.value, context.threshold, triggering_logs si présents)
                    triggering_logs = alert_doc.get("triggering_logs")
                    if triggering_logs and isinstance(triggering_logs, list):
                         poller_logger.warning(f"  Triggering Logs Sample ({len(triggering_logs)}):")
                         for i, log_hit in enumerate(triggering_logs):
                             log_source = log_hit.get("_source", {})
                             poller_logger.warning(f"    - {log_source.get('@timestamp')} | {log_source.get('level')} | {log_source.get('message')[:80]}...")
                             if i >= 2: # Limiter l'affichage
                                 poller_logger.warning(f"      ... (et plus)")
                                 break


                    # Mettre à jour le timestamp de la dernière alerte traitée dans ce batch
                    if alert_ts_str:
                        try:
                            current_alert_dt = datetime.datetime.fromisoformat(alert_ts_str.replace('Z', '+00:00'))
                            if current_alert_dt > latest_timestamp_in_batch:
                                latest_timestamp_in_batch = current_alert_dt
                        except ValueError:
                            poller_logger.error(f"Could not parse alert timestamp: {alert_ts_str}")

                # Jouer le son UNE SEULE FOIS par cycle de polling où de nouvelles alertes sont trouvées
                play_alert_sound()

            else:
                poller_logger.debug("No new alerts found.")

            # Mettre à jour l'heure de la dernière vérification pour la prochaine requête
            last_alert_timestamp_processed = latest_timestamp_in_batch if new_alerts_found else now

        except ESConnectionError as conn_err:
            poller_logger.error(f"Polling failed: Elasticsearch connection error: {conn_err}")
            # Peut-être ajouter une pause plus longue avant de réessayer
            time.sleep(POLL_INTERVAL_SECONDS * 2) # Attendre un peu plus longtemps
        except Exception as poll_err:
            poller_logger.error(f"An unexpected error occurred during polling: {poll_err}", exc_info=True)
            # Attendre avant de réessayer
            time.sleep(POLL_INTERVAL_SECONDS)

        # Attendre avant la prochaine vérification
        time.sleep(POLL_INTERVAL_SECONDS)

else:
    poller_logger.critical("Could not connect to Elasticsearch. Alert poller exiting.")
'''


import os
import time
import datetime
import logging
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError
from dotenv import load_dotenv
import pygame  # Nouveau module pour jouer le son
from datetime import timezone
from dateutil import parser




# --- Initialisation de pygame.mixer ---
pygame.mixer.init()

# --- Configuration du Logging ---
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
poller_logger = logging.getLogger('alert_poller')

# --- Configuration ---
load_dotenv()

# pour determiner la date dans la foerme convenable : 

def to_elasticsearch_timestamp(dt: datetime.datetime) -> str:
    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

ES_HOST_CONFIG = os.getenv("ELASTICSEARCH_HOST", "localhost:9200")
ALERT_INDEX = "alertes-kibana"
POLL_INTERVAL_SECONDS =10
SOUND_FILE_PATH = "alert_sound.mp3"

last_alert_timestamp_processed = datetime.datetime.now(timezone.utc) - datetime.timedelta(minutes=1)

# --- Connexion Elasticsearch ---
es_client: Elasticsearch | None = None
try:
    connection_url = f"http://{ES_HOST_CONFIG}" if not ES_HOST_CONFIG.startswith(('http://', 'https://')) else ES_HOST_CONFIG
    poller_logger.info(f"Connecting to Elasticsearch at {connection_url}...")
    es_client = Elasticsearch([connection_url], request_timeout=5)
    if not es_client.ping():
        poller_logger.error("Initial Elasticsearch ping failed.")
        es_client = None
    else:
        poller_logger.info("Elasticsearch connection successful.")
except Exception as e:
    poller_logger.error(f"Failed to connect to Elasticsearch: {e}")
    es_client = None

# --- Fonction pour Jouer le Son ---
def play_alert_sound():
    """Jouer le fichier son avec pygame."""
    try:
        if os.path.exists(SOUND_FILE_PATH):
            poller_logger.info(f"Playing sound: {SOUND_FILE_PATH}")
            pygame.mixer.music.load(SOUND_FILE_PATH)
            pygame.mixer.music.play()
            while pygame.mixer.music.get_busy():
                time.sleep(0.1)  # Attendre que le son se termine
        else:
            poller_logger.warning(f"Sound file not found at: {SOUND_FILE_PATH}")
    except Exception as e:
        poller_logger.error(f"Could not play sound: {e}")

# --- Boucle Principale de Polling ---
if es_client:
    poller_logger.info(f"Starting alert polling loop (Index: {ALERT_INDEX}, Interval: {POLL_INTERVAL_SECONDS}s)...")
    while True:
        try:
            now = datetime.datetime.now(timezone.utc)

            query_body = {
                "size": 10,
                "sort": [{"@timestamp": {"order": "asc"}}],
                "query": {
                    "range": {
                        "@timestamp": {
                            "gt": to_elasticsearch_timestamp(last_alert_timestamp_processed),
                            "lte": to_elasticsearch_timestamp(now)
                        }
                    }
                }
            }

            poller_logger.debug(f"Polling for new alerts after: {last_alert_timestamp_processed.isoformat()}Z")
            response = es_client.search(index=ALERT_INDEX, body=query_body, ignore=[404])

            hits = response.get("hits", {}).get("hits", [])
            new_alerts_found = len(hits) > 0
            latest_timestamp_in_batch = last_alert_timestamp_processed

            if new_alerts_found:
                poller_logger.info(f"Found {len(hits)} new alert(s)!")
                for hit in hits:
                    source_level1  = hit.get("_source", {})
                    
                    alert_doc = source_level1.get("_source", source_level1)
                    
                    alert_ts_str = alert_doc.get("@timestamp")
                    
                    rule_name = alert_doc.get("rule_name")

                    if not rule_name: # Sinon, chercher dans les champs Kibana potentiels
                       rule_name = alert_doc.get("kibana.alert.rule.name", "N/A") # Essayez ceci

                    poller_logger.warning(f"--- ALERT DETECTED ---")
                    poller_logger.warning(f"  Time: {alert_ts_str}")
                    poller_logger.warning(f"  Rule: {rule_name}")

                    triggering_logs = alert_doc.get("triggering_logs")
                    if triggering_logs and isinstance(triggering_logs, list):
                        poller_logger.warning(f"  Triggering Logs Sample ({len(triggering_logs)}):")
                        for i, log_hit in enumerate(triggering_logs):
                            log_source = log_hit.get("_source", {})
                            poller_logger.warning(f"    - {log_source.get('@timestamp')} | {log_source.get('level')} | {log_source.get('message')[:80]}...")
                            if i >= 2:
                                poller_logger.warning(f"      ... (et plus)")
                                break

                    if alert_ts_str:
                     try:
                       current_alert_dt = parser.isoparse(alert_ts_str)

                       # Forcer UTC si aucune timezone n’est présente
                       if current_alert_dt.tzinfo is None:
                          current_alert_dt = current_alert_dt.replace(tzinfo=timezone.utc)

                       if current_alert_dt > latest_timestamp_in_batch:
                          latest_timestamp_in_batch = current_alert_dt
                     except (ValueError, TypeError) as e:
                      poller_logger.error(f"Could not parse alert timestamp: {alert_ts_str} ({e})")
                     
                play_alert_sound()
            else:
                poller_logger.debug("No new alerts found.")

            last_alert_timestamp_processed = latest_timestamp_in_batch if new_alerts_found else now

        except ESConnectionError as conn_err:
            poller_logger.error(f"Polling failed: Elasticsearch connection error: {conn_err}")
            time.sleep(POLL_INTERVAL_SECONDS * 2)
        except Exception as poll_err:
            poller_logger.error(f"An unexpected error occurred during polling: {poll_err}", exc_info=True)
            time.sleep(POLL_INTERVAL_SECONDS)

        time.sleep(POLL_INTERVAL_SECONDS) # le temp entre deux fetch successives

else:
    poller_logger.critical("Could not connect to Elasticsearch. Alert poller exiting.")


