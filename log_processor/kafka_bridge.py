# log_processor/kafka_bridge.py
import time
import logging
import traceback

from kafka import KafkaConsumer # type: ignore
import redis # type: ignore Pour le test de ping direct

# Importer la configuration locale
from . import config #import config                       rrd lbaal
# Importer la TÂCHE spécifique depuis le module tasks

from .tasks import process_log_event #from tasks import process_log_event      rrrd lbaaaal

# Importer l'application Celery pour vérifier la config (optionnel mais bon pour debug)
# Cela suppose que celery_app.py définit une variable 'app'
try:
    from .celery_app import app as celery_application #from celery_app import app as celery_application    rrd lbaal
    celery_broker_url_check = celery_application.conf.broker_url
except ImportError:
    celery_application = None
    celery_broker_url_check = "Could not import celery_app"


# Configuration du logging simple pour le pont
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
bridge_logger = logging.getLogger('kafka_bridge')


def kafka_to_celery_bridge():
    """Lit Kafka et envoie les messages comme tâches à Celery."""
    bridge_logger.info(f"Starting Kafka consumer bridge for topic: {config.KAFKA_TOPIC} on {config.KAFKA_BROKER_URL}")
    bridge_logger.info(f"Celery App Broker URL configured as: {celery_broker_url_check}")

    consumer = None
    message_count = 0
    error_send_count = 0

    while True: # Boucle pour retenter la connexion Kafka
        try:
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BROKER_URL,
                group_id='log_processor_bridge_group_v4', # Incrémenter pour reset
                auto_offset_reset='earliest',
                consumer_timeout_ms=5000,
            )
            bridge_logger.info("Kafka consumer connected. Waiting for messages...")

            while True: # Boucle principale de consommation
                try:
                    for message in consumer:
                        message_count += 1
                        if message and message.value:
                            bridge_logger.info(f"Received message from Kafka (Offset: {message.offset}, Count: {message_count}). Size: {len(message.value)} bytes.")
                            # bridge_logger.debug(f"Raw message value (first 100 bytes): {message.value[:100]}")

                            # --- VÉRIFICATION AVANT .delay() ---
                            bridge_logger.info(">>> Preparing to send task to Celery broker...")
                            # 1. Test de connexion Redis direct (Broker = DB 0)
                            redis_direct_ping_ok = False
                            try:
                                redis_host_broker = config.REDIS_HOST # Doit être '127.0.0.1'
                                redis_port_broker = config.REDIS_PORT
                                bridge_logger.info(f"    Attempting DIRECT Redis ping to Broker {redis_host_broker}:{redis_port_broker} (DB 0)...")
                                direct_redis = redis.Redis(host=redis_host_broker, port=redis_port_broker, db=0, socket_connect_timeout=3, socket_timeout=3)
                                direct_redis.ping()
                                redis_direct_ping_ok = True
                                bridge_logger.info("    SUCCESS: Direct Redis ping to Broker OK.")
                            except Exception as direct_ping_err:
                                 bridge_logger.error(f"    !!! FAILED: Direct Redis ping to Broker FAILED: {direct_ping_err}", exc_info=False)

                            # 2. Tentative d'envoi de la tâche
                            if redis_direct_ping_ok:
                                bridge_logger.info("    Attempting process_log_event.delay()...")
                                try:
                                    # *** ICI L'APPEL CRUCIAL ***
                                    # On appelle .delay() sur la TÂCHE IMPORTÉE
                                    process_log_event.delay(message.value)
                                    bridge_logger.info("    SUCCESS: Task sent to Celery broker via .delay()")
                                except Exception as send_err:
                                    error_send_count += 1
                                    bridge_logger.critical(f"!!! CRITICAL: Error during process_log_event.delay() (Send Error Count: {error_send_count}): {send_err}", exc_info=True)
                                    bridge_logger.critical("    Check Redis connection and Celery worker status. Sleeping before next attempt...")
                                    time.sleep(10)
                            else:
                                bridge_logger.error("    Skipping .delay() because direct Redis ping to Broker failed.")
                                error_send_count += 1
                                time.sleep(10)

                        elif message:
                             bridge_logger.warning(f"Received message with empty value at offset {message.offset}")

                except StopIteration:
                     bridge_logger.debug("No new messages in Kafka poll, continuing...")
                     continue
                except Exception as inner_loop_err:
                     bridge_logger.error(f"!!! Unexpected error in Kafka message processing loop: {inner_loop_err}", exc_info=True)
                     time.sleep(5)

        except KeyboardInterrupt:
            bridge_logger.info("\nCtrl+C received. Stopping Kafka consumer bridge...")
            break
        except Exception as e:
            bridge_logger.error(f"!!! Kafka consumer connection or setup error: {e}. Retrying connection in 15 seconds...", exc_info=True)
            if consumer:
                try: consumer.close()
                except: pass
            consumer = None
            time.sleep(15)
        finally:
            bridge_logger.info(">>> Kafka bridge processing loop finished or interrupted.")
            if consumer:
                try:
                    bridge_logger.info("Closing Kafka consumer...")
                    consumer.close()
                    bridge_logger.info("Kafka consumer closed.")
                except Exception as close_err:
                    bridge_logger.error(f"Error closing Kafka consumer: {close_err}", exc_info=True)

if __name__ == '__main__':
    bridge_logger.info("Running Kafka to Celery Bridge Script...")
    kafka_to_celery_bridge()