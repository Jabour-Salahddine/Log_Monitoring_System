
# log_processor/config.py
KAFKA_BROKER_URL = 'localhost:29092' # Accessible depuis l'HÔTE via le port mappé
KAFKA_TOPIC = 'xampp_mysql_server.appdb.app_logs' # !! Correspondre au topic Debezium !!
ELASTICSEARCH_HOST = 'localhost' #ELASTICSEARCH_HOST = 'localhost:9200' # Accessible depuis l'HÔTE via le port mappé
ELASTICSEARCH_INDEX = 'app-logs-index-xampp' # nom d'index : comme une table en sql ou bien une collection en mongodb
REDIS_HOST = '127.0.0.1' #REDIS_HOST = 'localhost' # Accessible depuis l'HÔTE via le port mappé
REDIS_PORT = 6379
ANOMALY_ERROR_THRESHOLD = 3  # déclencher une alerte si 3 erreurs similaires en moins de 60 sec
ANOMALY_TIME_WINDOW_SECONDS = 60 



# pour un simple test : 

'''
# log_processor/config.py

# --- Seulement les configurations nécessaires pour le test de base ---

# Kafka
KAFKA_BROKER_URL = 'localhost:29092' # Accessible depuis l'HÔTE
KAFKA_TOPIC = 'xampp_mysql_server.appdb.app_logs' # Assurez-vous que c'est le bon topic

# Redis (pour Celery Broker)
REDIS_HOST = '127.0.0.1' # Ou 'localhost' si vous préférez
REDIS_PORT = 6379
'''