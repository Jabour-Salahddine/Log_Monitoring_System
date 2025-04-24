# log_processor/config.py
KAFKA_BROKER_URL = 'localhost:29092' # Accessible depuis l'HÔTE via le port mappé
KAFKA_TOPIC = 'xampp_mysql_server.appdb.app_logs' # !! Correspondre au topic Debezium !!
ELASTICSEARCH_HOST = 'localhost' #ELASTICSEARCH_HOST = 'localhost:9200' # Accessible depuis l'HÔTE via le port mappé
ELASTICSEARCH_INDEX = 'app-logs-index-xampp' # Nouveau nom d'index pour éviter conflits
REDIS_HOST = '127.0.0.1' #REDIS_HOST = 'localhost' # Accessible depuis l'HÔTE via le port mappé
REDIS_PORT = 6379
ANOMALY_ERROR_THRESHOLD = 3  # déclencher une alerte si 3 erreurs similaires en moins de 60 sec
ANOMALY_TIME_WINDOW_SECONDS = 60 