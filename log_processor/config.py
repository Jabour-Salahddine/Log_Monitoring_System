'''
# log_processor/config.py  : version 1 1ere categorie :
KAFKA_BROKER_URL = 'localhost:29092' # Accessible depuis l'HÔTE via le port mappé
KAFKA_TOPIC = 'xampp_mysql_server.appdb.app_logs' # !! Correspondre au topic Debezium !!
ELASTICSEARCH_HOST = 'localhost' #ELASTICSEARCH_HOST = 'localhost:9200' # Accessible depuis l'HÔTE via le port mappé
ELASTICSEARCH_INDEX = 'app-logs-index-xampp' # nom d'index : comme une table en sql ou bien une collection en mongodb
REDIS_HOST = '127.0.0.1' #REDIS_HOST = 'localhost' # Accessible depuis l'HÔTE via le port mappé
REDIS_PORT = 6379
ANOMALY_ERROR_THRESHOLD = 3  # déclencher une alerte si 3 erreurs similaires en moins de 60 sec
ANOMALY_TIME_WINDOW_SECONDS = 60 
'''


# log_processor/config.py    : version 2 


# --- Kafka Configuration ---
KAFKA_BROKER_URL = 'localhost:29092' # Accessible depuis l'HÔTE via le port mappé

# Renommé pour plus de clarté : topic pour les logs applicatifs
KAFKA_LOG_TOPIC = 'xampp_mysql_server.appdb.app_logs' # !! Correspondre au topic Debezium pour app_logs !!

# NOUVEAU : Topic pour les événements du panier d'achat
# Assurez-vous que ce nom correspond EXACTEMENT au topic créé par Debezium pour la table 'cart_events'
KAFKA_CART_EVENT_TOPIC = 'xampp_mysql_server.appdb.cart_events'

# --- Elasticsearch Configuration ---
ELASTICSEARCH_HOST = 'localhost'    # Accessible depuis l'HÔTE
ELASTICSEARCH_PORT = 9200           # Port standard d'Elasticsearch (ajouté pour clarté)

# Renommé pour plus de clarté : nom d'index pour les logs applicatifs
ELASTICSEARCH_LOG_INDEX = 'app-logs-index-xampp'

# NOUVEAU : Nom d'index pour les événements du panier d'achat
ELASTICSEARCH_CART_INDEX = 'cart-events-index' # Vous pouvez choisir un autre nom si vous préférez

# --- Redis Configuration (Utilisé par Celery Broker ET Anomaly Detection sur les logs) ---
REDIS_HOST = '127.0.0.1' # 'localhost' fonctionne aussi
REDIS_PORT = 6379

# --- Anomaly Detection Configuration (pour les logs de niveau ERREUR uniquement) ---
ANOMALY_ERROR_THRESHOLD = 3         # Déclencher une alerte si >= 3 erreurs dans la fenêtre de temps
ANOMALY_TIME_WINDOW_SECONDS = 60    # Fenêtre de temps (en secondes) pour compter les erreurs

# --- Configuration Minimale pour Test (inchangée, reste commentée) ---
