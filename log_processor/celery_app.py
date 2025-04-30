
# log_processor/celery_app.py
from celery import Celery # type: ignore
from . import config #import config

# Utiliser Redis comme broker (plus stable avec Celery)
'''
#broker_url = f"redis://{config.REDIS_HOST}:{config.REDIS_PORT}/0"
#result_backend_url = f"redis://{config.REDIS_HOST}:{config.REDIS_PORT}/1"
'''

# ---- CHANGER ICI ----
broker_url = f"redis://127.0.0.1:{config.REDIS_PORT}/0"
result_backend_url = f"redis://127.0.0.1:{config.REDIS_PORT}/1"
# ---------------------


app = Celery(
    'log_tasks',
    broker=broker_url,
    backend=result_backend_url,
    include=['log_processor.tasks'], # Module où trouver les tâches: tasks.py
    # Ajouter ces lignes :
    task_default_queue='logs',
    task_create_missing_queues=True,
    worker_send_task_events=True
)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

print(f"Celery app configured with broker: {app.conf.broker_url}")








 # pour faire un simple test : 

'''
# log_processor/celery_app.py
from celery import Celery # type: ignore
from . import config 

# --- Configuration du Broker Redis ---
# Utilisation de 127.0.0.1 pour être explicite (pointe vers la machine hôte)
broker_url = f"redis://{config.REDIS_HOST}:{config.REDIS_PORT}/0"
result_backend_url = f"redis://{config.REDIS_HOST}:{config.REDIS_PORT}/1" # Backend aussi sur Redis DB 1

# --- Instance de l'application Celery ---
app = Celery(
    'log_tasks', # Nom de l'application Celery
    broker=broker_url,
    backend=result_backend_url,
    include=['log_processor.tasks'], # IMPORTANT: Pointer vers le module contenant les tâches
    # --- Configuration de la file d'attente ---
    task_default_queue='logs',       # Nom de la file d'attente par défaut pour les tâches
    task_create_missing_queues=True, # Crée la file si elle n'existe pas
)

# --- Configuration générale de Celery ---
app.conf.update(
    task_serializer='json',         # Comment les données de tâche sont sérialisées (avant envoi)
    accept_content=['json'],        # Quels types de contenu le worker accepte
    result_serializer='json',       # Comment les résultats de tâche sont sérialisés
    timezone='UTC',
    enable_utc=True,
    worker_send_task_events=True    # Utile pour la surveillance (ex: Flower)
)

# --- Message de confirmation au démarrage (visible si ce module est importé) ---
print(f"--- Celery App Instance Created ---")
print(f"Broker URL: {app.conf.broker_url}")
print(f"Default Queue: {app.conf.task_default_queue}")
print(f"Included Task Modules: {app.conf.include}")
print(f"---------------------------------")
'''