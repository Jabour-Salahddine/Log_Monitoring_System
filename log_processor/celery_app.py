

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














 