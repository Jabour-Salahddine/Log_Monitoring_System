# log_generator.py (à la racine du projet)
import mysql.connector # type: ignore
import time
import random
import os
import signal # Pour gérer l'arrêt propre

# --- Configuration de la Base de Données XAMPP ---
# Assurez-vous que ces informations correspondent à votre XAMPP
DB_CONFIG = {
    'user': 'root',             # Utilisateur MySQL XAMPP (souvent 'root')
    'password': '',             # Mot de passe MySQL XAMPP (souvent vide par défaut)
    'host': '127.0.0.1',        # Adresse locale de MySQL
    'port': 3306,               # Port par défaut de MySQL
    'database': 'appdb'         # La base de données créée
}

LOG_LEVELS = ['INFO', 'INFO', 'INFO', 'INFO', 'WARNING', 'ERROR', 'ERROR'] # Un peu plus d'erreurs
SAMPLE_MESSAGES = [
    "User login attempt: {}",
    "Product search: term='{}'",
    "Item added to wishlist: SKU={}",
    "API endpoint /users/{} accessed",
    "Payment verification needed for order: {}",
    "WARN: Cache miss for key: {}",
    "ERROR: Database connection timeout",
    "ERROR: Invalid user input detected: {}",
    "ERROR: Third-party service unavailable",
    "User {} updated profile successfully",
    "File upload started: {}",
    "CRITICAL: Security breach attempt detected from IP: {}" # Ajout d'un message critique
]

# Variable globale pour contrôler la boucle principale
running = True

def signal_handler(sig, frame):
    """Gère le signal d'arrêt (Ctrl+C)."""
    global running
    print("\nSignal reçu, arrêt du générateur de logs...")
    running = False

def generate_log_entry():
    """Génère un log aléatoire."""
    level = random.choice(LOG_LEVELS)
    user_id = f"user_{random.randint(100, 999)}"
    item_id = random.randint(1000, 9999)
    ip_address = f"10.0.{random.randint(1,254)}.{random.randint(1,254)}"
    search_term = random.choice(["laptop", "shoes", "book", "phone"])

    # Choisir un message et le remplir
    message_template = random.choice(SAMPLE_MESSAGES)
    try:
        if "user" in message_template.lower() or "input" in message_template.lower():
            message = message_template.format(user_id)
        elif "item" in message_template.lower() or "sku" in message_template.lower() or "order" in message_template.lower():
            message = message_template.format(item_id)
        elif "term" in message_template.lower():
            message = message_template.format(search_term)
        elif "ip" in message_template.lower():
             message = message_template.format(ip_address)
        elif "key" in message_template.lower():
             message = message_template.format(f"cache_key_{random.randint(1,100)}")
        elif "{}" in message_template: # Placeholder générique
             message = message_template.format(random.randint(10000, 99999))
        else: # Pas de placeholder
            message = message_template
    except Exception as e:
        # print(f"Erreur formatage message: {e}")
        message = message_template # Utiliser le message non formaté en cas d'erreur

    # Limiter la longueur du message pour éviter les erreurs SQL
    max_len = 250
    message = (message[:max_len] + '...') if len(message) > max_len else message

    return level, message

def insert_log(cursor, level, message):
    """Insère un log dans la base de données."""
    sql = "INSERT INTO app_logs (level, message) VALUES (%s, %s)"
    val = (level, message)
    try:
        cursor.execute(sql, val)
        # Pas besoin de commit explicite si autocommit=True
        # print(f"Inserted: Level={level}, Msg='{message}'") # Debug
    except mysql.connector.Error as err:
        print(f"!!! Erreur d'insertion SQL: {err}")
        print(f"    Data: Level={level}, Message='{message}' (Len={len(message)})")


if __name__ == "__main__":
    # Enregistrer le gestionnaire de signal pour Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    connection = None
    cursor = None
    log_count = 0
    start_time = time.time()

    print("--- Log Generator ---")
    print("Tentative de connexion à la base de données XAMPP MySQL...")
    print(f"Config: User='{DB_CONFIG['user']}', Host='{DB_CONFIG['host']}', DB='{DB_CONFIG['database']}'")

    try:
        connection = mysql.connector.connect(**DB_CONFIG, autocommit=True) # Activer autocommit
        cursor = connection.cursor()
        print(">>> Connexion réussie à la base de données.")
        print("Démarrage de la génération de logs... Appuyez sur Ctrl+C pour arrêter.")

        while running:
            level, message = generate_log_entry()
            insert_log(cursor, level, message)
            log_count += 1

            # Attente variable pour simuler une charge
            sleep_time = random.uniform(0.2, 2.5) # Délai de base
            if level == 'ERROR' and random.random() < 0.3: # Chance d'avoir une rafale d'erreurs
                sleep_time = random.uniform(0.05, 0.3)
                # print("-> Rafale d'erreurs simulée")

            try:
                time.sleep(sleep_time)
            except InterruptedError: # Gérer l'interruption pendant le sleep
                 running = False # Arrêter proprement si sleep est interrompu par le signal

    except mysql.connector.Error as err:
        print(f"\n!!! Erreur de connexion à la base de données: {err}")
        print("!!! Vérifiez que MySQL (XAMPP) est démarré et que les identifiants sont corrects.")
        print("!!! Assurez-vous que le nom d'utilisateur et le mot de passe dans DB_CONFIG sont corrects pour VOTRE XAMPP.")
    except Exception as e:
        print(f"\nUne erreur inattendue est survenue : {e}")
    finally:
        print("\n--- Arrêt du générateur ---")
        if cursor:
            cursor.close()
            # print("Curseur MySQL fermé.")
        if connection and connection.is_connected():
            connection.close()
            # print("Connexion MySQL fermée.")
        end_time = time.time()
        duration = end_time - start_time
        print(f"Nombre total de logs générés : {log_count}")
        if duration > 0:
             print(f"Durée d'exécution : {duration:.2f} secondes")
             print(f"Taux moyen : {log_count / duration:.2f} logs/sec")
        print("Générateur arrêté.")