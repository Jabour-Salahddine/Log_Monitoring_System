# cart_event_generator.py
import mysql.connector # Assurez-vous d'avoir installé: pip install mysql-connector-python
import time
import random
import os
import signal
import json # Pour le champ 'details'

# --- Configuration de la Base de Données XAMPP ---
# !!! IMPORTANT: Vérifiez et adaptez ces informations à VOTRE XAMPP !!!
DB_CONFIG = {
    'user': 'root',         # Votre utilisateur MySQL (souvent 'root' pour XAMPP)
    'password': '',         # Votre mot de passe MySQL (souvent vide pour XAMPP)
    'host': '127.0.0.1',    # Ou 'localhost'
    'port': 3306,
    'database': 'appdb'     # Le nom de votre base de données
}

# Types d'événements panier que nous allons simuler
EVENT_TYPES = [
    'ADD_TO_CART',
    'REMOVE_FROM_CART',
    'UPDATE_QUANTITY',
    'VIEW_CART',
    'START_CHECKOUT',
    'APPLY_COUPON',
    'CLEAR_CART'
]

# Données de simulation
SAMPLE_USER_IDS = [f"user_{random.randint(100, 120)}" for _ in range(5)] # Petit pool d'utilisateurs pour voir des répétitions
SAMPLE_SESSION_IDS = [f"sess_{random.randint(1000, 1020)}" for _ in range(5)]
SAMPLE_PRODUCT_IDS = [f"prod_{random.randint(200, 210)}" for _ in range(7)] # Plus de produits
SAMPLE_COUPON_CODES = ["nawfalCoupon1", "nawfalCoupon2", "nawfalCoupon3", None, None] # None pour simuler pas de coupon

# Variable globale pour contrôler la boucle principale
running = True

def signal_handler(sig, frame):
    """Gère le signal d'arrêt (Ctrl+C)."""
    global running
    print("\nSignal reçu, arrêt du générateur d'événements panier...")
    running = False

def generate_cart_event_data():
    """Génère des données aléatoires pour un événement panier."""
    event_type = random.choice(EVENT_TYPES)
    user_id = random.choice(SAMPLE_USER_IDS)
    session_id = random.choice(SAMPLE_SESSION_IDS)

    product_id = None
    quantity = None
    unit_price = None
    details_dict = {} # Pour le champ JSON 'details'

    if event_type in ['ADD_TO_CART', 'REMOVE_FROM_CART', 'UPDATE_QUANTITY']:
        product_id = random.choice(SAMPLE_PRODUCT_IDS)
        quantity = random.randint(1, 5) if event_type != 'REMOVE_FROM_CART' else 1
        unit_price = round(random.uniform(5.99, 199.99), 2)
        if event_type == 'REMOVE_FROM_CART':
            details_dict['reason'] = random.choice(["changed_mind", "found_better_price", None])

    elif event_type == 'VIEW_CART':
        details_dict['item_count'] = random.randint(0, 10)
        details_dict['total_value'] = round(random.uniform(0, 500.00), 2) if details_dict['item_count'] > 0 else 0.00

    elif event_type == 'START_CHECKOUT':
        details_dict['item_count'] = random.randint(1, 8) # On ne commence pas un checkout avec un panier vide
        details_dict['total_value'] = round(random.uniform(10.00, 800.00), 2)
        details_dict['payment_method_preview'] = random.choice(["visa", "paypal", None])

    elif event_type == 'APPLY_COUPON':
        coupon_code = random.choice(SAMPLE_COUPON_CODES)
        if coupon_code: # S'assurer qu'un coupon est appliqué
            details_dict['coupon_code'] = coupon_code
            details_dict['discount_applied_percent'] = random.choice([5, 10, 15, 20])
        else: # Si pas de coupon choisi, on peut changer l'event_type ou ne rien faire
            # Pour cet exemple, on force un coupon si l'event est APPLY_COUPON
            details_dict['coupon_code'] = "DEFAULT_COUPON"
            details_dict['discount_applied_percent'] = 5


    elif event_type == 'CLEAR_CART':
        details_dict['items_cleared_count'] = random.randint(0, 10)

    # Convertir le dictionnaire de détails en chaîne JSON, ou None si vide
    details_json = json.dumps(details_dict) if details_dict else None

    # Note: event_timestamp sera géré par MySQL (DEFAULT CURRENT_TIMESTAMP)
    return {
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id, # Sera None pour certains types d'événements
        "quantity": quantity,     # Sera None pour certains types d'événements
        "unit_price": unit_price, # Sera None pour certains types d'événements
        "details": details_json   # Sera None si details_dict est vide
    }

def insert_cart_event(cursor, event_data):
    """Insère un événement panier dans la base de données."""
    # La colonne event_timestamp est gérée par MySQL (DEFAULT CURRENT_TIMESTAMP)
    sql = """
    INSERT INTO cart_events
    (event_type, user_id, session_id, product_id, quantity, unit_price, details)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    val = (
        event_data["event_type"],
        event_data["user_id"],
        event_data["session_id"],
        event_data["product_id"],
        event_data["quantity"],
        event_data["unit_price"],
        event_data["details"]
    )
    try:
        cursor.execute(sql, val)
        # Pas besoin de commit explicite si autocommit=True pour la connexion
        print(f"Inserted event: Type={event_data['event_type']}, User={event_data['user_id']}, Prod={event_data['product_id'] if event_data['product_id'] else 'N/A'}")
    except mysql.connector.Error as err:
        print(f"!!! Erreur d'insertion SQL pour cart_event: {err}")
        print(f"    Data: {val}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    connection = None
    cursor = None
    event_count = 0
    start_time = time.time()

    print("--- Générateur d'Événements Panier ---")
    print(f"Configuration DB: User='{DB_CONFIG['user']}', Host='{DB_CONFIG['host']}', DB='{DB_CONFIG['database']}'")
    print(f"!!! VÉRIFIEZ QUE CES IDENTIFIANTS SONT CORRECTS POUR VOTRE XAMPP/MySQL !!!")

    try:
        connection = mysql.connector.connect(**DB_CONFIG, autocommit=True)
        cursor = connection.cursor()
        print(">>> Connexion réussie à la base de données.")
        print("Démarrage de la génération d'événements panier (un toutes les 30s)... Appuyez sur Ctrl+C pour arrêter.")

        while running:
            event_data = generate_cart_event_data()
            insert_cart_event(cursor, event_data)
            event_count += 1
            
            # Attendre 30 secondes avant le prochain événement
            # Gérer l'interruption pendant le sleep
            for _ in range(10): # Découper le sleep pour vérifier 'running' plus souvent
                 if not running: break
                 time.sleep(0.1)
            if not running: break


    except mysql.connector.Error as err:
        print(f"\n!!! Erreur de connexion/opération MySQL: {err}")
    except Exception as e:
        print(f"\nUne erreur inattendue est survenue : {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n--- Arrêt du générateur d'événements panier ---")
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
        
        end_time = time.time()
        duration = end_time - start_time
        if duration > 0.1 : # Éviter division par zéro si arrêté trop vite
            print(f"Nombre total d'événements générés : {event_count}")
            print(f"Durée d'exécution : {duration:.2f} secondes")
            print(f"Taux moyen : {event_count / duration:.2f} événements/sec")
        else:
            print(f"Générateur arrêté avant la première insertion ou après {event_count} événement(s).")