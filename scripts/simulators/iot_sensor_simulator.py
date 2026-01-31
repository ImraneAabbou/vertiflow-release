#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PROJET VERTIFLOW - Agriculture Verticale Intelligente
================================================================================
Date de création    : 25/12/2025
Équipe              : VertiFlow Core Team

Membres de l'équipe :
    🧙‍♂️ Mounir      - Architecte & Scientifique
    🏗️ Imrane      - DevOps & Infrastructure
    🐍 Mouhammed   - Data Engineer & Analyste ETL
    🧬 Asama       - Biologiste & Domain Expert
    ⚖️ MrZakaria    - Encadrant & Architecte Data

--------------------------------------------------------------------------------
MODULE: iot_sensor_simulator.py
DESCRIPTION: Générateur de données synthétiques complet pour capteurs IoT (MQTT)

Fonctionnalités principales:
    - Simule un parc de capteurs complet couvrant les zones clés des 153 colonnes :
      1. Climat (Air)
      2. Rhizosphère (Eau/Racines)
      3. Lumière & Photosynthèse (Spectromètres)
      4. Hardware & Maintenance (Pompes, LED, Filtres)
    - Gestion des cycles jour/nuit pour le réalisme biologique.
    - Injection aléatoire d'anomalies pour tester les alertes (Algo A4/A5).
    - Publication MQTT vers le broker interne.

Développé par        : @Mouhammed
Ticket(s) associé(s): TICKET-040
Sprint              : Semaine 3 - Tests & Simulation

Dépendances:
    - paho-mqtt: Client MQTT Python

================================================================================
© 2025 VertiFlow Core Team - Tous droits réservés
Développé dans le cadre de l'Initiative Nationale Marocaine JobInTech
au sein de l'École YNOV Maroc Campus
================================================================================
"""

import time
import json
import random
import paho.mqtt.client as mqtt
from datetime import datetime, timezone

# Configuration du Broker (Local Docker)
BROKER = "localhost" 
PORT = 1883
TOPIC_BASE = "vertiflow/telemetry"

# Configuration de la Ferme simulée
FARM_CONFIG = {
    "farm_id": "VERT-MAROC-01",
    "racks": ["R01", "R02", "R03", "R04", "R05"],
    "modules_per_rack": 4,
    "zones": ["ZONE_GERMINATION", "ZONE_CROISSANCE"]
}

def connect_mqtt():
    """Établit la connexion avec le broker Mosquitto."""
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="VertiFlow_Simulator_Full",
    )
    try:
        client.connect(BROKER, PORT, 60)
        return client
    except Exception as e:
        print(f"[ERROR] Erreur connexion MQTT: {e}")
        return None

def generate_telemetry(rack_id, module_id, zone_id):
    """
    Génère un payload JSON réaliste couvrant les métriques physiques 
    du Golden Record (153 colonnes).
    Certaines colonnes (Biomasse, API Météo) sont gérées par d'autres sources.
    """
    
    # --- CONTEXTE TEMPOREL (Jour/Nuit) ---
    now = datetime.now()
    hour = now.hour
    is_day = 6 <= hour < 20
    
    # Injection d'anomalie (1 chance sur 500 par message)
    is_anomaly = random.random() < 0.002
    
    # --- I. IDENTIFICATION ---
    telemetry = {
        "timestamp": now.astimezone(timezone.utc).isoformat().replace("T", " "),
        "sensor_hardware_id": f"SN-{rack_id}-{module_id}",
        "rack_id": rack_id,
        "module_id": module_id,
        "zone_id": zone_id,
        "data_source_type": "IoT", # Col 146
        
        # --- VI. ENVIRONNEMENT & CLIMAT ---
        # Simulation d'un cycle thermique (Chaud le jour, Frais la nuit)
        "air_temp_internal": round(random.gauss(24.0 if is_day else 19.0, 0.5), 2) if not is_anomaly else 85.0,
        "air_humidity": round(random.gauss(65.0, 2.0), 2),
        "co2_level_ambient": int(random.gauss(800 if is_day else 450, 30)), # CO2 injecté le jour
        "airflow_velocity": round(random.uniform(0.5, 1.5), 2),
        "air_pressure": round(random.gauss(1013, 5), 1),
        
        # Calculé par NiFi habituellement, mais simulé ici pour test direct
        "vapor_pressure_deficit": round(random.uniform(0.8, 1.2), 2),
        
        # --- VII. RHIZOSPHÈRE & EAU ---
        "water_ph": round(random.gauss(6.0, 0.1), 2), # Cible 5.8 - 6.2
        "nutrient_solution_ec": round(random.gauss(1.8, 0.05), 2), # EC (mS/cm)
        "water_temp": round(random.gauss(20.0, 0.5), 2),
        "dissolved_oxygen": round(random.gauss(8.5, 0.5), 2), # mg/L
        "irrigation_line_pressure": round(random.gauss(2.5, 0.1), 2), # Bar
        "water_turbidity": round(random.uniform(0.1, 2.0), 2), # NTU
        
        # --- II. NUTRITION MINÉRALE (Simulation Sondes Ioniques) ---
        "nutrient_n_total": round(random.gauss(150, 5), 1), # ppm
        "nutrient_p_phosphorus": round(random.gauss(50, 2), 1),
        "nutrient_k_potassium": round(random.gauss(200, 10), 1),
        "nutrient_ca_calcium": round(random.gauss(120, 5), 1),
        "nutrient_mg_magnesium": round(random.gauss(40, 2), 1),
        
        # --- III. PHOTOSYNTHÈSE & LUMIÈRE ---
        # Lumière allumée le jour, éteinte la nuit (ou faible pollution)
        "light_intensity_ppfd": round(random.gauss(450, 20), 1) if is_day else round(random.uniform(0, 5), 1),
        "light_photoperiod": 16.0, # Durée programmée
        "light_ratio_red_blue": round(random.uniform(3.8, 4.2), 2),
        "light_far_red_intensity": round(random.uniform(10, 15), 1) if is_day else 0,
        
        # --- IX. HARDWARE & MAINTENANCE ---
        "pump_vibration_level": round(random.uniform(0.1, 0.5), 2) if not is_anomaly else round(random.uniform(2.0, 5.0), 2),
        "fan_current_draw": round(random.gauss(1.5, 0.1), 2), # Ampères
        "led_driver_temp": round(random.gauss(45.0, 2.0), 1) if is_day else 20.0,
        "filter_differential_pressure": round(random.uniform(0.1, 0.3), 2), # Bar (encrassement)
        "ups_battery_health": round(random.uniform(95, 100), 1), # %
        
        # ALERTE CRITIQUE (Rare)
        "leak_detection_status": 1 if random.random() < 0.0005 else 0,
        "emergency_stop_status": 0
    }
    
    return telemetry

def main():
    """Boucle principale de simulation."""
    client = connect_mqtt()
    if not client: return

    print(f"[START] Demarrage du simulateur VertiFlow sur {BROKER}:{PORT}")
    print(f"[INFO] Simulation de {len(FARM_CONFIG['racks']) * FARM_CONFIG['modules_per_rack']} modules...")
    print("[INFO] Couverture : Climat, Nutrition, Lumiere, Hardware (Cols 11-100+)")

    try:
        while True:
            for rack in FARM_CONFIG["racks"]:
                # Simulation de zone (R01-R02 = Germination, R03+ = Croissance)
                zone = FARM_CONFIG["zones"][0] if rack in ["R01", "R02"] else FARM_CONFIG["zones"][1]
                
                for mod_idx in range(1, FARM_CONFIG["modules_per_rack"] + 1):
                    module_id = f"M{mod_idx:02d}"
                    
                    payload = generate_telemetry(rack, module_id, zone)
                    
                    # Topic structuré : vertiflow/telemetry/R01/M01
                    topic = f"{TOPIC_BASE}/{rack}/{module_id}"
                    
                    # Publication MQTT
                    client.publish(topic, json.dumps(payload))
                    
                    # Log echantillon pour verifier que ca tourne
                    if mod_idx == 1 and rack == "R01":
                        print(f"[MQTT] {topic} | Temp: {payload['air_temp_internal']}C | PPFD: {payload['light_intensity_ppfd']} | N: {payload['nutrient_n_total']} ppm")
            
            # Fréquence de simulation : 1 seconde
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[STOP] Arret du simulateur.")
        client.disconnect()

if __name__ == "__main__":
    main()
