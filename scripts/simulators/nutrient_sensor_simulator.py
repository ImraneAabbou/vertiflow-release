#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PROJET VERTIFLOW - Agriculture Verticale Intelligente
================================================================================
Date de creation    : 04/01/2026
Equipe              : VertiFlow Core Team

Membres de l'equipe :
    Mounir      - Architecte & Scientifique
    Imrane      - DevOps & Infrastructure
    Mouhammed   - Data Engineer & Analyste ETL
    Asama       - Biologiste & Domain Expert
    MrZakaria   - Encadrant & Architecte Data

--------------------------------------------------------------------------------
MODULE: nutrient_sensor_simulator.py
DESCRIPTION: Simulateur de donnees nutritives et rhizosphere pour hydroponique

Fonctionnalites principales:
    - Simule les capteurs EC, pH, DO (oxygene dissous)
    - Genere les concentrations NPK et micronutriments
    - Modelise la dynamique de la solution nutritive
    - Gestion des alertes de desequilibre
    - Publication MQTT et fichiers JSON pour NiFi

Colonnes ClickHouse couvertes:
    II. NUTRITION MINERALE (15 colonnes):
        - nutrient_n_total, nutrient_p_phosphorus, nutrient_k_potassium
        - nutrient_ca_calcium, nutrient_mg_magnesium, nutrient_s_sulfur
        - nutrient_fe_iron, nutrient_mn_manganese, nutrient_zn_zinc
        - nutrient_cu_copper, nutrient_b_boron, nutrient_mo_molybdenum
        - nutrient_cl_chlorine, nutrient_ni_nickel, nutrient_solution_ec

    VII. RHIZOSPHERE & EAU (15 colonnes):
        - water_temp, water_ph, dissolved_oxygen, water_turbidity
        - wue_current, water_recycled_rate, coefficient_cultural_kc
        - microbial_density, beneficial_microbes_ratio, root_fungal_pressure
        - biofilm_thickness, algae_growth_index, redox_potential
        - irrigation_line_pressure, leaching_fraction

Developpe par        : @Mouhammed (amelioration automatique)
Ticket(s) associe(s): TICKET-056
Sprint              : Semaine 6 - Completude Donnees

================================================================================
2025-2026 VertiFlow Core Team - Tous droits reserves
================================================================================
"""

import json
import random
import time
import os
import math
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

# Configuration pour MQTT (optionnel)
try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False
    print("Warning: paho-mqtt non installe. Mode fichier uniquement.")

# ============================================================================
# CONFIGURATION
# ============================================================================

BROKER = "localhost"
PORT = 1883
TOPIC_BASE = "vertiflow/telemetry/nutrient"

# Dossier de sortie pour NiFi
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                          "data_ingestion", "nutrient_data")

# Configuration de la ferme
FARM_CONFIG = {
    "farm_id": "VERT-MAROC-01",
    "tanks": ["TANK_A", "TANK_B", "TANK_RESERVE"],
    "irrigation_zones": ["ZONE_GERMINATION", "ZONE_CROISSANCE", "ZONE_FLORAISON"],
    "racks_per_zone": {
        "ZONE_GERMINATION": ["R01", "R02"],
        "ZONE_CROISSANCE": ["R03", "R04"],
        "ZONE_FLORAISON": ["R05"]
    }
}

# ============================================================================
# RECETTES NUTRITIVES (Basees sur Cornell CEA & HydroBuddy)
# ============================================================================

NUTRIENT_RECIPES = {
    "RECIPE_SEEDLING": {
        "id": "NUT-001",
        "name": "Solution Germination",
        "description": "Solution diluee pour jeunes plants",
        "ec_target": 1.0,  # mS/cm
        "ph_target": 5.8,
        "concentrations_ppm": {
            "N": 100,
            "P": 30,
            "K": 120,
            "Ca": 80,
            "Mg": 25,
            "S": 30,
            "Fe": 2.5,
            "Mn": 0.5,
            "Zn": 0.3,
            "Cu": 0.05,
            "B": 0.3,
            "Mo": 0.05,
            "Cl": 5,
            "Ni": 0.01
        }
    },
    "RECIPE_VEGETATIVE": {
        "id": "NUT-002",
        "name": "Solution Croissance",
        "description": "Solution equilibree pour croissance vegetative",
        "ec_target": 1.6,
        "ph_target": 6.0,
        "concentrations_ppm": {
            "N": 150,
            "P": 50,
            "K": 200,
            "Ca": 120,
            "Mg": 40,
            "S": 50,
            "Fe": 3.0,
            "Mn": 0.8,
            "Zn": 0.4,
            "Cu": 0.08,
            "B": 0.5,
            "Mo": 0.08,
            "Cl": 8,
            "Ni": 0.02
        }
    },
    "RECIPE_FLOWERING": {
        "id": "NUT-003",
        "name": "Solution Floraison",
        "description": "Solution riche en K et P pour floraison",
        "ec_target": 2.0,
        "ph_target": 6.2,
        "concentrations_ppm": {
            "N": 120,
            "P": 80,
            "K": 280,
            "Ca": 140,
            "Mg": 50,
            "S": 60,
            "Fe": 3.5,
            "Mn": 1.0,
            "Zn": 0.5,
            "Cu": 0.10,
            "B": 0.6,
            "Mo": 0.10,
            "Cl": 10,
            "Ni": 0.03
        }
    },
    "RECIPE_FINISHING": {
        "id": "NUT-004",
        "name": "Solution Finition",
        "description": "Solution reduite pour concentration aromes",
        "ec_target": 1.2,
        "ph_target": 5.8,
        "concentrations_ppm": {
            "N": 80,
            "P": 40,
            "K": 150,
            "Ca": 100,
            "Mg": 30,
            "S": 35,
            "Fe": 2.0,
            "Mn": 0.5,
            "Zn": 0.3,
            "Cu": 0.05,
            "B": 0.3,
            "Mo": 0.05,
            "Cl": 5,
            "Ni": 0.01
        }
    }
}

# Mapping zone -> recette
ZONE_RECIPE_MAP = {
    "ZONE_GERMINATION": "RECIPE_SEEDLING",
    "ZONE_CROISSANCE": "RECIPE_VEGETATIVE",
    "ZONE_FLORAISON": "RECIPE_FLOWERING"
}


# ============================================================================
# FONCTIONS DE SIMULATION
# ============================================================================

def simulate_ec_drift(base_ec: float, hours_since_change: float) -> float:
    """
    Simule la derive de l'EC due a l'absorption des plantes.
    L'EC augmente legerement car l'eau est absorbee plus vite que les sels.
    """
    # Derive positive de ~0.05 mS/cm par heure d'utilisation
    drift = hours_since_change * 0.02 * random.uniform(0.8, 1.2)
    return round(base_ec + drift, 2)


def simulate_ph_drift(base_ph: float, hours_since_change: float) -> float:
    """
    Simule la derive du pH.
    Tendance a monter due a l'absorption des anions.
    """
    # Derive positive de ~0.03 pH par heure
    drift = hours_since_change * 0.015 * random.uniform(0.8, 1.2)
    return round(base_ph + drift, 2)


def calculate_do_saturation(temp: float) -> float:
    """
    Calcule la saturation en oxygene dissous en fonction de la temperature.
    Formule basee sur la loi de Henry.
    """
    # DO saturation (mg/L) diminue avec la temperature
    # A 20C: ~9.1 mg/L, A 25C: ~8.2 mg/L
    do_sat = 14.6 - 0.41 * temp + 0.008 * (temp ** 2) - 0.00008 * (temp ** 3)
    return max(5.0, min(12.0, do_sat))


def simulate_microbial_activity(temp: float, do: float, ph: float) -> Dict[str, float]:
    """
    Simule l'activite microbienne dans la rhizosphere.
    """
    # Activite optimale entre 20-28C, pH 5.5-6.5, DO > 5 mg/L
    temp_factor = 1 - abs(temp - 24) / 20
    ph_factor = 1 - abs(ph - 6.0) / 2
    do_factor = min(1.0, do / 8)

    activity_index = temp_factor * ph_factor * do_factor

    return {
        "microbial_density": round(random.gauss(500000, 50000) * activity_index, 0),  # UFC/mL
        "beneficial_microbes_ratio": round(random.gauss(0.85, 0.05) * activity_index, 3),
        "root_fungal_pressure": round(random.gauss(0.1, 0.02), 3),
        "biofilm_thickness": round(random.gauss(0.5, 0.1), 2),  # mm
        "algae_growth_index": round(random.gauss(0.2, 0.05), 2)
    }


def generate_nutrient_telemetry(zone_id: str, tank_id: str) -> Dict[str, Any]:
    """
    Genere les donnees de telemetrie nutritive completes.
    """
    now = datetime.now(timezone.utc)
    hour = datetime.now().hour

    # Selectionner la recette
    recipe_key = ZONE_RECIPE_MAP.get(zone_id, "RECIPE_VEGETATIVE")
    recipe = NUTRIENT_RECIPES[recipe_key]

    # Simuler derive temporelle (heures depuis derniere calibration)
    hours_since_calibration = random.uniform(0, 12)

    # Temperature de l'eau (varie selon l'heure du jour)
    water_temp_base = 20 if 6 <= hour < 18 else 18
    water_temp = round(random.gauss(water_temp_base, 0.5), 2)

    # EC avec derive
    ec_base = recipe["ec_target"]
    ec = simulate_ec_drift(ec_base, hours_since_calibration)
    ec = round(ec * random.uniform(0.95, 1.05), 2)

    # pH avec derive
    ph_base = recipe["ph_target"]
    ph = simulate_ph_drift(ph_base, hours_since_calibration)
    ph = round(ph * random.uniform(0.98, 1.02), 2)

    # Oxygene dissous
    do_saturation = calculate_do_saturation(water_temp)
    # Aeration active -> proche de la saturation
    dissolved_oxygen = round(do_saturation * random.uniform(0.85, 0.98), 2)

    # Concentrations NPK et micronutriments (avec variation ±10%)
    concentrations = {}
    for nutrient, target in recipe["concentrations_ppm"].items():
        variation = random.uniform(0.90, 1.10)
        concentrations[nutrient] = round(target * variation, 2)

    # Activite microbienne
    microbial = simulate_microbial_activity(water_temp, dissolved_oxygen, ph)

    # Metriques d'eau
    water_turbidity = round(random.gauss(1.0, 0.3), 2)  # NTU
    redox_potential = round(random.gauss(250, 20), 1)    # mV (oxydant)
    irrigation_pressure = round(random.gauss(2.5, 0.1), 2)  # Bar

    # Efficacite d'utilisation de l'eau (WUE)
    wue = round(random.gauss(4.5, 0.3), 2)  # g biomasse / L eau

    # Taux de recyclage
    water_recycled_rate = round(random.gauss(95, 2), 1)  # %

    # Coefficient cultural (Kc) - varie selon le stade
    kc_values = {
        "ZONE_GERMINATION": 0.4,
        "ZONE_CROISSANCE": 0.8,
        "ZONE_FLORAISON": 1.0
    }
    kc = round(kc_values.get(zone_id, 0.8) * random.uniform(0.95, 1.05), 2)

    # Fraction de lessivage
    leaching_fraction = round(random.uniform(0.10, 0.20), 2)

    # Detection d'anomalies
    anomaly_score = 0.0
    data_flag = 0

    # Alertes EC
    if ec < 0.8 or ec > 2.5:
        anomaly_score = 0.8
        data_flag = 1

    # Alertes pH
    if ph < 5.0 or ph > 7.0:
        anomaly_score = max(anomaly_score, 0.9)
        data_flag = 1

    telemetry = {
        # Identification
        "timestamp": now.astimezone(timezone.utc).isoformat().replace("T", " "),
        "farm_id": FARM_CONFIG["farm_id"],
        "zone_id": zone_id,
        "tank_id": tank_id,
        "sensor_hardware_id": f"NUT-{tank_id}-{zone_id[:4]}",
        "data_source_type": "IoT",
        "nutrient_recipe_id": recipe["id"],

        # II. NUTRITION MINERALE (15 colonnes)
        "nutrient_n_total": concentrations["N"],
        "nutrient_p_phosphorus": concentrations["P"],
        "nutrient_k_potassium": concentrations["K"],
        "nutrient_ca_calcium": concentrations["Ca"],
        "nutrient_mg_magnesium": concentrations["Mg"],
        "nutrient_s_sulfur": concentrations["S"],
        "nutrient_fe_iron": concentrations["Fe"],
        "nutrient_mn_manganese": concentrations["Mn"],
        "nutrient_zn_zinc": concentrations["Zn"],
        "nutrient_cu_copper": concentrations["Cu"],
        "nutrient_b_boron": concentrations["B"],
        "nutrient_mo_molybdenum": concentrations["Mo"],
        "nutrient_cl_chlorine": concentrations["Cl"],
        "nutrient_ni_nickel": concentrations["Ni"],
        "nutrient_solution_ec": ec,

        # VII. RHIZOSPHERE & EAU (15 colonnes)
        "water_temp": water_temp,
        "water_ph": ph,
        "dissolved_oxygen": dissolved_oxygen,
        "water_turbidity": water_turbidity,
        "wue_current": wue,
        "water_recycled_rate": water_recycled_rate,
        "coefficient_cultural_kc": kc,
        "microbial_density": microbial["microbial_density"],
        "beneficial_microbes_ratio": microbial["beneficial_microbes_ratio"],
        "root_fungal_pressure": microbial["root_fungal_pressure"],
        "biofilm_thickness": microbial["biofilm_thickness"],
        "algae_growth_index": microbial["algae_growth_index"],
        "redox_potential": redox_potential,
        "irrigation_line_pressure": irrigation_pressure,
        "leaching_fraction": leaching_fraction,

        # Metriques additionnelles
        "do_saturation_pct": round((dissolved_oxygen / do_saturation) * 100, 1),
        "ec_target": ec_base,
        "ph_target": ph_base,
        "hours_since_calibration": round(hours_since_calibration, 1),

        # Cibles referentielles (XI)
        "ref_n_target": recipe["concentrations_ppm"]["N"],
        "ref_p_target": recipe["concentrations_ppm"]["P"],
        "ref_k_target": recipe["concentrations_ppm"]["K"],
        "ref_ca_target": recipe["concentrations_ppm"]["Ca"],
        "ref_mg_target": recipe["concentrations_ppm"]["Mg"],

        # Qualite donnees
        "data_integrity_flag": data_flag,
        "anomaly_confidence_score": anomaly_score
    }

    return telemetry


def connect_mqtt() -> Optional[mqtt.Client]:
    """Etablit la connexion MQTT."""
    if not MQTT_AVAILABLE:
        return None

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="VertiFlow_Nutrient_Simulator"
    )
    try:
        client.connect(BROKER, PORT, 60)
        return client
    except Exception as e:
        print(f"Erreur connexion MQTT: {e}")
        return None


def save_to_file(data: Dict[str, Any], output_dir: str) -> str:
    """Sauvegarde les donnees dans un fichier JSON pour NiFi."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"nutrient_{data['zone_id']}_{data['tank_id']}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return filepath


def main():
    """Boucle principale de simulation."""
    print("=" * 70)
    print("VERTIFLOW - Simulateur Capteurs Nutriments & Rhizosphere")
    print("=" * 70)

    # Connexion MQTT (optionnelle)
    mqtt_client = connect_mqtt()
    if mqtt_client:
        print(f"Connecte au broker MQTT {BROKER}:{PORT}")
    else:
        print("Mode fichier uniquement (MQTT non disponible)")

    print(f"Dossier de sortie NiFi: {OUTPUT_DIR}")
    print(f"Zones simulees: {FARM_CONFIG['irrigation_zones']}")
    print(f"Tanks: {FARM_CONFIG['tanks']}")
    print("-" * 70)

    iteration = 0

    try:
        while True:
            iteration += 1
            batch_data = []

            for zone in FARM_CONFIG["irrigation_zones"]:
                # Chaque zone a un tank principal
                tank = FARM_CONFIG["tanks"][FARM_CONFIG["irrigation_zones"].index(zone) % len(FARM_CONFIG["tanks"])]

                # Generer telemetrie
                telemetry = generate_nutrient_telemetry(zone, tank)
                batch_data.append(telemetry)

                # Publication MQTT
                if mqtt_client:
                    topic = f"{TOPIC_BASE}/{zone}/{tank}"
                    mqtt_client.publish(topic, json.dumps(telemetry))

                # Sauvegarde fichier (tous les 5 iterations)
                if iteration % 5 == 0:
                    save_to_file(telemetry, OUTPUT_DIR)

            # Log echantillon
            sample = batch_data[0]
            status = "OK" if sample["data_integrity_flag"] == 0 else "ALERT"
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"EC: {sample['nutrient_solution_ec']:.2f} mS | "
                  f"pH: {sample['water_ph']:.2f} | "
                  f"DO: {sample['dissolved_oxygen']:.1f} mg/L | "
                  f"N: {sample['nutrient_n_total']:.0f} ppm | "
                  f"K: {sample['nutrient_k_potassium']:.0f} ppm | "
                  f"[{status}]")

            # Frequence: toutes les 10 secondes
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nArret du simulateur Nutriments.")
        if mqtt_client:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
