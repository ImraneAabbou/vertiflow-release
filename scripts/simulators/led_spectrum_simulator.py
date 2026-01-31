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
MODULE: led_spectrum_simulator.py
DESCRIPTION: Simulateur de donnees spectrales LED pour agriculture verticale

Fonctionnalites principales:
    - Simule les spectres LED (Rouge, Bleu, Far-Red, Blanc)
    - Genere les metriques PPFD, DLI, ratio R:B, efficacite quantique
    - Gestion des recettes spectrales par phase de croissance
    - Support des cycles photoperiodiques (jour/nuit)
    - Publication MQTT vers le broker interne
    - Generation de fichiers JSON pour NiFi

Colonnes ClickHouse couvertes:
    - light_intensity_ppfd (III.1)
    - light_compensation_point (III.2)
    - light_saturation_point (III.3)
    - light_ratio_red_blue (III.4)
    - light_far_red_intensity (III.5)
    - light_dli_accumulated (III.6)
    - light_photoperiod (III.7)
    - quantum_yield_psii (III.8)
    - photosynthetic_rate_max (III.9)
    - light_use_efficiency (III.13)
    - leaf_absorption_pct (III.14)
    - spectral_recipe_id (III.15)
    - led_driver_temp (IX.3)

Developpe par        : @Mouhammed (amelioration automatique)
Ticket(s) associe(s): TICKET-055
Sprint              : Semaine 6 - Completude Donnees

================================================================================
2025-2026 VertiFlow Core Team - Tous droits reserves
Developpe dans le cadre de l'Initiative Nationale Marocaine JobInTech
au sein de l'Ecole YNOV Maroc Campus
================================================================================
"""

import json
import random
import time
import os
import math
from datetime import datetime, timezone
from typing import Dict, Any, Optional

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
TOPIC_BASE = "vertiflow/telemetry/led"

# Dossier de sortie pour NiFi
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                          "data_ingestion", "led_spectrum")

# Configuration de la ferme
FARM_CONFIG = {
    "farm_id": "VERT-MAROC-01",
    "racks": ["R01", "R02", "R03", "R04", "R05"],
    "levels_per_rack": 4,
    "zones": {
        "R01": "ZONE_GERMINATION",
        "R02": "ZONE_GERMINATION",
        "R03": "ZONE_CROISSANCE",
        "R04": "ZONE_CROISSANCE",
        "R05": "ZONE_FLORAISON"
    }
}

# ============================================================================
# RECETTES SPECTRALES (Basees sur recherche Cornell CEA & Fluence)
# ============================================================================

SPECTRAL_RECIPES = {
    "RECIPE_GERMINATION": {
        "id": "SPR-001",
        "name": "Germination Basilic",
        "description": "Spectre doux pour germination - dominante bleu",
        "ppfd_target": 150,  # umol/m2/s
        "photoperiod": 18,   # heures
        "dli_target": 9.72,  # mol/m2/day
        "spectrum": {
            "blue_450nm_pct": 35,
            "red_660nm_pct": 45,
            "far_red_730nm_pct": 5,
            "white_pct": 15
        },
        "ratio_red_blue": 1.29,
        "phy_ratio": 0.85  # Phytochrome photostationary state
    },
    "RECIPE_VEGETATIVE": {
        "id": "SPR-002",
        "name": "Croissance Vegetative",
        "description": "Spectre equilibre pour croissance foliaire",
        "ppfd_target": 350,
        "photoperiod": 16,
        "dli_target": 20.16,
        "spectrum": {
            "blue_450nm_pct": 25,
            "red_660nm_pct": 55,
            "far_red_730nm_pct": 8,
            "white_pct": 12
        },
        "ratio_red_blue": 2.2,
        "phy_ratio": 0.80
    },
    "RECIPE_FLOWERING": {
        "id": "SPR-003",
        "name": "Induction Florale",
        "description": "Spectre rouge dominant + far-red pour floraison",
        "ppfd_target": 450,
        "photoperiod": 12,
        "dli_target": 19.44,
        "spectrum": {
            "blue_450nm_pct": 15,
            "red_660nm_pct": 60,
            "far_red_730nm_pct": 15,
            "white_pct": 10
        },
        "ratio_red_blue": 4.0,
        "phy_ratio": 0.70
    },
    "RECIPE_FINISHING": {
        "id": "SPR-004",
        "name": "Finition Pre-Recolte",
        "description": "Stress lumineux pour concentration aromes",
        "ppfd_target": 500,
        "photoperiod": 14,
        "dli_target": 25.2,
        "spectrum": {
            "blue_450nm_pct": 30,
            "red_660nm_pct": 50,
            "far_red_730nm_pct": 5,
            "white_pct": 15
        },
        "ratio_red_blue": 1.67,
        "phy_ratio": 0.88
    }
}

# Mapping zone -> recette
ZONE_RECIPE_MAP = {
    "ZONE_GERMINATION": "RECIPE_GERMINATION",
    "ZONE_CROISSANCE": "RECIPE_VEGETATIVE",
    "ZONE_FLORAISON": "RECIPE_FLOWERING"
}


# ============================================================================
# FONCTIONS DE SIMULATION
# ============================================================================

def get_current_light_state() -> Dict[str, Any]:
    """Determine l'etat lumineux base sur l'heure (cycle photoperiode)."""
    now = datetime.now()
    hour = now.hour
    minute = now.minute

    # Cycle standard: lumieres ON de 6h a 22h (16h photoperiode)
    light_on_hour = 6
    light_off_hour = 22

    is_light_on = light_on_hour <= hour < light_off_hour

    # Calcul du temps ecoule dans la periode lumineuse
    if is_light_on:
        hours_since_dawn = hour - light_on_hour + (minute / 60)
    else:
        hours_since_dawn = 0

    return {
        "is_light_on": is_light_on,
        "hours_since_dawn": hours_since_dawn,
        "current_hour": hour,
        "photoperiod_progress_pct": (hours_since_dawn / 16) * 100 if is_light_on else 0
    }


def calculate_dli_accumulated(ppfd: float, hours_elapsed: float) -> float:
    """
    Calcule le DLI accumule.
    DLI (mol/m2/day) = PPFD (umol/m2/s) * heures * 3600 / 1,000,000
    """
    return round((ppfd * hours_elapsed * 3600) / 1_000_000, 2)


def calculate_quantum_yield(ppfd: float, temp: float = 25.0) -> float:
    """
    Calcule l'efficacite quantique du PSII (Fv/Fm).
    Valeur optimale ~0.83, diminue avec stress thermique ou lumineux.
    """
    # Efficacite de base
    base_yield = 0.83

    # Penalite si PPFD trop eleve (photoinhibition)
    if ppfd > 600:
        light_penalty = (ppfd - 600) * 0.0002
    else:
        light_penalty = 0

    # Penalite thermique (optimal 20-25C)
    if temp < 15 or temp > 35:
        temp_penalty = 0.05
    elif temp < 18 or temp > 30:
        temp_penalty = 0.02
    else:
        temp_penalty = 0

    yield_value = base_yield - light_penalty - temp_penalty
    return round(max(0.5, min(0.85, yield_value + random.gauss(0, 0.01))), 3)


def calculate_photosynthetic_rate(ppfd: float, co2: int = 800, temp: float = 25.0) -> float:
    """
    Calcule le taux photosynthetique max (umol CO2/m2/s).
    Modele simplifie base sur Farquhar.
    """
    # Parametres du modele
    Vcmax = 100  # Vitesse max carboxylation
    Km = 400     # Constante Michaelis-Menten pour CO2

    # Effet de la lumiere (saturation vers 400+ PPFD)
    light_factor = 1 - math.exp(-ppfd / 200)

    # Effet CO2 (saturation vers 1000+ ppm)
    co2_factor = co2 / (co2 + Km)

    # Effet temperature (optimum 25C)
    temp_factor = math.exp(-((temp - 25) ** 2) / 200)

    rate = Vcmax * light_factor * co2_factor * temp_factor
    return round(rate + random.gauss(0, 2), 2)


def generate_led_telemetry(rack_id: str, level: int, zone_id: str) -> Dict[str, Any]:
    """
    Genere les donnees de telemetrie LED completes.
    """
    # Obtenir l'etat lumineux actuel
    light_state = get_current_light_state()

    # Selectionner la recette spectrale
    recipe_key = ZONE_RECIPE_MAP.get(zone_id, "RECIPE_VEGETATIVE")
    recipe = SPECTRAL_RECIPES[recipe_key]

    # Simulation de variation realiste
    now = datetime.now(timezone.utc)

    # PPFD avec variation (±5%)
    if light_state["is_light_on"]:
        ppfd_base = recipe["ppfd_target"]
        ppfd = round(ppfd_base * random.uniform(0.95, 1.05), 1)

        # Variation selon position dans le rack (niveaux hauts = plus de lumiere)
        level_factor = 1 + (level - 2) * 0.03
        ppfd = round(ppfd * level_factor, 1)
    else:
        # Nuit: lumiere residuelle minimale
        ppfd = round(random.uniform(0, 2), 1)

    # Temperature du driver LED
    if light_state["is_light_on"]:
        led_temp = round(random.gauss(55, 3), 1)  # Chauffe pendant fonctionnement
    else:
        led_temp = round(random.gauss(25, 2), 1)  # Refroidit la nuit

    # Calculs derives
    dli_accumulated = calculate_dli_accumulated(ppfd, light_state["hours_since_dawn"])
    quantum_yield = calculate_quantum_yield(ppfd)
    photo_rate = calculate_photosynthetic_rate(ppfd)

    # Ratio R:B avec variation
    ratio_rb = round(recipe["ratio_red_blue"] * random.uniform(0.98, 1.02), 2)

    # Far-red intensity (% du PPFD total)
    far_red_pct = recipe["spectrum"]["far_red_730nm_pct"]
    far_red_intensity = round(ppfd * (far_red_pct / 100), 1)

    # Light Use Efficiency (g biomasse / mol photons)
    lue = round(random.gauss(0.8, 0.05), 2) if light_state["is_light_on"] else 0

    # Absorption foliaire (depend de l'age des feuilles)
    leaf_absorption = round(random.gauss(85, 2), 1)

    # Points de compensation et saturation
    light_compensation = round(random.gauss(25, 3), 1)  # PPFD minimum pour photosynthese positive
    light_saturation = round(random.gauss(800, 50), 1)  # PPFD de saturation

    telemetry = {
        # Identification
        "timestamp": now.astimezone(timezone.utc).isoformat().replace("T", " "),
        "farm_id": FARM_CONFIG["farm_id"],
        "rack_id": rack_id,
        "level_index": level,
        "zone_id": zone_id,
        "sensor_hardware_id": f"LED-{rack_id}-L{level}",
        "data_source_type": "IoT",

        # III. PHOTOSYNTHESE & LUMIERE (15 colonnes)
        "light_intensity_ppfd": ppfd,
        "light_compensation_point": light_compensation,
        "light_saturation_point": light_saturation,
        "light_ratio_red_blue": ratio_rb,
        "light_far_red_intensity": far_red_intensity,
        "light_dli_accumulated": dli_accumulated,
        "light_photoperiod": recipe["photoperiod"],
        "quantum_yield_psii": quantum_yield,
        "photosynthetic_rate_max": photo_rate,
        "light_use_efficiency": lue,
        "leaf_absorption_pct": leaf_absorption,
        "spectral_recipe_id": recipe["id"],

        # Spectre detaille (colonnes supplementaires)
        "spectrum_blue_450nm_pct": recipe["spectrum"]["blue_450nm_pct"],
        "spectrum_red_660nm_pct": recipe["spectrum"]["red_660nm_pct"],
        "spectrum_far_red_730nm_pct": recipe["spectrum"]["far_red_730nm_pct"],
        "spectrum_white_pct": recipe["spectrum"]["white_pct"],
        "phytochrome_ratio": round(recipe["phy_ratio"] * random.uniform(0.98, 1.02), 3),

        # Hardware
        "led_driver_temp": led_temp,
        "led_power_consumption_w": round(ppfd * 0.4 + random.gauss(0, 2), 1) if ppfd > 0 else 0,
        "led_hours_total": random.randint(1000, 5000),  # Heures d'utilisation
        "led_efficiency_pct": round(random.gauss(92, 1), 1),

        # Etat du cycle
        "is_light_period": 1 if light_state["is_light_on"] else 0,
        "photoperiod_progress_pct": round(light_state["photoperiod_progress_pct"], 1),

        # Qualite donnees
        "data_integrity_flag": 0,
        "anomaly_confidence_score": 0.0
    }

    return telemetry


def connect_mqtt() -> Optional[mqtt.Client]:
    """Etablit la connexion MQTT."""
    if not MQTT_AVAILABLE:
        return None

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="VertiFlow_LED_Simulator"
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
    filename = f"led_spectrum_{data['rack_id']}_{data['level_index']}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return filepath


def main():
    """Boucle principale de simulation."""
    print("=" * 70)
    print("VERTIFLOW - Simulateur Spectre LED")
    print("=" * 70)

    # Connexion MQTT (optionnelle)
    mqtt_client = connect_mqtt()
    if mqtt_client:
        print(f"Connecte au broker MQTT {BROKER}:{PORT}")
    else:
        print("Mode fichier uniquement (MQTT non disponible)")

    print(f"Dossier de sortie NiFi: {OUTPUT_DIR}")
    print(f"Racks simules: {FARM_CONFIG['racks']}")
    print("-" * 70)

    iteration = 0

    try:
        while True:
            iteration += 1
            batch_data = []

            for rack in FARM_CONFIG["racks"]:
                zone = FARM_CONFIG["zones"].get(rack, "ZONE_CROISSANCE")

                for level in range(1, FARM_CONFIG["levels_per_rack"] + 1):
                    # Generer telemetrie
                    telemetry = generate_led_telemetry(rack, level, zone)
                    batch_data.append(telemetry)

                    # Publication MQTT
                    if mqtt_client:
                        topic = f"{TOPIC_BASE}/{rack}/L{level}"
                        mqtt_client.publish(topic, json.dumps(telemetry))

                    # Sauvegarde fichier (1 fichier sur 10 pour eviter surcharge)
                    if iteration % 10 == 0:
                        save_to_file(telemetry, OUTPUT_DIR)

            # Log echantillon
            sample = batch_data[0]
            light_status = "ON" if sample["is_light_period"] else "OFF"
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"PPFD: {sample['light_intensity_ppfd']:>5.1f} umol | "
                  f"DLI: {sample['light_dli_accumulated']:>5.2f} mol | "
                  f"R:B: {sample['light_ratio_red_blue']:.2f} | "
                  f"LED Temp: {sample['led_driver_temp']:.1f}C | "
                  f"Light: {light_status}")

            # Frequence: toutes les 5 secondes
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nArret du simulateur LED.")
        if mqtt_client:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
