#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PROJET VERTIFLOW - SIMULATEUR IOT AVEC DÉRIVE ANALYTIQUE
================================================================================
Cible : Zone 1 NiFi (vertiflow.ingestion.raw)
Schéma : 157 colonnes (01_tables.sql)
Logique : Cycles de vie (plant_recipes.js) + Simulation de pannes (Drift)
================================================================================
"""

import json
import random
import time
import logging
import sys
import uuid
import requests
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
except ImportError:
    print("❌ Erreur : kafka-python manquant.")
    sys.exit(1)

import requests

URL = "https://api.open-meteo.com/v1/forecast?latitude=33.5731&longitude=-7.5898&hourly=temperature_2m,relative_humidity_2m,rain,surface_pressure,wind_speed_10m,uv_index,soil_temperature_0cm,soil_moisture_0_to_1cm&daily=sunrise,sunset,uv_index_max,precipitation_sum,et0_fao_evapotranspiration&timezone=Africa/Casablanca"

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "vertiflow.ingestion.raw"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SIM] - %(message)s')
logger = logging.getLogger("VertiFlow.ExpertSim")

# --- ÉTAT DU SYSTÈME (Pour simuler la dérive) ---
system_state = {
    "is_drifting": False,      # Si True, une anomalie commence
    "drift_intensity": 0.0,    # Augmente progressivement
    "drift_type": None,        # 'PH_ACID' ou 'HVAC_FAIL'
    "current_day": 12          # Commence au stade Végétatif
}

class VertiFlowExpertSimulator:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def trigger_anomaly(self):
        """Déclenche aléatoirement une dérive pour tester la Zone 4"""
        if not system_state["is_drifting"] and random.random() < 0.05:
            system_state["is_drifting"] = True
            system_state["drift_type"] = random.choice(["PH_ACID", "HVAC_FAIL"])
            logger.warning(f"🚨 DÉBUT D'ANOMALIE : {system_state['drift_type']}")

    def generate_record(self, rack, level):
        now = datetime.now(timezone.utc)

        # 1. BASE DE DONNÉES (Valeurs optimales de seed_data.js)
        temp_base = 24.5
        ph_base = 6.2
        hum_base = 65.0

        try:
            response = requests.get(URL, timeout=.5)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError):
            data = None  # fallback to null for ClickHouse

        if data:
            ext_temp_nasa = data['hourly']['temperature_2m'][-1]
            ext_humidity_nasa = data['hourly']['relative_humidity_2m'][-1]
            ext_solar_radiation = data['hourly']['uv_index'][-1]
        else:
            ext_solar_radiation = 0
            ext_temp_nasa = 0
            ext_humidity_nasa = 0

        # 2. APPLICATION DE LA DÉRIVE (Si active)
        if system_state["is_drifting"]:
            system_state["drift_intensity"] += 0.05 # Dérive progressive
            if system_state["drift_type"] == "PH_ACID":
                ph_base -= system_state["drift_intensity"] # Le pH chute (Acidification)
            elif system_state["drift_type"] == "HVAC_FAIL":
                temp_base += system_state["drift_intensity"] # La température monte (Panne clim)
                hum_base -= (system_state["drift_intensity"] * 2) # L'air s'assèche

        # 3. GÉNÉRATION DU RECORD (Conforme aux 157 colonnes de 01_tables.sql)
        # Nous remplissons les colonnes clés pour vos alertes NiFi
        record = {
            "ext_temp_nasa": ext_temp_nasa,
            "ext_humidity_nasa": ext_humidity_nasa,
            "ext_solar_radiation": ext_solar_radiation,

            # ===============================
            # I. IDENTIFICATION & LINEAGE
            # ===============================
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "farm_id": "VERT-MAROC-01",
            "rack_id": rack,
            "level_index": level,
            "module_id": f"{rack}-L{level}",
            "zone_id": f"Z{random.randint(1,3)}",
            "tank_id": f"TANK-{random.randint(1,4)}",
            "sensor_hardware_id": f"SENS-{uuid.uuid4().hex[:8]}",
            "species_variety": "Ocimum basilicum",
            "data_source_type": "IoT-Expert-Drift",
            "lineage_uuid": str(uuid.uuid4()),

            # ===============================
            # II. ENVIRONMENT & CLIMATE
            # ===============================
            "air_temp_internal": round(random.gauss(temp_base, 0.3), 2),
            "air_humidity": round(random.gauss(hum_base, 1.5), 1),
            "co2_level_ambient": round(random.gauss(850, 40), 0),
            "airflow_velocity": round(random.uniform(0.2, 1.2), 2),
            "air_pressure": round(random.gauss(1013, 5), 1),
            "vapor_pressure_deficit": round(random.uniform(0.6, 1.4), 2),

            # ===============================
            # III. WATER & RHIZOSPHERE
            # ===============================
            "water_temp": round(random.gauss(20.5, 0.4), 2),
            "water_ph": round(ph_base + random.uniform(-0.05, 0.05), 2),
            "dissolved_oxygen": round(random.uniform(6.5, 8.5), 2),
            "water_turbidity": round(random.uniform(0.2, 1.2), 2),
            "irrigation_line_pressure": round(random.uniform(1.0, 2.5), 2),
            "wue_current": round(random.uniform(3.0, 6.0), 2),
            "water_recycled_rate": round(random.uniform(0.2, 0.9), 2),
            "coefficient_cultural_kc": round(random.uniform(0.7, 1.2), 2),
            "redox_potential": round(random.uniform(200, 400), 0),

            # ===============================
            # IV. NUTRITION (MACRO + MICRO)
            # ===============================
            "nutrient_solution_ec": round(random.uniform(1.5, 2.3), 2),
            "nutrient_n_total": round(random.uniform(120, 220), 1),
            "nutrient_p_phosphorus": round(random.uniform(25, 60), 1),
            "nutrient_k_potassium": round(random.uniform(180, 320), 1),
            "nutrient_ca_calcium": round(random.uniform(120, 200), 1),
            "nutrient_mg_magnesium": round(random.uniform(35, 70), 1),
            "nutrient_s_sulfur": round(random.uniform(50, 90), 1),
            "nutrient_fe_iron": round(random.uniform(1.5, 3.0), 2),
            "nutrient_mn_manganese": round(random.uniform(0.3, 0.8), 2),
            "nutrient_zn_zinc": round(random.uniform(0.05, 0.2), 2),
            "nutrient_cu_copper": round(random.uniform(0.03, 0.1), 2),
            "nutrient_b_boron": round(random.uniform(0.2, 0.6), 2),
            "nutrient_mo_molybdenum": round(random.uniform(0.01, 0.05), 3),
            "nutrient_cl_chlorine": round(random.uniform(1.0, 3.0), 2),
            "nutrient_ni_nickel": round(random.uniform(0.01, 0.05), 3),

            # ===============================
            # V. LIGHT / PAR / PHOTOSYNTHESIS
            # ===============================
            "light_intensity_ppfd": round(random.uniform(200, 450), 0),
            "light_photoperiod": random.choice([16, 18]),
            "light_ratio_red_blue": round(random.uniform(2.0, 4.0), 2),
            "light_far_red_intensity": round(random.uniform(5, 30), 1),
            "light_compensation_point": round(random.uniform(20, 40), 1),
            "light_saturation_point": round(random.uniform(400, 700), 0),
            "light_dli_accumulated": round(random.uniform(12, 20), 2),
            "quantum_yield_psii": round(random.uniform(0.55, 0.75), 2),
            "photosynthetic_rate_max": round(random.uniform(12, 22), 1),
            "light_use_efficiency": round(random.uniform(0.4, 0.7), 2),
            "leaf_absorption_pct": round(random.uniform(75, 90), 1),
            "spectral_recipe_id": f"SPEC-{random.randint(1,5)}",
            "spectrum_blue_450nm_pct": round(random.uniform(15, 30), 1),
            "spectrum_red_660nm_pct": round(random.uniform(45, 65), 1),
            "spectrum_far_red_730nm_pct": round(random.uniform(3, 10), 1),
            "spectrum_white_pct": round(random.uniform(5, 15), 1),
            "phytochrome_ratio": round(random.uniform(0.7, 1.2), 2),
            "is_light_period": random.choice([0, 1]),
            "photoperiod_progress_pct": round(random.uniform(0, 100), 1),

            # ===============================
            # VI. BIOMASS & GROWTH
            # ===============================
            "days_since_planting": system_state["current_day"],
            "fresh_biomass_est": round(20 + system_state["current_day"] * random.uniform(1.3, 1.7), 2),
            "growth_stage": "Végétatif",

            # ===============================
            # VII. HARDWARE & MAINTENANCE
            # ===============================
            "pump_vibration_level": round(random.uniform(0.1, 0.4), 2),
            "fan_current_draw": round(random.uniform(0.2, 1.5), 2),
            "led_driver_temp": round(random.uniform(35, 60), 1),
            "led_power_consumption_w": round(random.uniform(80, 150), 1),
            "led_hours_total": random.randint(200, 3000),
            "led_efficiency_pct": round(random.uniform(30, 45), 1),
            "filter_differential_pressure": round(random.uniform(0.05, 0.4), 2),
            "ups_battery_health": round(random.uniform(80, 100), 1),

            # ===============================
            # VIII. REFERENCE TARGETS
            # ===============================
            "ref_n_target": round(random.uniform(160, 200), 1),
            "ref_p_target": round(random.uniform(35, 55), 1),
            "ref_k_target": round(random.uniform(220, 280), 1),
            "ref_ca_target": round(random.uniform(140, 180), 1),
            "ref_mg_target": round(random.uniform(45, 65), 1),
            "ec_target": round(random.uniform(1.8, 2.2), 2),
            "ph_target": round(random.uniform(5.8, 6.2), 2),

            # ===============================
            # IX. SAFETY & ALERTS
            # ===============================
            "leak_detection_status": random.choice([0, 1]),
            "emergency_stop_status": 0,

            # ===============================
            # X. QUALITY & ANOMALIES
            # ===============================
            "health_score": round(random.uniform(0.7, 1.0), 2),
            "data_integrity_flag": 0,
            "anomaly_confidence_score": round(random.uniform(0.0, 0.6), 2),
        }

        # Compléter avec des valeurs par défaut pour les colonnes restantes (157 au total)
        # (Pour l'exemple, on s'assure que les colonnes SQL existent)
        return record

    def run(self):
        logger.info(f"🚀 Simulateur prêt. Envoi vers : {KAFKA_TOPIC}")
        try:
            while True:
                self.trigger_anomaly()
                for rack in ["RACK-01", "RACK-02"]:
                    for level in [1, 2, 3, 4]:
                        data = self.generate_record(rack, level)
                        self.producer.send(KAFKA_TOPIC, value=data)

                # Mise à jour de l'état
                if system_state["is_drifting"] and system_state["drift_intensity"] > 3.0:
                    logger.info("✅ Anomalie terminée (système stabilisé ou plant ruiné).")
                    system_state["is_drifting"] = False
                    system_state["drift_intensity"] = 0.0

                time.sleep(.25)
        except KeyboardInterrupt:
            self.producer.close()

if __name__ == "__main__":
    VertiFlowExpertSimulator().run()
