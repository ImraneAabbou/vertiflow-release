#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PROJET VERTIFLOW - SIMULATEUR EXPERT COMPLET AVEC CORRECTION DYNAMIQUE
================================================================================
Cible : Zone 1 NiFi (vertiflow.ingestion.raw) + Feedback depuis sensor_corrector
Schéma : 157 colonnes (01_tables.sql)
Logique : Modèle métier complet avec adaptation dynamique
================================================================================
"""

import yaml
import json
import random
import time
import logging
import sys
import os
import uuid
import requests
import math
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from collections import deque
from concurrent.futures import ThreadPoolExecutor

try:
    from kafka import KafkaProducer, KafkaConsumer
except ImportError:
    print("❌ Erreur : kafka-python manquant.")
    sys.exit(1)

# Configuration API Météo
URL = "https://api.open-meteo.com/v1/forecast?latitude=33.5731&longitude=-7.5898&hourly=temperature_2m,relative_humidity_2m,rain,surface_pressure,wind_speed_10m,uv_index,soil_temperature_0cm,soil_moisture_0_to_1cm&daily=sunrise,sunset,uv_index_max,precipitation_sum,et0_fao_evapotranspiration&timezone=Africa/Casablanca"
CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "agronomic_parameters.yaml"

# Configuration Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = os.getenv("KAFKA_TOPIC", "vertiflow.ingestion.raw")
CORRECTION_TOPIC = "sensor_corrector"
FEEDBACK_TOPIC = "sensor_feedback"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SIM] - %(message)s')
logger = logging.getLogger("VertiFlow.ExpertSim.Adaptive")

# --- ÉTAT GLOBAL AVEC FEEDBACK ---
system_state = {
    "is_drifting": False,
    "drift_intensity": 0.0,
    "drift_type": None,
    "current_day": 12,
    "thermal_sum": 185.0,
    "farm_state": {},
    "energy_prices": {"peak": 0.15, "off_peak": 0.10},
    "market_price": 42.5,
    "correction_history": {},  # Historique des corrections par module
    "validity_stats": {"valid": 0, "invalid": 0, "corrected": 0},
    "adaptive_parameters": {}  # Paramètres adaptatifs par module
}

class AdaptiveFeedbackManager:
    """Gestionnaire de feedback pour adaptation dynamique"""
    
    def __init__(self):
        self.consumer = KafkaConsumer(
            CORRECTION_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='vertiflow_simulator_adaptive'
        )
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.running = True
        self.feedback_queue = deque(maxlen=1000)
        
    def start_listening(self):
        """Démarre l'écoute des corrections en arrière-plan"""
        def listen_corrections():
            logger.info(f"📡 Écoute des corrections sur {CORRECTION_TOPIC}")
            for message in self.consumer:
                if not self.running:
                    break
                try:
                    correction_data = message.value
                    self.process_correction(correction_data)
                except Exception as e:
                    logger.error(f"Erreur traitement correction: {e}")
        
        self.executor.submit(listen_corrections)
        logger.info("✅ Gestionnaire de feedback démarré")
    
    def process_correction(self, correction_data):
        """Traite une correction reçue"""
        try:
            module_id = correction_data.get('module_id')
            is_valid = correction_data.get('is_valid', False)
            violations = correction_data.get('violations', [])
            
            if not module_id:
                return
            
            # Mettre à jour les statistiques globales
            if is_valid:
                system_state["validity_stats"]["valid"] += 1
            else:
                system_state["validity_stats"]["invalid"] += 1
            
            # Stocker les corrections pour ce module
            if module_id not in system_state["correction_history"]:
                system_state["correction_history"][module_id] = {
                    'valid_count': 0,
                    'invalid_count': 0,
                    'last_correction': datetime.now(timezone.utc),
                    'violation_patterns': {}
                }
            
            module_history = system_state["correction_history"][module_id]
            
            if is_valid:
                module_history['valid_count'] += 1
            else:
                module_history['invalid_count'] += 1
                module_history['last_correction'] = datetime.now(timezone.utc)
                
                # Analyser les patterns de violation
                for violation in violations:
                    param = violation.get('parameter')
                    violation_type = violation.get('type', 'UNKNOWN')
                    
                    if param not in module_history['violation_patterns']:
                        module_history['violation_patterns'][param] = {}
                    
                    if violation_type not in module_history['violation_patterns'][param]:
                        module_history['violation_patterns'][param][violation_type] = 0
                    
                    module_history['violation_patterns'][param][violation_type] += 1
            
            # Calculer le score de fiabilité du module
            total_checks = module_history['valid_count'] + module_history['invalid_count']
            if total_checks > 0:
                reliability_score = module_history['valid_count'] / total_checks
                
                # Mettre à jour les paramètres adaptatifs
                if module_id not in system_state["adaptive_parameters"]:
                    system_state["adaptive_parameters"][module_id] = {
                        'reliability_score': reliability_score,
                        'adjustment_factors': {},
                        'last_adjustment': None
                    }
                
                adaptive_params = system_state["adaptive_parameters"][module_id]
                adaptive_params['reliability_score'] = reliability_score
                
                # Ajuster les paramètres si fiabilité faible
                if reliability_score < 0.7:
                    self.adjust_module_parameters(module_id, module_history)
            
            # Ajouter à la queue pour monitoring
            self.feedback_queue.append({
                'timestamp': datetime.now(timezone.utc),
                'module_id': module_id,
                'is_valid': is_valid,
                'violation_count': len(violations)
            })
            
            logger.debug(f"📊 Feedback traité: {module_id} - Valid: {is_valid}")
            
        except Exception as e:
            logger.error(f"Erreur traitement correction: {e}")
    
    def adjust_module_parameters(self, module_id, module_history):
        """Ajuste dynamiquement les paramètres d'un module problématique"""
        try:
            # Analyser les patterns de violation
            violation_patterns = module_history.get('violation_patterns', {})
            
            adjustments = {}
            
            for param, violations in violation_patterns.items():
                total_violations = sum(violations.values())
                
                # Si un paramètre a trop de violations, ajuster son ciblage
                if total_violations >= 3:
                    # Calculer un facteur de correction
                    if 'MIN_PHASE_VIOLATION' in violations:
                        adjustments[param] = {'adjustment': 1.05, 'reason': 'Tendance trop basse'}
                    elif 'MAX_PHASE_VIOLATION' in violations:
                        adjustments[param] = {'adjustment': 0.95, 'reason': 'Tendance trop haute'}
                    elif 'STAT_OUTLIER' in violations:
                        adjustments[param] = {'adjustment': 0.98, 'reason': 'Variabilité excessive'}
            
            # Appliquer les ajustements
            if adjustments:
                if module_id not in system_state["adaptive_parameters"]:
                    system_state["adaptive_parameters"][module_id] = {
                        'reliability_score': 0.0,
                        'adjustment_factors': {},
                        'last_adjustment': None
                    }
                
                adaptive_params = system_state["adaptive_parameters"][module_id]
                adaptive_params['adjustment_factors'].update(adjustments)
                adaptive_params['last_adjustment'] = datetime.now(timezone.utc)
                
                logger.info(f"🔧 Ajustements pour {module_id}: {adjustments}")
                
        except Exception as e:
            logger.error(f"Erreur ajustement paramètres: {e}")
    
    def get_adjustment_factor(self, module_id, parameter):
        """Récupère le facteur d'ajustement pour un paramètre donné"""
        if module_id in system_state["adaptive_parameters"]:
            adjustments = system_state["adaptive_parameters"][module_id].get('adjustment_factors', {})
            if parameter in adjustments:
                return adjustments[parameter].get('adjustment', 1.0)
        return 1.0
    
    def get_reliability_score(self, module_id):
        """Récupère le score de fiabilité d'un module"""
        if module_id in system_state["adaptive_parameters"]:
            return system_state["adaptive_parameters"][module_id].get('reliability_score', 1.0)
        return 1.0
    
    def stop(self):
        """Arrête le gestionnaire de feedback"""
        self.running = False
        self.consumer.close()
        self.executor.shutdown(wait=True)
        logger.info("🛑 Gestionnaire de feedback arrêté")

class VertiFlowAdaptiveSimulator:
    def __init__(self):
        # Producteurs Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Gestionnaire de feedback
        self.feedback_manager = AdaptiveFeedbackManager()
        
        # Chargement des paramètres
        self.params = self._load_params()
        self.basil_config = self.params["crops"]["basil_genovese"]
        self.stage_plan = self._load_stage_plan(self.basil_config)
        self.max_stage_day = max(stage["day_end"] for stage in self.stage_plan)
        self.last_day_tick = time.time()
        self.virtual_day_seconds = 60
        
        # Initialisation
        self.initialize_farm_state()
        
        # Démarrer l'écoute des corrections
        self.feedback_manager.start_listening()

    def _load_params(self):
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def _load_stage_plan(self, basil_config):
        stage_plan = basil_config.get("stage_plan", [])
        if stage_plan:
            return stage_plan

        growth_days = basil_config.get("growth_cycle_days", 30)
        return [
            {
                "stage_id": 0,
                "stage_name": "VEGETATIVE",
                "day_start": 0,
                "day_end": growth_days,
                "photoperiod_h": basil_config.get("photoperiod", {}).get("vegetative", 16),
                "ppfd_target_umol": basil_config.get("optimal_ranges", {}).get("par_umol", {}).get("optimal", 300),
                "dli_target_mol_m2": basil_config.get("dli_targets", {}).get("vegetative", 18),
                "temp_day_c": basil_config.get("optimal_ranges", {}).get("temperature_c", {}).get("optimal", 24),
                "temp_night_c": basil_config.get("optimal_ranges", {}).get("temperature_c", {}).get("optimal", 24),
                "vpd_target_kpa": basil_config.get("vpd_targets", {}).get("vegetative", 0.9),
                "co2_ppm": basil_config.get("optimal_ranges", {}).get("co2_ppm", {}).get("optimal", 900),
                "rel_humidity_target_pct": basil_config.get("optimal_ranges", {}).get("humidity_pct", {}).get("optimal", 62),
                "ec_target_dS_m": basil_config.get("nutrition", {}).get("vegetative", {}).get("ec_target", 1.6),
                "ph_target": basil_config.get("optimal_ranges", {}).get("ph", {}).get("optimal", 6.0),
                "N_ppm": basil_config.get("nutrition", {}).get("vegetative", {}).get("N", 160),
                "P_ppm": basil_config.get("nutrition", {}).get("vegetative", {}).get("P", 55),
                "K_ppm": basil_config.get("nutrition", {}).get("vegetative", {}).get("K", 210),
                "Ca_ppm": basil_config.get("nutrition", {}).get("vegetative", {}).get("Ca", 120),
                "Mg_ppm": basil_config.get("nutrition", {}).get("vegetative", {}).get("Mg", 42),
                "irrigation_strategy": "DEFAULT",
            }
        ]

    def initialize_farm_state(self):
        """Initialise l'état historique pour chaque module"""
        for rack in ["RACK-01", "RACK-02"]:
            for level in [1, 2, 3, 4]:
                module_id = f"{rack}-L{level}"
                system_state["farm_state"][module_id] = {
                    "prev_biomass": 20.0,
                    "prev_timestamp": datetime.now(timezone.utc),
                    "uptime_hours": random.randint(200, 3000),
                    "system_reboot_count": random.randint(0, 3),
                    "daily_biomass_gain": [],
                    "last_maintenance": datetime.now(timezone.utc) - timedelta(days=random.randint(5, 30)),
                    "correction_attempts": 0,
                    "auto_correction_enabled": True
                }

    def _advance_day_if_needed(self):
        now = time.time()
        if now - self.last_day_tick < self.virtual_day_seconds:
            return

        days_to_add = int((now - self.last_day_tick) // self.virtual_day_seconds)
        self.last_day_tick += days_to_add * self.virtual_day_seconds
        system_state["current_day"] += days_to_add
        for _ in range(days_to_add):
            system_state["thermal_sum"] += max(0, 22.0 - 10.0)
        if system_state["current_day"] > self.max_stage_day:
            system_state["current_day"] = 0

    def _get_stage_for_day(self, day):
        for stage in self.stage_plan:
            if stage["day_start"] <= day <= stage["day_end"]:
                return stage
        return self.stage_plan[-1]

    def _map_stage_key(self, stage_name):
        if stage_name in {"GERMINATION"}:
            return "germination"
        if stage_name in {"EMERGENCE"}:
            return "seedling"
        if stage_name in {"VEGETATIVE_EARLY", "VEGETATIVE_RAPID", "VEGETATIVE"}:
            return "vegetative"
        if stage_name in {"PRE_HARVEST_FINISHING", "HARVEST_HOLD"}:
            return "finishing"
        return "vegetative"

    def _gauss_target_with_adjustment(self, target, pct, min_std, module_id, parameter_name):
        """Génère une valeur gaussienne avec ajustement dynamique"""
        if target <= 0:
            return 0.0
        
        # Appliquer l'ajustement du feedback
        adjustment = self.feedback_manager.get_adjustment_factor(module_id, parameter_name)
        adjusted_target = target * adjustment
        
        reliability = self.feedback_manager.get_reliability_score(module_id)
        
        # Réduire la variabilité si le module est fiable
        if reliability > 0.8:
            pct *= 0.7  # Réduire la variation de 30%
        
        return random.gauss(adjusted_target, max(adjusted_target * pct, min_std))

    def _generate_value_with_feedback(self, base_value, parameter_name, module_id, stage_config):
        """Génère une valeur avec prise en compte du feedback"""
        try:
            # Récupérer le score de fiabilité
            reliability = self.feedback_manager.get_reliability_score(module_id)
            
            # Si le module a une faible fiabilité, réduire les anomalies
            if reliability < 0.6:
                # Réduire la probabilité de dérive
                if system_state["is_drifting"] and random.random() > 0.3:
                    return base_value
            
            # Appliquer les ajustements spécifiques
            adjustment = self.feedback_manager.get_adjustment_factor(module_id, parameter_name)
            
            if adjustment != 1.0:
                # Ajuster la valeur de base
                adjusted_base = base_value * adjustment
                
                # Pour les paramètres critiques, ajuster plus précisément
                critical_params = ['air_temp_internal', 'nutrient_solution_ec', 'water_ph']
                if parameter_name in critical_params:
                    # Garder dans les limites du stade
                    if parameter_name == 'air_temp_internal':
                        stage_temp = stage_config.get('temp_day_c', 24.0)
                        adjusted_base = max(stage_temp - 2, min(stage_temp + 2, adjusted_base))
                    
                    return round(adjusted_base, 2)
            
            return base_value
            
        except Exception as e:
            logger.error(f"Erreur ajustement {parameter_name}: {e}")
            return base_value

    # =========================================================================
    # MODÈLES MÉTIER (inchangés)
    # =========================================================================

    def calculate_dew_point(self, temp, humidity):
        """Calcule le point de rosée (formule de Magnus)"""
        if humidity <= 0:
            return temp
        alpha = 17.27
        beta = 237.7
        gamma = (alpha * temp) / (beta + temp) + math.log(humidity/100.0)
        return round((beta * gamma) / (alpha - gamma), 2)

    def calculate_dry_biomass(self, fresh_biomass, growth_stage):
        """Estime la biomasse sèche selon le stade"""
        ratio_dry_matter = {
            'GERMINATION': 0.08,
            'SEEDLING': 0.10,
            'VEGETATIVE': 0.12,
            'PRE_HARVEST_FINISHING': 0.20,
            'HARVEST': 0.22
        }
        ratio = ratio_dry_matter.get(growth_stage, 0.15)
        return round(fresh_biomass * ratio, 3)

    def calculate_lai(self, fresh_biomass, growth_stage, days_since_planting):
        """Calcule l'indice de surface foliaire"""
        if growth_stage == 'PRE_HARVEST_FINISHING':
            lai = 3.2 + (fresh_biomass / 100)
        elif days_since_planting < 15:
            lai = 0.5 + (days_since_planting * 0.15)
        else:
            lai = 2.8 + (fresh_biomass - 40) / 60
        return max(0.5, round(lai, 2))

    def calculate_root_shoot_ratio(self, growth_stage, vpd):
        """Calcule le ratio racine/tige"""
        base_ratio = {
            'GERMINATION': 0.8,
            'SEEDLING': 0.6,
            'VEGETATIVE': 0.4,
            'PRE_HARVEST_FINISHING': 0.35,
            'HARVEST': 0.3
        }
        vpd_adjustment = max(0.8, min(1.2, 1.0 + (vpd - 1.0) * 0.1))
        return round(base_ratio.get(growth_stage, 0.4) * vpd_adjustment, 2)

    def calculate_rgr(self, prev_biomass, current_biomass, time_hours, light_ppfd, light_saturation, temp, temp_opt, lue):
        """Calcule le taux de croissance relatif"""
        if prev_biomass <= 0 or time_hours <= 0:
            return 0.0
        
        rgr_daily = 0.15  # g/g/jour base
        light_factor = min(1.0, light_ppfd / light_saturation) if light_saturation > 0 else 1.0
        temp_factor = 1.0 - abs(temp - temp_opt) * 0.05
        
        # Calcul RGR réel
        try:
            rgr_actual = (math.log(current_biomass) - math.log(prev_biomass)) / (time_hours / 24)
        except (ValueError, ZeroDivisionError):
            rgr_actual = rgr_daily * light_factor * temp_factor * lue
        
        return round(rgr_actual, 4)

    def calculate_nar(self, biomass_gain, lai, time_hours):
        """Calcule le taux d'assimilation nette"""
        if lai <= 0 or time_hours <= 0:
            return 0.0
        return round(biomass_gain / (lai * (time_hours / 24)), 3)

    def calculate_stomatal_conductance(self, vpd, light_ppfd, co2_level):
        """Calcule la conductance stomatique"""
        if vpd > 1.5:
            stomatal_factor = 0.7
        elif vpd < 0.8:
            stomatal_factor = 0.9
        else:
            stomatal_factor = 0.85
        
        light_factor = min(1.0, light_ppfd / 500) if light_ppfd > 0 else 0.1
        co2_factor = 1.0 / (1 + (co2_level - 400) / 200) if co2_level > 0 else 1.0
        
        conductance = 0.3 * stomatal_factor * light_factor * co2_factor
        return round(conductance, 3)

    def calculate_chlorophyll_index(self, nutrient_n, n_target, growth_stage):
        """Calcule l'indice de chlorophylle (SPAD)"""
        n_sufficiency = nutrient_n / n_target if n_target > 0 else 1.0
        base_spad = {
            'GERMINATION': 25,
            'SEEDLING': 30,
            'VEGETATIVE': 40,
            'PRE_HARVEST_FINISHING': 45,
            'HARVEST': 42
        }
        spad = base_spad.get(growth_stage, 35) * min(1.2, n_sufficiency)
        return round(spad, 1)

    def calculate_essential_oil_yield(self, fresh_biomass, vpd, ec, ec_target):
        """Calcule le rendement en huile essentielle"""
        stress_index = 0.5 * abs(vpd - 1.0) + 0.3 * abs(ec - ec_target)
        oil_yield = 0.015 * fresh_biomass * (1 + min(0.5, stress_index * 0.3))
        return round(oil_yield, 4)

    def calculate_microbial_density(self, water_temp, water_ph, recycled_rate):
        """Calcule la densité microbienne"""
        temp_factor = water_temp / 25.0
        ph_factor = 1.0 - abs(water_ph - 6.5) * 0.2
        density = 1e6 * temp_factor * ph_factor * (1 + recycled_rate)
        return round(density, 0)

    def calculate_energy_footprint(self, led_power, fan_power, pump_power, temp_diff):
        """Calcule l'empreinte énergétique horaire"""
        hvac_power = abs(temp_diff) * 100  # estimation
        return round((led_power + fan_power + pump_power + hvac_power) / 1000, 3)

    def calculate_operational_cost(self, energy_kwh, energy_price, fresh_biomass, recycled_rate):
        """Calcule le coût opérationnel journalier"""
        energy_cost = energy_kwh * 24 * energy_price
        water_cost = 0.002 * (1 - recycled_rate)
        nutrient_cost = 0.015 * fresh_biomass
        labor_cost = 0.10 * fresh_biomass
        return round(energy_cost + water_cost + nutrient_cost + labor_cost, 2)

    def calculate_tip_burn_risk(self, ca_level, ca_target, humidity, light_ppfd):
        """Calcule le risque de brûlure des pointes"""
        ca_deficit = max(0, ca_target - ca_level) / ca_target if ca_target > 0 else 0
        humidity_risk = 1.0 if humidity > 80 else 0.5
        light_risk = 1.0 if light_ppfd > 600 else 0.3
        risk = min(1.0, (ca_deficit * 0.6 + humidity_risk * 0.3 + light_risk * 0.1))
        return round(risk, 2)

    def calculate_pest_outbreak_risk(self, humidity, lai, health_score):
        """Calcule le risque d'épidémie de ravageurs"""
        humidity_factor = 0.5 if humidity > 70 else 0.2
        density_factor = min(1.0, lai / 4.0) if lai > 0 else 0.5
        health_factor = 1.0 - health_score
        risk = (humidity_factor * 0.4 + density_factor * 0.4 + health_factor * 0.2)
        return round(risk, 2)

    def calculate_module_integrity(self, led_efficiency, pump_vibration, fan_current, 
                                   anomaly_score, filter_pressure):
        """Calcule le score d'intégrité du module"""
        component_scores = {
            'led': led_efficiency / 100,
            'pump': 1.0 - min(1.0, pump_vibration),
            'fan': 1.0 - min(1.0, fan_current / 2.0),
            'sensors': 1.0 - anomaly_score,
            'filtration': 1.0 - min(1.0, filter_pressure)
        }
        integrity = sum(component_scores.values()) / len(component_scores)
        return round(integrity, 2)

    def calculate_predicted_yield(self, days_since_planting, health_score, ec, ec_target):
        """Prédit le rendement final"""
        max_yield = 6.0  # kg/m²
        growth_factor = 1.0 - math.exp(-days_since_planting / 30)
        stress_factor = health_score * (ec / ec_target if ec_target > 0 else 1.0)
        yield_est = max_yield * growth_factor * stress_factor
        return round(yield_est, 2)

    def calculate_expected_harvest_date(self, current_date, thermal_sum, thermal_target, temp_internal):
        """Calcule la date de récolte attendue"""
        thermal_daily = max(0, temp_internal - 10)  # base 10°C
        if thermal_daily <= 0:
            return None
        days_to_harvest = max(0, (thermal_target - thermal_sum) / thermal_daily)
        harvest_date = current_date + timedelta(days=days_to_harvest)
        return harvest_date.strftime("%Y-%m-%d")

    def trigger_anomaly_with_feedback(self):
        """Déclenche une anomalie avec prise en compte du feedback"""
        if system_state["is_drifting"]:
            return
        
        # Vérifier si un module a une faible fiabilité
        low_reliability_modules = []
        for module_id, adaptive_params in system_state["adaptive_parameters"].items():
            if adaptive_params.get('reliability_score', 1.0) < 0.5:
                low_reliability_modules.append(module_id)
        
        # Si un module a une faible fiabilité, réduire la probabilité d'anomalie
        anomaly_probability = 0.05
        if low_reliability_modules:
            anomaly_probability = 0.02  # Réduire de moitié
        
        if random.random() < anomaly_probability:
            system_state["is_drifting"] = True
            system_state["drift_type"] = random.choice(["PH_ACID", "HVAC_FAIL", "NUTRIENT_IMBALANCE", "LIGHT_FAILURE"])
            
            # Log supplémentaire si modules peu fiables
            if low_reliability_modules:
                logger.warning(f"🚨 ANOMALIE AVEC MODULES PEU FIABLES: {system_state['drift_type']}")
            else:
                logger.warning(f"🚨 DÉBUT D'ANOMALIE: {system_state['drift_type']}")

    def generate_record_with_feedback(self, rack, level):
        now = datetime.now(timezone.utc)
        self._advance_day_if_needed()
        
        module_id = f"{rack}-L{level}"
        module_state = system_state["farm_state"][module_id]
        
        stage = self._get_stage_for_day(system_state["current_day"])
        growth_stage = stage["stage_name"]
        photoperiod_h = int(stage["photoperiod_h"])
        
        # Calcul des périodes lumineuses
        hours_since_dawn = (now.hour - 6) % 24
        is_light_period = 1 if photoperiod_h and hours_since_dawn < photoperiod_h else 0
        photoperiod_progress_pct = round((hours_since_dawn / photoperiod_h) * 100, 1) if is_light_period else 0.0
        
        # Températures de base
        temp_base = stage["temp_day_c"] if is_light_period else stage["temp_night_c"]
        ph_base = stage["ph_target"]
        hum_base = stage["rel_humidity_target_pct"]
        
        # Données météo externes
        try:
            response = requests.get(URL, timeout=.5)
            response.raise_for_status()
            data = response.json()
            ext_temp_nasa = data['hourly']['temperature_2m'][-1]
            ext_humidity_nasa = data['hourly']['relative_humidity_2m'][-1]
            ext_solar_radiation = data['hourly']['uv_index'][-1]
        except (requests.RequestException, ValueError):
            ext_solar_radiation = random.uniform(200, 850) 
            ext_temp_nasa = random.uniform(15.0, 32.0)
            ext_humidity_nasa = random.uniform(40.0, 90.0)
        
        # Application de la dérive avec feedback
        if system_state["is_drifting"]:
            system_state["drift_intensity"] += 0.05
            
            # Vérifier la fiabilité du module
            reliability = self.feedback_manager.get_reliability_score(module_id)
            
            # Si le module est fiable, réduire l'intensité de la dérive
            if reliability > 0.8:
                system_state["drift_intensity"] *= 0.8
            
            if system_state["drift_type"] == "PH_ACID":
                ph_base -= system_state["drift_intensity"]
            elif system_state["drift_type"] == "HVAC_FAIL":
                temp_base += system_state["drift_intensity"]
                hum_base -= (system_state["drift_intensity"] * 2)
        
        # Valeurs de référence
        ref_humidity_opt = stage["rel_humidity_target_pct"]
        ref_temp_opt = stage["temp_day_c"]
        ref_n_target = stage["N_ppm"]
        ref_p_target = stage["P_ppm"]
        ref_k_target = stage["K_ppm"]
        ref_ca_target = stage["Ca_ppm"]
        ref_mg_target = stage["Mg_ppm"]
        ec_target = stage["ec_target_dS_m"]
        ph_target = stage["ph_target"]
        co2_target = stage["co2_ppm"]
        vpd_target = stage["vpd_target_kpa"]
        ppfd_target = stage["ppfd_target_umol"]
        dli_target = stage["dli_target_mol_m2"]
        
        spectrum_key = self._map_stage_key(stage["stage_name"])
        spectrum = self.basil_config.get("spectrum", {}).get(spectrum_key, {})
        
        # =====================================================================
        # CALCUL DES VALEURS AVEC FEEDBACK
        # =====================================================================
        
        # Climat interne avec ajustement
        air_temp_internal = round(self._gauss_target_with_adjustment(
            temp_base, 0.04, 0.3, module_id, 'air_temp_internal'
        ), 2)
        
        air_humidity = round(self._gauss_target_with_adjustment(
            hum_base, 0.03, 0.8, module_id, 'air_humidity'
        ), 1)
        
        co2_level_ambient = round(self._gauss_target_with_adjustment(
            co2_target, 0.05, 8, module_id, 'co2_level_ambient'
        ), 0)
        
        airflow_velocity = round(random.uniform(0.2, 1.2), 2)
        air_pressure = round(random.gauss(1013, 5), 1)
        vapor_pressure_deficit = round(self._gauss_target_with_adjustment(
            vpd_target, 0.07, 0.04, module_id, 'vapor_pressure_deficit'
        ), 2)
        
        # Eau et rhizosphère avec ajustement
        water_temp = round(random.gauss(20.5, 0.4), 2)
        water_ph = round(self._generate_value_with_feedback(
            ph_base + random.uniform(-0.03, 0.03),
            'water_ph', module_id, stage
        ), 2)
        
        dissolved_oxygen = round(random.uniform(6.5, 8.5), 2)
        water_turbidity = round(random.uniform(0.2, 1.2), 2)
        irrigation_line_pressure = round(random.uniform(1.0, 2.5), 2)
        wue_current = round(random.uniform(3.0, 6.0), 2)
        water_recycled_rate = round(random.uniform(0.2, 0.9), 2)
        coefficient_cultural_kc = round(random.uniform(0.7, 1.2), 2)
        redox_potential = round(random.uniform(200, 400), 0)
        
        # Nutrition avec ajustement
        nutrient_solution_ec = round(self._gauss_target_with_adjustment(
            ec_target, 0.04, 0.04, module_id, 'nutrient_solution_ec'
        ), 2)
        
        nutrient_n_total = round(self._gauss_target_with_adjustment(
            ref_n_target, 0.05, 1.5, module_id, 'nutrient_n_total'
        ), 1)
        
        nutrient_p_phosphorus = round(self._gauss_target_with_adjustment(
            ref_p_target, 0.06, 0.8, module_id, 'nutrient_p_phosphorus'
        ), 1)
        
        nutrient_k_potassium = round(self._gauss_target_with_adjustment(
            ref_k_target, 0.05, 1.5, module_id, 'nutrient_k_potassium'
        ), 1)
        
        nutrient_ca_calcium = round(self._gauss_target_with_adjustment(
            ref_ca_target, 0.06, 2, module_id, 'nutrient_ca_calcium'
        ), 1)
        
        nutrient_mg_magnesium = round(self._gauss_target_with_adjustment(
            ref_mg_target, 0.07, 1, module_id, 'nutrient_mg_magnesium'
        ), 1)
        
        nutrient_s_sulfur = round(random.uniform(50, 90), 1)
        nutrient_fe_iron = round(random.uniform(1.5, 3.0), 2)
        nutrient_mn_manganese = round(random.uniform(0.3, 0.8), 2)
        nutrient_zn_zinc = round(random.uniform(0.05, 0.2), 2)
        nutrient_cu_copper = round(random.uniform(0.03, 0.1), 2)
        nutrient_b_boron = round(random.uniform(0.2, 0.6), 2)
        nutrient_mo_molybdenum = round(random.uniform(0.01, 0.05), 3)
        nutrient_cl_chlorine = round(random.uniform(1.0, 3.0), 2)
        nutrient_ni_nickel = round(random.uniform(0.01, 0.05), 3)
        
        # Lumière avec ajustement
        light_intensity_ppfd = round(self._gauss_target_with_adjustment(
            ppfd_target, 0.05, 4, module_id, 'light_intensity_ppfd'
        ), 0)
        
        light_photoperiod = photoperiod_h
        light_ratio_red_blue = round(spectrum.get("ratio_rb", random.uniform(2.0, 4.0)), 2)
        light_far_red_intensity = round(random.uniform(5, 30), 1)
        light_compensation_point = round(random.uniform(20, 40), 1)
        light_saturation_point = round(random.uniform(400, 700), 0)
        light_dli_accumulated = round(self._gauss_target_with_adjustment(
            dli_target, 0.06, 0.2, module_id, 'light_dli_accumulated'
        ), 2)
        
        quantum_yield_psii = round(random.uniform(0.55, 0.75), 2)
        photosynthetic_rate_max = round(random.uniform(12, 22), 1)
        light_use_efficiency = round(random.uniform(0.4, 0.7), 2)
        leaf_absorption_pct = round(random.uniform(75, 90), 1)
        
        # Biomasse et croissance
        days_since_planting = system_state["current_day"]
        fresh_biomass_est = round(20 + system_state["current_day"] * random.uniform(1.3, 1.7), 2)
        
        # =====================================================================
        # CALCULS MÉTIER AVANCÉS
        # =====================================================================
        
        # 1. Données dérivées de base
        dry_biomass_est = self.calculate_dry_biomass(fresh_biomass_est, growth_stage)
        leaf_area_index_lai = self.calculate_lai(fresh_biomass_est, growth_stage, days_since_planting)
        root_shoot_ratio = self.calculate_root_shoot_ratio(growth_stage, vapor_pressure_deficit)
        
        # 2. Calculs de croissance
        time_since_last = (now - module_state["prev_timestamp"]).total_seconds() / 3600
        rgr = self.calculate_rgr(module_state["prev_biomass"], fresh_biomass_est, 
                                 time_since_last, light_intensity_ppfd, 
                                 light_saturation_point, air_temp_internal, 
                                 ref_temp_opt, light_use_efficiency)
        
        biomass_gain = fresh_biomass_est - module_state["prev_biomass"]
        net_assimilation_rate = self.calculate_nar(biomass_gain, leaf_area_index_lai, time_since_last)
        
        # 3. Physiologie
        stomatal_conductance = self.calculate_stomatal_conductance(
            vapor_pressure_deficit, light_intensity_ppfd, co2_level_ambient
        )
        
        chlorophyll_index_spad = self.calculate_chlorophyll_index(
            nutrient_n_total, ref_n_target, growth_stage
        )
        
        # 4. Microbiologie
        microbial_density = self.calculate_microbial_density(
            water_temp, water_ph, water_recycled_rate
        )
        
        beneficial_microbes_ratio = 0.85 if redox_potential > 200 else 0.75
        root_fungal_pressure = (1.0 if water_temp > 22 else 0.5 + 1.0 if dissolved_oxygen < 6 else 0.3) / 2 * 0.8
        
        # 5. Qualité
        essential_oil_yield = self.calculate_essential_oil_yield(
            fresh_biomass_est, vapor_pressure_deficit, nutrient_solution_ec, ec_target
        )
        
        aroma_compounds_ratio = 0.7 * (1.0 - abs(air_temp_internal - 24) * 0.02) * (1.0 + (light_intensity_ppfd - 400) * 0.001)
        
        # 6. Énergie et économie
        led_power = random.uniform(80, 150)
        fan_power = random.uniform(0.2, 1.5) * 230
        pump_power = 50
        energy_footprint_hourly = self.calculate_energy_footprint(
            led_power, fan_power, pump_power, air_temp_internal - ref_temp_opt
        )
        
        hour = now.hour
        energy_price_kwh = system_state["energy_prices"]["peak"] if 6 <= hour < 22 else system_state["energy_prices"]["off_peak"]
        
        operational_cost_total = self.calculate_operational_cost(
            energy_footprint_hourly, energy_price_kwh, fresh_biomass_est, water_recycled_rate
        )
        
        # 7. Risques
        tip_burn_risk = self.calculate_tip_burn_risk(
            nutrient_ca_calcium, ref_ca_target, air_humidity, light_intensity_ppfd
        )
        
        risk_pest_outbreak = self.calculate_pest_outbreak_risk(
            air_humidity, leaf_area_index_lai, 0.77  # health_score par défaut
        )
        
        # 8. Prédictions
        predicted_yield_kg_m2 = self.calculate_predicted_yield(
            days_since_planting, 0.77, nutrient_solution_ec, ec_target
        )
        
        expected_harvest_date = self.calculate_expected_harvest_date(
            now, system_state["thermal_sum"], 350, air_temp_internal
        )
        
        # 9. Matériel et intégrité
        pump_vibration_level = round(random.uniform(0.1, 0.4), 2)
        fan_current_draw = round(random.uniform(0.2, 1.5), 2)
        led_driver_temp = round(random.uniform(35, 60), 1)
        led_power_consumption_w = round(led_power, 1)
        led_hours_total = module_state["uptime_hours"]
        led_efficiency_pct = round(random.uniform(30, 45), 1)
        filter_differential_pressure = round(random.uniform(0.05, 0.4), 2)
        ups_battery_health = round(random.uniform(80, 100), 1)
        
        module_integrity_score = self.calculate_module_integrity(
            led_efficiency_pct, pump_vibration_level, fan_current_draw,
            0.28, filter_differential_pressure  # anomaly_confidence_score par défaut
        )
        
        # 10. Divers calculs
        dew_point = self.calculate_dew_point(air_temp_internal, air_humidity)
        canopy_height = 0.5 / (1 + math.exp(-0.08 * (days_since_planting - 15)))
        harvest_index = 0.85 if growth_stage in ['PRE_HARVEST_FINISHING', 'HARVEST'] else 0.0
        biomass_accumulation_daily = round(biomass_gain * (24 / time_since_last) if time_since_last > 0 else 2.0, 3)
        target_harvest_weight = round(fresh_biomass_est * 1.2, 2)  # +20% pour la cible
        
        # Construction du record complet
        record = {
            # I. IDENTIFICATION & LINEAGE
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "farm_id": "VERT-MAROC-01",
            "parcel_id": f"PARCEL-{random.randint(1, 4)}",
            "latitude": round(33.5731 + random.uniform(-0.001, 0.001), 6),
            "longitude": round(-7.5898 + random.uniform(-0.001, 0.001), 6),
            "zone_id": f"Z{random.randint(1,3)}",
            "rack_id": rack,
            "level_index": level,
            "module_id": module_id,
            "batch_id": f"BATCH-{random.randint(1000, 9999)}",
            "species_variety": "Ocimum basilicum",
            
            # Nouveaux champs de feedback
            "module_reliability_score": round(self.feedback_manager.get_reliability_score(module_id), 3),
            "adaptive_correction_enabled": module_state["auto_correction_enabled"],
            "correction_attempts": module_state["correction_attempts"],
            
            # II. STRUCTURE & POSITION
            "position_x_y": f"{random.randint(1, 10)},{random.randint(1, 10)}",
            "structural_weight_load": round(random.uniform(50, 150), 1),
            
            # III. NUTRITION
            "nutrient_n_total": nutrient_n_total,
            "nutrient_p_phosphorus": nutrient_p_phosphorus,
            "nutrient_k_potassium": nutrient_k_potassium,
            "nutrient_ca_calcium": nutrient_ca_calcium,
            "nutrient_mg_magnesium": nutrient_mg_magnesium,
            "nutrient_s_sulfur": nutrient_s_sulfur,
            "nutrient_fe_iron": nutrient_fe_iron,
            "nutrient_mn_manganese": nutrient_mn_manganese,
            "nutrient_zn_zinc": nutrient_zn_zinc,
            "nutrient_cu_copper": nutrient_cu_copper,
            "nutrient_b_boron": nutrient_b_boron,
            "nutrient_mo_molybdenum": nutrient_mo_molybdenum,
            "nutrient_cl_chlorine": nutrient_cl_chlorine,
            "nutrient_ni_nickel": nutrient_ni_nickel,
            "nutrient_solution_ec": nutrient_solution_ec,
            
            # IV. LUMIÈRE & PHOTOSYNTHÈSE
            "light_intensity_ppfd": light_intensity_ppfd,
            "light_compensation_point": light_compensation_point,
            "light_saturation_point": light_saturation_point,
            "light_ratio_red_blue": light_ratio_red_blue,
            "light_far_red_intensity": light_far_red_intensity,
            "light_dli_accumulated": light_dli_accumulated,
            "light_photoperiod": light_photoperiod,
            "quantum_yield_psii": quantum_yield_psii,
            "photosynthetic_rate_max": photosynthetic_rate_max,
            "co2_level_ambient": co2_level_ambient,
            
            # V. PHYSIOLOGIE AVANCÉE
            "co2_consumption_rate": round(photosynthetic_rate_max * 0.1 * leaf_area_index_lai, 2),
            "night_respiration_rate": round(0.2 * (2.0 ** ((air_temp_internal - 20.0) / 10.0)) * (1 - is_light_period), 3),
            "light_use_efficiency": light_use_efficiency,
            "leaf_absorption_pct": leaf_absorption_pct,
            "spectral_recipe_id": f"SPEC-{random.randint(1,5)}",
            "fresh_biomass_est": fresh_biomass_est,
            "dry_biomass_est": dry_biomass_est,
            "leaf_area_index_lai": leaf_area_index_lai,
            
            # VI. CROISSANCE & DÉVELOPPEMENT
            "root_shoot_ratio": root_shoot_ratio,
            "relative_growth_rate": rgr,
            "net_assimilation_rate": net_assimilation_rate,
            "canopy_height": round(canopy_height, 2),
            "harvest_index": harvest_index,
            "days_since_planting": days_since_planting,
            "thermal_sum_accumulated": round(system_state["thermal_sum"], 1),
            "growth_stage": growth_stage,
            "predicted_yield_kg_m2": predicted_yield_kg_m2,
            "expected_harvest_date": expected_harvest_date,
            "biomass_accumulation_daily": biomass_accumulation_daily,
            
            # VII. RENDEMENT & CIBLE
            "target_harvest_weight": target_harvest_weight,
            "health_score": round(random.uniform(0.7, 1.0), 2),
            "chlorophyll_index_spad": chlorophyll_index_spad,
            
            # VIII. SANTÉ FOLIAIRE
            "stomatal_conductance": stomatal_conductance,
            "anthocyanin_index": round(random.uniform(0.1, 0.5), 2),
            "tip_burn_risk": tip_burn_risk,
            "leaf_temp_delta": round(-stomatal_conductance * 0.5, 2),
            "stem_diameter_micro": round(random.uniform(2000, 5000), 0),
            "sap_flow_rate": round(stomatal_conductance * vapor_pressure_deficit * 10, 3),
            "leaf_wetness_duration": round(random.uniform(0, 2), 1),
            "potential_hydrique_foliaire": round(-0.3 - (stomatal_conductance * 50 / 1000), 3),
            "ethylene_level": round(random.uniform(0.01, 0.1), 3),
            
            # IX. QUALITÉ NUTRITIONNELLE
            "ascorbic_acid_content": round(random.uniform(20, 50), 1),
            "phenolic_content": round(random.uniform(1.5, 3.5), 2),
            "essential_oil_yield": essential_oil_yield,
            "aroma_compounds_ratio": round(aroma_compounds_ratio, 2),
            
            # X. CLIMAT INTERNE
            "air_temp_internal": air_temp_internal,
            "air_humidity": air_humidity,
            "vapor_pressure_deficit": vapor_pressure_deficit,
            "airflow_velocity": airflow_velocity,
            "air_pressure": air_pressure,
            "fan_speed_pct": round(random.uniform(30, 80), 1),
            
            # XI. ENVIRONNEMENT EXTERNE
            "ext_temp_nasa": ext_temp_nasa,
            "ext_humidity_nasa": ext_humidity_nasa,
            "ext_solar_radiation": ext_solar_radiation,
            "oxygen_level": round(random.uniform(20.5, 21.5), 1),
            "dew_point": dew_point,
            "hvac_load_pct": round(abs(air_temp_internal - ref_temp_opt) * 5, 1),
            "co2_injection_status": 1 if co2_level_ambient < co2_target * 0.9 else 0,
            "energy_footprint_hourly": energy_footprint_hourly,
            "renewable_energy_pct": round(random.uniform(20, 40), 1),
            
            # XII. POLLUTION LUMINEUSE
            "ambient_light_pollution": round(random.uniform(0.1, 0.5), 2),
            
            # XIII. SYSTÈME HYDRO
            "water_temp": water_temp,
            "water_ph": water_ph,
            "dissolved_oxygen": dissolved_oxygen,
            "water_turbidity": water_turbidity,
            "wue_current": wue_current,
            "water_recycled_rate": water_recycled_rate,
            "coefficient_cultural_kc": coefficient_cultural_kc,
            "microbial_density": microbial_density,
            "beneficial_microbes_ratio": round(beneficial_microbes_ratio, 2),
            
            # XIV. SANTÉ RACINAIRE
            "root_fungal_pressure": round(root_fungal_pressure, 2),
            "biofilm_thickness": round(random.uniform(0.01, 0.1), 3),
            "algae_growth_index": round(random.uniform(0.1, 0.8), 2),
            "redox_potential": redox_potential,
            
            # XV. IRRIGATION & FERTIGATION
            "irrigation_line_pressure": irrigation_line_pressure,
            "leaching_fraction": round(0.1 + (irrigation_line_pressure - 1.5) * 0.05, 3),
            "energy_price_kwh": energy_price_kwh,
            
            # XVI. ÉCONOMIE & MARCHÉ
            "market_price_kg": system_state["market_price"],
            "lease_index_value": round(random.uniform(80, 120), 1),
            "daily_rent_cost": round(random.uniform(50, 150), 2),
            "lease_profitability_index": round((fresh_biomass_est * system_state["market_price"]) / (operational_cost_total + 50), 2),
            "is_compliant_lease": 1,
            "labor_cost_pro_rata": round(0.10 * fresh_biomass_est, 2),
            "carbon_credit_value": round(random.uniform(10, 30), 2),
            "operational_cost_total": operational_cost_total,
            
            # XVII. DURABILITÉ
            "carbon_footprint_per_kg": round((energy_footprint_hourly * 24 * 0.45 * 0.7) / (fresh_biomass_est / 1000), 2),
            
            # XVIII. MATÉRIEL & CAPTEURS
            "pump_vibration_level": pump_vibration_level,
            "fan_current_draw": fan_current_draw,
            "led_driver_temp": led_driver_temp,
            "filter_differential_pressure": filter_differential_pressure,
            "ups_battery_health": ups_battery_health,
            "leak_detection_status": random.choice([0, 1]),
            "emergency_stop_status": 0,
            "network_latency_ms": round(random.uniform(5, 50), 1),
            
            # XIX. CALIBRATION & INTÉGRITÉ
            "sensor_calibration_offset": round(random.uniform(-0.1, 0.1), 2),
            "module_integrity_score": module_integrity_score,
            "ai_decision_mode": "AUTONOMOUS" if 0.77 > 0.9 and 0.28 < 0.1 else "ASSISTED",
            "anomaly_confidence_score": round(random.uniform(0.0, 0.6), 2),
            
            # XX. PRÉDICTIONS & RISQUES
            "predicted_energy_need_24h": round(energy_footprint_hourly * 24 * 1.1, 2),
            "risk_pest_outbreak": risk_pest_outbreak,
            "irrigation_strategy_id": f"STRAT-{random.randint(1, 5)}",
            
            # XXI. CONFORMITÉ & TRACABILITÉ
            "master_compliance_index": round(random.uniform(0.8, 1.0), 2),
            "blockchain_hash": f"0x{uuid.uuid4().hex[:64]}",
            "audit_trail_signature": f"SIG-{uuid.uuid4().hex[:16]}",
            "quality_grade_prediction": random.choice(["Rejet", "Standard", "Premium"]),
            "system_reboot_count": module_state["system_reboot_count"],
            
            # XXII. CIBLES DE RÉFÉRENCE
            "ref_n_target": ref_n_target,
            "ref_p_target": ref_p_target,
            "ref_k_target": ref_k_target,
            "ref_ca_target": ref_ca_target,
            "ref_mg_target": ref_mg_target,
            "ref_temp_opt": ref_temp_opt,
            "ref_lai_target": 3.5,
            "ref_oil_target": 0.018,
            "ref_wue_target": 3.5,
            "ref_microbial_target": 1e6,
            "ref_photoperiod_opt": ref_humidity_opt,
            "ref_sum_thermal_target": 350,
            "ref_brix_target": 6.5,
            "ref_nitrate_limit": 150,
            "ref_humidity_opt": ref_humidity_opt,
            
            # XXIII. MÉTADONNÉES & LINEAGE
            "data_source_type": "IoT",
            "sensor_hardware_id": f"SENS-{uuid.uuid4().hex[:8]}",
            "api_endpoint_version": "2.1.0",
            "source_reliability_score": round(random.uniform(0.8, 1.0), 2),
            "data_integrity_flag": 0,
            "last_calibration_date": (now - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d"),
            "maintenance_urgency_score": round((1 - module_integrity_score) * 100, 1),
            "lineage_uuid": str(uuid.uuid4()),
        }
        
        # Mise à jour de l'état du module
        module_state["prev_biomass"] = fresh_biomass_est
        module_state["prev_timestamp"] = now
        module_state["daily_biomass_gain"].append(biomass_gain)
        if len(module_state["daily_biomass_gain"]) > 24:
            module_state["daily_biomass_gain"].pop(0)
        
        return record

    def send_feedback_metrics(self):
        """Envoie périodiquement les métriques de feedback"""
        metrics = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "validity_stats": system_state["validity_stats"],
            "module_stats": {},
            "adaptive_parameters": len(system_state["adaptive_parameters"])
        }
        
        # Ajouter les stats par module
        for module_id, history in system_state["correction_history"].items():
            metrics["module_stats"][module_id] = {
                "valid_count": history.get('valid_count', 0),
                "invalid_count": history.get('invalid_count', 0),
                "reliability_score": self.feedback_manager.get_reliability_score(module_id)
            }
        
        # Envoyer les métriques
        self.producer.send(FEEDBACK_TOPIC, value=metrics)
        logger.debug(f"📈 Métriques feedback envoyées: {metrics}")

    def run(self):
        logger.info(f"🚀 Simulateur Adaptatif prêt. Envoi vers: {INPUT_TOPIC}")
        logger.info(f"📡 Écoute des corrections sur: {CORRECTION_TOPIC}")
        
        last_metrics_sent = time.time()
        
        try:
            while True:
                self.trigger_anomaly_with_feedback()
                
                for rack in ["RACK-01", "RACK-02"]:
                    for level in [1, 2, 3, 4]:
                        data = self.generate_record_with_feedback(rack, level)
                        self.producer.send(INPUT_TOPIC, value=data)
                
                # Envoyer les métriques toutes les 30 secondes
                if time.time() - last_metrics_sent > 30:
                    self.send_feedback_metrics()
                    last_metrics_sent = time.time()
                
                # Mise à jour de l'état global
                if system_state["is_drifting"] and system_state["drift_intensity"] > 3.0:
                    logger.info("✅ Anomalie terminée.")
                    system_state["is_drifting"] = False
                    system_state["drift_intensity"] = 0.0
                
                time.sleep(0.25)
                
        except KeyboardInterrupt:
            logger.info("Arrêt du simulateur adaptatif...")
            self.feedback_manager.stop()
            self.producer.close()

if __name__ == "__main__":
    simulator = VertiFlowAdaptiveSimulator()
    simulator.run()
