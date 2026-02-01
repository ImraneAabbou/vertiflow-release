#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PROJET VERTIFLOW - INTELLIGENCE ARTIFICIELLE (PHASE 4)
================================================================================
MODULE: classifier.py (ALGO A10)
DESCRIPTION: Moteur de classification de qualité (Premium/Standard/Rejet).
             Utilise Random Forest pour évaluer le potentiel qualitatif.

Développé par        : @Mounir & @Mouhammed
Ticket(s) associé(s): TICKET-028
Sprint              : Semaine 4 - Intelligence Prédictive
================================================================================
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

# Ajouter le chemin racine pour importer les constantes
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.vertiflow_constants import KafkaTopics, Infrastructure

# Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [CLASSIFIER] - %(levelname)s - %(message)s')
logger = logging.getLogger("VertiFlowClassifier")

KAFKA_BROKER = Infrastructure.KAFKA_BOOTSTRAP_SERVERS
TOPIC_INPUT = KafkaTopics.TELEMETRY_FULL
TOPIC_OUTPUT = KafkaTopics.QUALITY_PREDICTIONS

class QualityInspector:
    def __init__(self, model_path=None):
        # Chemin modèle relatif au projet (pas au cwd)
        default_model_path = str(PROJECT_ROOT / 'models' / 'rf_quality_v1.pkl')
        self.model_path = model_path or os.getenv('CLASSIFIER_MODEL_PATH', default_model_path)
        self.model = None
        self.features_list = ['air_temp_internal', 'vapor_pressure_deficit', 'light_dli_accumulated', 'nutrient_solution_ec', 'days_since_planting']
        
        self.consumer = KafkaConsumer(
            TOPIC_INPUT,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.load_model()

    def load_model(self):
        if os.path.exists(self.model_path):
            self.model = joblib.load(self.model_path)
            logger.info("✅ Modèle Random Forest chargé.")
        else:
            logger.warning("⚠️ Modèle non trouvé. Mode 'Mock' activé.")

    def mock_predict(self, data):
        """Simulation de classification si pas de modèle."""
        score = data.get('health_score', 0.5)
        if score > 0.85: return "PREMIUM"
        if score > 0.60: return "STANDARD"
        return "REJECT"

    def real_predict(self, data):
        """Classification avec le vrai modèle RandomForest."""
        import numpy as np
        try:
            # Extraction des features alignées avec le simulateur
            features = [
                data.get('air_temp_internal', 22.0),
                data.get('air_humidity', 22.0),
                data.get('light_intensity_ppfd', 14.0),
                data.get('co2_level_ambient', 1.0),
                data.get('nutrient_solution_ec', 1.8),
                data.get('water_ph', 1.0),
                data.get('vapor_pressure_deficit', 1.0),
                data.get('days_since_planting', 20)
            ]
            X = np.array(features).reshape(1, -1)
            prediction_idx = int(self.model["model"].predict(X)[0])
            grades = ["REJECT", "STANDARD", "PREMIUM"]
            return grades[min(prediction_idx, 2)]
        except Exception as e:
            logger.warning(f"Erreur prédiction ML, fallback mock: {e}")
            return self.mock_predict(data)

    def run(self):
        logger.info("🕵️ Inspecteur Qualité A10 en service.")

        for message in self.consumer:
            data = message.value

            # On traite 1 message sur 100 pour ne pas surcharger (Simulation)
            if hash(str(data.get('timestamp'))) % 100 == 0:

                try:
                    # Utilise le vrai modèle si disponible, sinon mock
                    if self.model is not None:
                        prediction = self.real_predict(data)
                    else:
                        prediction = self.mock_predict(data)

                    # Publication
                    result = {
                        "timestamp": datetime.now().isoformat(),
                        "batch_id": data.get('batch_id', 'UNKNOWN'),
                        "algo_id": "A10_RF",
                        "predicted_quality_grade": prediction,
                        "health_score_input": data.get('health_score')
                    }

                    self.producer.send(TOPIC_OUTPUT, value=result)
                    logger.info(f"🏷️ Classification {data.get('batch_id')}: {prediction}")

                except Exception as e:
                    logger.error(f"Erreur classification: {e}")

if __name__ == "__main__":
    inspector = QualityInspector()
    inspector.run()
