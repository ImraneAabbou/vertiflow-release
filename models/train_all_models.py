#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
PROJET VERTIFLOW - Agriculture Verticale Intelligente
============================================================================
FICHIER: train_all_models.py
DESCRIPTION: Script d'entrainement complet pour tous les modeles ML
             - Oracle (RandomForest yield predictor)
             - Quality Classifier (qualite du produit)
             - Harvest LSTM (prediction date de recolte)
             - Feature Scaler (normalisation des features)

Developpe par       : @Mounir, @Imrane 
Ticket(s) associe(s): TICKET-132
Sprint              : Semaine 6 - ML Pipeline
============================================================================
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pickle
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import yaml

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger("vertiflow.ml.training")

# Paths
BASE_DIR = Path(__file__).parent
MODELS_DIR = BASE_DIR
DATA_DIR = BASE_DIR.parent / "data"
REGISTRY_PATH = MODELS_DIR / "model_registry.yaml"


@dataclass
class TrainingConfig:
    """Configuration pour l'entrainement des modeles."""

    random_state: int = 42
    test_size: float = 0.2
    n_estimators: int = 100
    max_depth: Optional[int] = 10
    min_samples_split: int = 5
    cv_folds: int = 5


@dataclass
class TrainingResult:
    """Resultat d'un entrainement de modele."""

    model_name: str
    success: bool
    metrics: Dict[str, float] = field(default_factory=dict)
    artifact_path: Optional[str] = None
    checksum: Optional[str] = None
    training_time_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)


class ModelTrainer:
    """
    Entraineur de modeles ML pour VertiFlow.

    Cette classe gere l'entrainement, la validation et la serialisation
    des modeles de Machine Learning utilises par le systeme nerveux.
    """

    def __init__(self, config: Optional[TrainingConfig] = None):
        self.config = config or TrainingConfig()
        self.results: List[TrainingResult] = []

        LOGGER.info("Model Trainer initialized (random_state=%d)", self.config.random_state)

    def generate_synthetic_training_data(
        self,
        n_samples: int = 5000,
    ) -> Dict[str, np.ndarray]:
        """
        Genere des donnees synthetiques pour l'entrainement.

        En production, ces donnees viendraient de ClickHouse.
        Pour l'initialisation, on genere des donnees realistes.
        """
        np.random.seed(self.config.random_state)

        LOGGER.info("Generating %d synthetic training samples...", n_samples)

        # Features environnementales (basees sur les plages optimales)
        temperature = np.random.normal(21, 2, n_samples)  # 21C optimal
        humidity = np.random.normal(60, 8, n_samples)  # 60% optimal
        par = np.random.normal(300, 50, n_samples)  # 300 umol/m2/s
        co2 = np.random.normal(800, 150, n_samples)  # 800 ppm
        ec = np.random.normal(1.6, 0.3, n_samples)  # 1.6 mS/cm
        ph = np.random.normal(6.0, 0.3, n_samples)  # 6.0
        vpd = np.random.normal(1.0, 0.2, n_samples)  # 1.0 kPa

        # Rolling averages (24h)
        temp_mean_24h = temperature + np.random.normal(0, 0.5, n_samples)
        par_mean_24h = par + np.random.normal(0, 20, n_samples)
        humidity_mean_24h = humidity + np.random.normal(0, 3, n_samples)
        co2_mean_24h = co2 + np.random.normal(0, 50, n_samples)

        # Variabilite
        temp_stddev_24h = np.abs(np.random.normal(1.5, 0.5, n_samples))

        # Jours depuis plantation
        days_since_planting = np.random.randint(1, 35, n_samples)

        # Target: Yield (g) - modele biophysique simplifie
        # Yield = f(temperature, light, nutrients, time)
        yield_base = 10 + days_since_planting * 1.5  # Croissance lineaire de base

        # Facteur temperature (optimum a 21C)
        temp_factor = 1 - 0.05 * np.abs(temperature - 21)
        temp_factor = np.clip(temp_factor, 0.5, 1.2)

        # Facteur lumiere (optimum a 300)
        light_factor = 1 - 0.002 * np.abs(par - 300)
        light_factor = np.clip(light_factor, 0.6, 1.1)

        # Facteur EC (optimum a 1.6)
        ec_factor = 1 - 0.2 * np.abs(ec - 1.6)
        ec_factor = np.clip(ec_factor, 0.7, 1.1)

        # Yield final avec bruit
        yield_g = yield_base * temp_factor * light_factor * ec_factor
        yield_g += np.random.normal(0, 2, n_samples)  # Bruit
        yield_g = np.clip(yield_g, 5, 100)  # Bornes realistes

        # Quality score (0-100)
        quality_base = 75
        quality = quality_base + 5 * temp_factor + 3 * light_factor + 2 * ec_factor
        quality += np.random.normal(0, 5, n_samples)
        quality = np.clip(quality, 40, 100)

        # Quality class (Good, Medium, Poor)
        quality_class = np.where(quality >= 80, 2, np.where(quality >= 60, 1, 0))

        # Days to harvest (basil: 28 jours optimal)
        days_remaining = 28 - days_since_planting + np.random.normal(0, 2, n_samples)
        days_remaining = np.clip(days_remaining, 0, 35)

        return {
            # Features
            "temperature": temperature,
            "humidity": humidity,
            "par": par,
            "co2": co2,
            "ec": ec,
            "ph": ph,
            "vpd": vpd,
            "temp_mean_24h": temp_mean_24h,
            "par_mean_24h": par_mean_24h,
            "humidity_mean_24h": humidity_mean_24h,
            "co2_mean_24h": co2_mean_24h,
            "temp_stddev_24h": temp_stddev_24h,
            "days_since_planting": days_since_planting,
            # Targets
            "yield_g": yield_g,
            "quality_score": quality,
            "quality_class": quality_class,
            "days_to_harvest": days_remaining,
        }

    def train_oracle_model(self, data: Dict[str, np.ndarray]) -> TrainingResult:
        """
        Entraine le modele Oracle (prediction de rendement).

        Modele: RandomForestRegressor
        Target: yield_g (fresh biomass at harvest)
        """
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_squared_error, r2_score
        from sklearn.model_selection import cross_val_score, train_test_split

        LOGGER.info("Training Oracle model (yield prediction)...")
        start_time = datetime.now()

        result = TrainingResult(model_name="oracle_rf", success=False)

        try:
            # Preparation des features
            feature_names = [
                "temp_mean_24h",
                "par_mean_24h",
                "humidity_mean_24h",
                "co2_mean_24h",
                "temp_stddev_24h",
                "days_since_planting",
                "ec",
                "ph",
            ]

            X = np.column_stack([data[f] for f in feature_names])
            y = data["yield_g"]

            # Split train/test
            X_train, X_test, y_train, y_test = train_test_split(
                X, y,
                test_size=self.config.test_size,
                random_state=self.config.random_state,
            )

            # Entrainement
            model = RandomForestRegressor(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                min_samples_split=self.config.min_samples_split,
                random_state=self.config.random_state,
                n_jobs=-1,
            )

            model.fit(X_train, y_train)

            # Evaluation
            y_pred = model.predict(X_test)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            r2 = r2_score(y_test, y_pred)

            # Cross-validation
            cv_scores = cross_val_score(
                model, X, y,
                cv=self.config.cv_folds,
                scoring="r2",
            )

            # Feature importance
            feature_importance = dict(zip(feature_names, model.feature_importances_))

            # Sauvegarde
            artifact_path = MODELS_DIR / "oracle_rf.pkl"
            with open(artifact_path, "wb") as f:
                pickle.dump({
                    "model": model,
                    "feature_names": feature_names,
                    "training_date": datetime.now().isoformat(),
                    "metrics": {"rmse": rmse, "r2": r2},
                }, f)

            # Checksum
            with open(artifact_path, "rb") as f:
                checksum = hashlib.sha256(f.read()).hexdigest()[:16]

            result.success = True
            result.metrics = {
                "rmse": round(rmse, 4),
                "r2": round(r2, 4),
                "cv_mean_r2": round(np.mean(cv_scores), 4),
                "cv_std_r2": round(np.std(cv_scores), 4),
            }
            result.artifact_path = str(artifact_path)
            result.checksum = checksum

            LOGGER.info("Oracle model trained: RMSE=%.4f, R2=%.4f", rmse, r2)
            LOGGER.info("Feature importance: %s", feature_importance)

        except Exception as e:
            result.errors.append(str(e))
            LOGGER.error("Oracle training failed: %s", e)

        result.training_time_seconds = (datetime.now() - start_time).total_seconds()
        return result

    def train_quality_classifier(self, data: Dict[str, np.ndarray]) -> TrainingResult:
        """
        Entraine le classificateur de qualite.

        Modele: RandomForestClassifier
        Target: quality_class (0=Poor, 1=Medium, 2=Good)
        """
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import classification_report, f1_score
        from sklearn.model_selection import train_test_split

        LOGGER.info("Training Quality Classifier...")
        start_time = datetime.now()

        result = TrainingResult(model_name="quality_classifier_rf", success=False)

        try:
            # Features
            feature_names = [
                "temperature",
                "humidity",
                "par",
                "co2",
                "ec",
                "ph",
                "vpd",
                "days_since_planting",
            ]

            X = np.column_stack([data[f] for f in feature_names])
            y = data["quality_class"].astype(int)

            # Split
            X_train, X_test, y_train, y_test = train_test_split(
                X, y,
                test_size=self.config.test_size,
                random_state=self.config.random_state,
                stratify=y,
            )

            # Entrainement
            model = RandomForestClassifier(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                min_samples_split=self.config.min_samples_split,
                random_state=self.config.random_state,
                n_jobs=-1,
            )

            model.fit(X_train, y_train)

            # Evaluation
            y_pred = model.predict(X_test)
            f1 = f1_score(y_test, y_pred, average="weighted")

            # Sauvegarde
            artifact_path = MODELS_DIR / "rf_quality_v1.pkl"
            with open(artifact_path, "wb") as f:
                pickle.dump({
                    "model": model,
                    "feature_names": feature_names,
                    "class_labels": ["Poor", "Medium", "Good"],
                    "training_date": datetime.now().isoformat(),
                }, f)

            with open(artifact_path, "rb") as f:
                checksum = hashlib.sha256(f.read()).hexdigest()[:16]

            result.success = True
            result.metrics = {"f1_weighted": round(f1, 4)}
            result.artifact_path = str(artifact_path)
            result.checksum = checksum

            LOGGER.info("Quality Classifier trained: F1=%.4f", f1)

        except Exception as e:
            result.errors.append(str(e))
            LOGGER.error("Quality Classifier training failed: %s", e)

        result.training_time_seconds = (datetime.now() - start_time).total_seconds()
        return result

    def train_harvest_predictor(self, data: Dict[str, np.ndarray]) -> TrainingResult:
        """
        Entraine le predicteur de date de recolte.

        Modele: RandomForestRegressor (simplifie, LSTM en production)
        Target: days_to_harvest
        """
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_absolute_error
        from sklearn.model_selection import train_test_split

        LOGGER.info("Training Harvest Predictor...")
        start_time = datetime.now()

        result = TrainingResult(model_name="harvest_predictor_rf", success=False)

        try:
            # Features
            feature_names = [
                "temperature",
                "par",
                "ec",
                "days_since_planting",
                "temp_mean_24h",
            ]

            X = np.column_stack([data[f] for f in feature_names])
            y = data["days_to_harvest"]

            # Split
            X_train, X_test, y_train, y_test = train_test_split(
                X, y,
                test_size=self.config.test_size,
                random_state=self.config.random_state,
            )

            # Entrainement
            model = RandomForestRegressor(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                random_state=self.config.random_state,
                n_jobs=-1,
            )

            model.fit(X_train, y_train)

            # Evaluation
            y_pred = model.predict(X_test)
            mae = mean_absolute_error(y_test, y_pred)

            # Sauvegarde
            artifact_path = MODELS_DIR / "harvest_predictor_rf.pkl"
            with open(artifact_path, "wb") as f:
                pickle.dump({
                    "model": model,
                    "feature_names": feature_names,
                    "training_date": datetime.now().isoformat(),
                }, f)

            with open(artifact_path, "rb") as f:
                checksum = hashlib.sha256(f.read()).hexdigest()[:16]

            result.success = True
            result.metrics = {"mae_days": round(mae, 2)}
            result.artifact_path = str(artifact_path)
            result.checksum = checksum

            LOGGER.info("Harvest Predictor trained: MAE=%.2f days", mae)

        except Exception as e:
            result.errors.append(str(e))
            LOGGER.error("Harvest Predictor training failed: %s", e)

        result.training_time_seconds = (datetime.now() - start_time).total_seconds()
        return result

    def train_feature_scaler(self, data: Dict[str, np.ndarray]) -> TrainingResult:
        """
        Entraine le scaler pour normalisation des features.

        Modele: StandardScaler
        """
        from sklearn.preprocessing import StandardScaler

        LOGGER.info("Training Feature Scaler...")
        start_time = datetime.now()

        result = TrainingResult(model_name="telemetry_scaler", success=False)

        try:
            # Features a normaliser
            feature_names = [
                "temperature",
                "humidity",
                "par",
                "co2",
                "ec",
                "ph",
                "vpd",
            ]

            X = np.column_stack([data[f] for f in feature_names])

            # Fit scaler
            scaler = StandardScaler()
            scaler.fit(X)

            # Sauvegarde
            artifact_path = MODELS_DIR / "telemetry_scaler.pkl"
            with open(artifact_path, "wb") as f:
                pickle.dump({
                    "scaler": scaler,
                    "feature_names": feature_names,
                    "mean": scaler.mean_.tolist(),
                    "std": scaler.scale_.tolist(),
                    "training_date": datetime.now().isoformat(),
                }, f)

            with open(artifact_path, "rb") as f:
                checksum = hashlib.sha256(f.read()).hexdigest()[:16]

            result.success = True
            result.metrics = {
                "n_features": len(feature_names),
                "n_samples_fit": len(X),
            }
            result.artifact_path = str(artifact_path)
            result.checksum = checksum

            LOGGER.info("Feature Scaler trained (%d features)", len(feature_names))

        except Exception as e:
            result.errors.append(str(e))
            LOGGER.error("Feature Scaler training failed: %s", e)

        result.training_time_seconds = (datetime.now() - start_time).total_seconds()
        return result

    def update_model_registry(self, results: List[TrainingResult]) -> None:
        """Met a jour le registre des modeles (model_registry.yaml)."""
        LOGGER.info("Updating model registry...")

        registry = {
            "version": "1.0.0",
            "last_updated": datetime.now().isoformat(),
            "models": {},
        }

        for r in results:
            if r.success:
                registry["models"][r.model_name] = {
                    "artifact_path": r.artifact_path,
                    "checksum_sha256": r.checksum,
                    "metrics": r.metrics,
                    "training_date": datetime.now().strftime("%Y-%m-%d"),
                    "training_time_seconds": round(r.training_time_seconds, 2),
                    "status": "production",
                }

        with open(REGISTRY_PATH, "w") as f:
            yaml.dump(registry, f, default_flow_style=False, sort_keys=False)

        LOGGER.info("Model registry updated: %s", REGISTRY_PATH)

    def run_full_training(self) -> Dict[str, Any]:
        """Execute l'entrainement complet de tous les modeles."""
        LOGGER.info("=" * 60)
        LOGGER.info("VERTIFLOW ML TRAINING PIPELINE")
        LOGGER.info("=" * 60)

        start_time = datetime.now()

        # Generation des donnees
        data = self.generate_synthetic_training_data(n_samples=5000)

        # Entrainement des modeles
        results = [
            self.train_oracle_model(data),
            self.train_quality_classifier(data),
            self.train_harvest_predictor(data),
            self.train_feature_scaler(data),
        ]

        self.results = results

        # Mise a jour du registre
        self.update_model_registry(results)

        # Resume
        total_time = (datetime.now() - start_time).total_seconds()
        successful = sum(1 for r in results if r.success)

        summary = {
            "total_models": len(results),
            "successful": successful,
            "failed": len(results) - successful,
            "total_time_seconds": round(total_time, 2),
            "models": {r.model_name: r.metrics for r in results if r.success},
            "artifacts": [r.artifact_path for r in results if r.success],
        }

        LOGGER.info("=" * 60)
        LOGGER.info("TRAINING COMPLETE: %d/%d models successful", successful, len(results))
        LOGGER.info("Total time: %.2f seconds", total_time)
        LOGGER.info("=" * 60)

        return summary


def main():
    """Point d'entree CLI."""
    import argparse

    parser = argparse.ArgumentParser(description="VertiFlow ML Model Training")
    parser.add_argument(
        "--n-samples",
        type=int,
        default=5000,
        help="Number of training samples",
    )
    parser.add_argument(
        "--n-estimators",
        type=int,
        default=100,
        help="Number of trees in Random Forest",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=MODELS_DIR,
        help="Output directory for models",
    )
    args = parser.parse_args()

    config = TrainingConfig(n_estimators=args.n_estimators)
    trainer = ModelTrainer(config=config)

    summary = trainer.run_full_training()

    print("\n" + "=" * 60)
    print("TRAINING SUMMARY")
    print("=" * 60)
    print(json.dumps(summary, indent=2))

    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
