# 📊 VERTIFLOW - DOCUMENTATION DASHBOARDS GRAFANA

## Vue d'ensemble

Ce document rassemble toute la documentation relative aux dashboards Grafana du projet VertiFlow.
Les fichiers JSON nettoyés (sans commentaires) sont disponibles dans le dossier `dashboards/grafana/clean/`.

---

## 📁 Structure des fichiers

| Fichier | Description | Status |
|---------|-------------|--------|
| `clean/01_operational_cockpit.json` | Cockpit opérationnel temps réel | ✅ Prêt |
| `clean/02_science_lab.json` | Analyse scientifique et ML | ✅ Prêt |
| `clean/03_executive_finance.json` | Dashboard financier exécutif | ✅ Prêt |
| `clean/04_system_health.json` | Santé infrastructure | ✅ Prêt |
| `05_mlops_models.json` | Modèles MLOps | ✅ Déjà valide |
| `06_mongodb_operations.json` | Opérations MongoDB | ✅ Déjà valide |

---

## 🎯 Sources de données requises

### 1. Prometheus
- **URL** : `http://prometheus:9090`
- **Métriques exposées** :
  - `vertiflow_temperature_celsius` - Température par facility
  - `vertiflow_humidity_percent` - Humidité par facility
  - `vertiflow_co2_ppm` - Niveau CO2
  - `vertiflow_light_intensity_umol_m2_s` - Intensité lumineuse
  - `vertiflow_model_health` - Santé des modèles ML
  - `vertiflow_inference_duration_seconds` - Latence d'inférence
  - `vertiflow_predictions_total` - Compteur de prédictions
  - `vertiflow_model_drift_status` - Status de drift
  - `up{job="..."}` - Status des services

### 2. ClickHouse
- **Connexion Grafana** :
  - Server address: `clickhouse`
  - Port: `8123`
  - Protocol: `HTTP`
  - Username: `default`
  - Password: `default`
  - Database: `vertiflow`

- **Tables utilisées** :
  - `telemetry.facility_summary` - Résumé des facilities
  - `telemetry.plant_growth_daily` - Croissance quotidienne
  - `telemetry.quality_assessment` - Évaluation qualité
  - `telemetry.environmental_commands` - Commandes environnement
  - `ml_models.model_performance` - Performance des modèles
  - `ml_models.feature_importance` - Importance des features
  - `science.photosynthesis_validation` - Validation photosynthèse
  - `financial.daily_summary` - Résumé financier quotidien
  - `financial.cost_analysis` - Analyse des coûts
  - `financial.revenue_by_crop` - Revenus par culture
  - `financial.unit_economics` - Économie unitaire
  - `facilities.facility_metrics` - Métriques des facilities

---

## 📋 Dashboard 1: Operational Cockpit

**Référence**: TICKET-119  
**Auteur**: @Imrane (DevOps & Monitoring Lead)  
**Fréquence de rafraîchissement**: 30 secondes

### Panneaux inclus

#### Panel 1: Facility Status Overview
- **Objectif**: Vue d'ensemble rapide de toutes les facilities
- **Source**: ClickHouse.telemetry.facility_summary
- **Affiche**: Nom facility, plantes actives, température moyenne, score qualité, santé système
- **Seuils**: Rouge <60%, Jaune 60-85%, Vert >85%

#### Panel 2: Telemetry Real-Time
- **Objectif**: Monitorer les conditions environnementales en temps réel
- **Source**: Prometheus (via MQTT → Kafka → Prometheus)
- **Métriques**:
  - Température (°C) - Cible: 22-25°C pour basilic [ligne ROUGE]
  - Humidité (%) - Cible: 60-70% [ligne BLEUE]
  - CO2 (ppm) - Cible: 800-1000 [ligne VERTE]
  - Intensité lumineuse (µmol/m²/s) - Cible: 14-16 [ligne JAUNE]

#### Panel 3: Plant Growth Metrics
- **Objectif**: Suivre les taux de croissance quotidiens
- **Source**: ClickHouse.telemetry.plant_growth_daily
- **Affiche**: Accumulation de biomasse quotidienne par plante (g/jour)
- **Seuils**: Rouge <1.5g, Jaune 1.5-2.5g, Vert >2.5g

#### Panel 4: Quality Distribution
- **Objectif**: Suivre les grades de qualité produit
- **Source**: ClickHouse.telemetry.quality_assessment
- **Affiche**: Distribution des grades (PREMIUM, GRADE_A, GRADE_B, REJECT)
- **Cibles**: PREMIUM >30%, GRADE_A >50%, GRADE_B <15%, REJECT <5%

#### Panel 5: Environmental Control Status
- **Objectif**: Monitorer les systèmes de contrôle environnemental
- **Source**: ClickHouse.telemetry.environmental_commands
- **Affiche**: Status HVAC, irrigation, dosage nutriments, lumières, enrichissement CO2

---

## 📋 Dashboard 2: Science Lab

**Référence**: TICKET-120  
**Auteurs**: @Asama (Biologist), @Mounir (ML Engineering)  
**Fréquence de rafraîchissement**: 1 heure

### Panneaux inclus

#### Panel 1: ML Model Performance Dashboard
- **Objectif**: Monitorer la précision de tous les modèles ML VertiFlow
- **Source**: ClickHouse.ml_models.model_performance
- **Modèles suivis**:
  - **Oracle (RandomForest)**: Prédiction rendement (kg/m²) et date récolte
    - Métrique: RMSE <0.5 kg/m², R² >0.92
  - **Classifier (Quality)**: Classification qualité (PREMIUM/A/B/REJECT)
    - Métrique: F1-Score >0.95, Accuracy >96%
  - **Nervous System (LSTM)**: Prévision date récolte
    - Métrique: MAE <3 jours, MAPE <8%

#### Panel 2: Photosynthesis Model Validation
- **Objectif**: Valider le modèle Farquhar vs mesures réelles
- **Source**: ClickHouse.science.photosynthesis_validation
- **Affiche**: Scatter plot taux photosynthèse prédit vs mesuré

#### Panel 3: Feature Importance
- **Objectif**: Analyse d'importance des features pour le modèle de rendement
- **Source**: ClickHouse.ml_models.feature_importance
- **Affiche**: Top 15 features par importance

#### Panel 4: Model Inference Latency
- **Objectif**: Distribution de latence d'inférence des modèles
- **Source**: Prometheus (histogramme)
- **Affiche**: Percentiles p50, p95, p99

#### Panel 5: Model Drift Status
- **Objectif**: Détection de drift des modèles
- **Source**: Prometheus.vertiflow_model_drift_status
- **États**: 0=No Drift (vert), 1=Minor Drift (jaune), 2=Critical Drift (rouge)

---

## 📋 Dashboard 3: Executive Finance

**Référence**: TICKET-121  
**Auteur**: @MrZakaria (Project Lead & Finance)  
**Classification**: Confidentiel - Interne  
**Fréquence de rafraîchissement**: Quotidien

### Panneaux inclus

#### Panel 1: Executive Financial Summary
- **Objectif**: Vue exécutive des 4 KPIs financiers clés
- **Source**: ClickHouse.financial.daily_summary
- **Métriques**:
  - **Total Revenue (MTD)**: Somme mois en cours ($XXX,XXX.XX)
  - **Gross Margin %**: (Revenue - COGS) / Revenue - Cible >70%
  - **EBITDA Margin %**: Profit opérationnel / Revenue - Cible >15%
  - **Yield (kg/m²/year)**: Productivité normalisée par surface

#### Panel 2: Monthly Revenue Trend & Forecast
- **Objectif**: Trajectoire de croissance et prévisions
- **Source**: Prometheus + modèle ARIMA
- **Affiche**: Historique 12+ mois, MTD projeté, prévision 3-6 mois avec intervalle de confiance

#### Panel 3: Cost Structure Breakdown
- **Objectif**: Comprendre la répartition des coûts
- **Source**: ClickHouse.financial.cost_analysis
- **Catégories**:
  - Énergie (~40-50% COGS): LED, HVAC, eau, électronique
  - Nutriments (~15-20% COGS): Fertilisants, pH, substrats
  - Main d'œuvre (~20-30% COGS): Opérations, QA, maintenance
  - Autres (~10-15%): Packaging, transport, overhead

#### Panel 4: Revenue by Crop Type
- **Objectif**: Identifier les cultures les plus rentables
- **Source**: ClickHouse.financial.revenue_by_crop
- **Affiche**: Pie chart distribution revenus par culture

#### Panel 5: Unit Economics by Crop
- **Objectif**: Économie unitaire et rentabilité
- **Source**: ClickHouse.financial.unit_economics
- **Affiche**: Prix/kg, coût/kg, marge %, rendement/m² par culture

---

## 📋 Dashboard 4: System Health

**Référence**: TICKET-122  
**Auteur**: @Imrane (DevOps Lead)  
**Fréquence de rafraîchissement**: 30 secondes

### Panneaux inclus

#### Panel 1: Infrastructure Status
- **Objectif**: Vérification rapide de santé des composants critiques
- **Source**: Prometheus.up{job="..."}
- **Composants**: Kafka, ClickHouse, MongoDB, Redis, Prometheus, Grafana
- **États**: UP (vert), DOWN (rouge)

#### Panel 2: CPU & Memory Utilization
- **Objectif**: Utilisation des ressources compute
- **Source**: Prometheus (node-exporter)
- **Seuils**:
  - CPU: Jaune >70%, Rouge >90%
  - Mémoire: Jaune >80%, Rouge >95%

#### Panel 3: Disk Usage
- **Objectif**: Capacité stockage et performance I/O
- **Source**: Prometheus (node-exporter)
- **Partitions**: / (root), /data (stockage), /var/log (logs)
- **Seuils**: Jaune >75%, Rouge >90%

#### Panel 4: Kafka Metrics
- **Objectif**: Santé du broker et métriques consommateur
- **Source**: Prometheus (JMX exporter)
- **Métriques**: Messages In/sec, Consumer Lag, Partition Count

#### Panel 5: ClickHouse Metrics
- **Objectif**: Performance base de données OLAP
- **Source**: Prometheus (ClickHouse exporter)
- **Métriques**: Queries/sec, Active Queries, Memory Usage

#### Panel 6: Container Status
- **Objectif**: Status et utilisation ressources conteneurs
- **Source**: Prometheus (cAdvisor)
- **Affiche**: CPU, mémoire par conteneur

---

## 📋 Dashboard 5: MLOps Models

**Fichier**: `05_mlops_models.json` (déjà valide)

### Panneaux inclus
- Model Health Status (Oracle, Quality, Harvest, Cortex)
- Model Performance Metrics
- Predictions Counter
- Inference Latency

---

## 📋 Dashboard 6: MongoDB Operations

**Fichier**: `06_mongodb_operations.json` (déjà valide)

### Panneaux inclus
- MongoDB Cluster Status
- Active Connections
- Operations per Second
- Oplog Size & Lag

---

## 🚀 Instructions d'import

### Méthode 1: Import manuel
1. Ouvrir Grafana (http://localhost:3000)
2. Menu → Dashboards → Import
3. "Upload JSON file" → Sélectionner un fichier du dossier `clean/`
4. Configurer les sources de données (Prometheus, ClickHouse)
5. Cliquer "Import"

### Méthode 2: Provisioning automatique
Les dashboards peuvent être provisionnés automatiquement en plaçant les fichiers dans:
```
/etc/grafana/provisioning/dashboards/
```

Configuration du provisioner (`dashboards.yml`):
```yaml
apiVersion: 1
providers:
  - name: 'VertiFlow Dashboards'
    orgId: 1
    folder: 'VertiFlow'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /etc/grafana/provisioning/dashboards
```

---

## 📞 Contacts

| Domaine | Contact | Responsabilité |
|---------|---------|----------------|
| Infrastructure/Monitoring | @Imrane | Prometheus, Grafana, Docker |
| Data Pipelines | @Mouhammed | ClickHouse, Kafka, ETL |
| ML/Architecture | @Mounir | Modèles, KPIs, design |
| Business/Finance | @MrZakaria | Métriques business, stratégie |
| Biologie/Qualité | @Asama | Paramètres agronomiques |

---

## 📅 Historique des versions

| Version | Date | Auteur | Changements |
|---------|------|--------|-------------|
| 1.0.0 | 2026-01-03 | @Imrane | Création initiale |
| 1.0.1 | 2026-01-19 | Équipe | Nettoyage JSON pour compatibilité Grafana |

---

*© 2025-2026 VertiFlow Core Team - Initiative Nationale Marocaine JobInTech - YNOV Maroc Campus*
