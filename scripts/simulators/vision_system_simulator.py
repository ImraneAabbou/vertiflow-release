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
MODULE: vision_system_simulator.py
DESCRIPTION: Simulateur de caméras intelligentes (Computer Vision)

Fonctionnalités principales:
    - Génération de données de phénoypage (Biomasse, Surface foliaire)
    - Envoi via HTTP POST vers le Listener NiFi
    - Simulation de croissance exponentielle pour tester l'Algo A9

Développé par        : @Mounir
Ticket(s) associé(s): TICKET-041
Sprint              : Semaine 3 - Tests & Simulation

================================================================================
© 2025 VertiFlow Core Team - Tous droits réservés
Développé dans le cadre de l'Initiative Nationale Marocaine JobInTech
au sein de l'École YNOV Maroc Campus
================================================================================
"""

import requests
import json
import time
import random
from datetime import datetime

# Configuration NiFi (Port exposé par ListenHTTP Processor)
# Note: Vous devrez configurer un ListenHTTP sur le port 8081 dans NiFi
NIFI_ENDPOINT = "http://localhost:8081/contentListener"

def generate_vision_data(batch_id):
    # Simulation d'une croissance exponentielle (Sigmoïde)
    days_growth = random.randint(1, 30)
    biomass = 5 * (1.15 ** days_growth) # Croissance rapide
    now = datetime.now(timezone.utc)
    
    return {
        "source": "VISION_AI_EDGE_01",
        "timestamp": now.astimezone(timezone.utc).isoformat().replace("T", " "),
        "batch_id": batch_id,
        "fresh_biomass_est": round(biomass, 1),
        "leaf_area_index_lai": round(biomass / 50, 2),
        "canopy_height": round(days_growth * 1.5, 1),
        "health_score": round(random.uniform(0.85, 0.99), 2),
        "chlorophyll_index_spad": round(random.gauss(45, 2), 1)
    }

def main():
    print(f"📷 Démarrage du simulateur Vision -> {NIFI_ENDPOINT}")
    
    batches = ["BATCH-2025-A", "BATCH-2025-B", "BATCH-2025-C"]
    
    try:
        while True:
            for batch in batches:
                data = generate_vision_data(batch)
                
                try:
                    response = requests.post(NIFI_ENDPOINT, json=data)
                    if response.status_code == 200:
                        print(f"✅ [HTTP] Vision Data envoyée pour {batch} ({data['fresh_biomass_est']}g)")
                    else:
                        print(f"⚠️ [HTTP] Erreur NiFi: {response.status_code}")
                except requests.exceptions.ConnectionError:
                    print("❌ [HTTP] NiFi injoignable (Vérifiez le processeur ListenHTTP sur le port 8081)")
            
            # Simulation : Une analyse toutes les 10 secondes (accéléré)
            time.sleep(10)

    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
