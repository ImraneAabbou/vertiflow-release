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
MODULE: lab_data_generator.py
DESCRIPTION: Générateur de fichiers CSV pour les analyses laboratoires

Fonctionnalités principales:
    - Simule la saisie manuelle des techniciens
    - Génère des données biologiques avancées (Microbiome, Huiles)
    - Dépose le fichier dans le dossier surveillé par NiFi

Développé par        : @Asama
Ticket(s) associé(s): TICKET-043
Sprint              : Semaine 3 - Simulation

================================================================================
© 2025 VertiFlow Core Team - Tous droits réservés
Développé dans le cadre de l'Initiative Nationale Marocaine JobInTech
au sein de l'École YNOV Maroc Campus
================================================================================
"""

import csv
import random
import time
import os
from datetime import datetime

OUTPUT_DIR = "./data_ingestion/lab_input"

def generate_csv():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    filename = f"{OUTPUT_DIR}/lab_analysis_{int(time.time())}.csv"
    
    headers = ["sample_id", "timestamp", "batch_id", "microbial_density", "essential_oil_yield", "lab_technician"]
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        now = datetime.now()
        
        # Générer 5 échantillons
        for i in range(5):
            writer.writerow([
                f"SAMP-{random.randint(1000,9999)}",
                datetime.now().isoformat(),
                now.astimezone(timezone.utc).isoformat().replace("T", " "),
                f"BATCH-2025-{random.choice(['A','B','C'])}",
                f"{random.randint(100000, 1000000)}", # UFC/g
                round(random.uniform(0.5, 1.8), 2),    # % Huile
                "Tech_Mounir"
            ])
            
    print(f"🧪 Rapport Labo généré : {filename}")

if __name__ == "__main__":
    generate_csv()
