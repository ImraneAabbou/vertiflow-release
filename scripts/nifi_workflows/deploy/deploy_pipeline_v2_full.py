#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
VERTIFLOW CLOUD RELEASE - TECHNICAL GOVERNANCE MASTER PROTOCOL v4.2
================================================================================
Version     : 4.2.0 (Gold Standard)
Stability   : Production Robust
Core Logic  : Drift Detection + VPD Calculation + Digital Twin
================================================================================
"""

import requests
import json
import time
import urllib3
import logging
import sys
import os
import asyncio
import math
import socket
import subprocess
from datetime import datetime
from pathlib import Path
from functools import partial

# --- CONFIGURATION DES CONSTANTES TECHNIQUES ---
NIFI_BASE_URL = "https://localhost:8443/nifi-api"
NIFI_USER = "admin"
NIFI_PASS = "ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB"
CK_HOST = "localhost"
CK_PORT = 8123
MONGO_URI = "mongodb://localhost:27017"
KAFKA_BROKERS = "localhost:9092"

# Désactivation des warnings SSL pour les environnements isolés
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- SYSTÈME DE LOGGING INDUSTRIEL ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - [VERTIFLOW_V4.2] - %(message)s'
)
logger = logging.getLogger("VERTIFLOW_V4.2")

# --- TÂCHE 1.1 : Création récursive des répertoires ---
def create_directories():
    """T1.1 : Création de toute l'arborescence projet"""
    directories = [
        "logs",
        "exchange/input/knowledge_base",
        "exchange/input/lab_reports",
        "exchange/alerts",
        "exchange/backups"
    ]

    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            # T1.2 : Application des permissions
            subprocess.run(["chmod", "777", directory], check=True)
            logger.info(f"📁 Répertoire créé avec permissions: {directory}")
        except Exception as e:
            logger.warning(f"⚠️ Erreur création répertoire {directory}: {e}")

    logger.info("✅ Arborescence système créée")

# --- TÂCHE 1.3 : Validation des ports Docker ---
def validate_ports():
    """T1.3 : Vérification de l'accessibilité des services"""
    ports = [
        (CK_HOST, CK_PORT, "ClickHouse"),
        ("localhost", 27017, "MongoDB"),
        ("localhost", 9092, "Kafka"),
        ("localhost", 8443, "NiFi")
    ]

    accessible = []
    for host, port, service in ports:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                logger.info(f"✅ {service} accessible sur {host}:{port}")
                accessible.append(service)
            else:
                logger.warning(f"⚠️ {service} inaccessible sur {host}:{port}")
        except Exception as e:
            logger.warning(f"⚠️ Erreur test {service}: {e}")

    return len(accessible) == len(ports)

# --- TÂCHE 1.5 : Vérification du driver ClickHouse ---
def verify_clickhouse_driver():
    """T1.5 : Vérifie la présence du driver JDBC ClickHouse"""
    driver_paths = [
        "/opt/nifi/nifi-current/drivers/clickhouse-jdbc-0.6.0-all.jar",
        "/opt/nifi/nifi-current/drivers/clickhouse-jdbc.jar",
        "/drivers/clickhouse-jdbc-0.6.0-all.jar"
    ]

    for path in driver_paths:
        if os.path.exists(path):
            logger.info(f"✅ Driver ClickHouse trouvé: {path}")
            return path

    logger.error("❌ Driver ClickHouse introuvable")
    return None

# --- MOTEUR DE SESSION AVANCÉ AVEC RETRY EXPONENTIEL ---
class NiFiSessionManager:
    """Gère l'état de la connexion avec retry exponentiel et résilience réseau"""
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.token = None
        self.headers = {'Content-Type': 'application/json'}

        adapter = requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_connections=20,
            pool_maxsize=20
        )
        self.session.mount('https://', adapter)

    async def exponential_backoff_retry(self, operation, max_retries=5, base_delay=2):
        """Retry exponentiel pour les opérations critiques"""
        for attempt in range(max_retries):
            try:
                return await operation()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                delay = base_delay * (2 ** attempt)
                logger.warning(f"⚠️ Tentative {attempt+1}/{max_retries} échouée, attente {delay}s: {e}")
                await asyncio.sleep(delay)

    async def authenticate(self):
        """Authentification auprès de NiFi avec gestion d'erreurs robuste"""
        logger.info(f"🔑 Authentification auprès de NiFi ({NIFI_BASE_URL})...")

        async def auth_operation():
            auth_data = {'username': NIFI_USER, 'password': NIFI_PASS}
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(
                    self.session.post,
                    f"{NIFI_BASE_URL}/access/token",
                    data=auth_data,
                    timeout=30
                )
            )

            if response.status_code == 201:
                self.token = response.text.strip()
                self.headers['Authorization'] = f'Bearer {self.token}'
                self.session.headers.update(self.headers)
                logger.info("✅ Authentification réussie")
                return True
            else:
                raise Exception(f"Code {response.status_code}: {response.text}")

        return await self.exponential_backoff_retry(auth_operation, max_retries=5, base_delay=3)

    # --- TÂCHE 2.1 : Extraction dynamique du Root ID ---
    def get_root_id(self):
        """T2.1 : Récupère l'ID du root process group"""
        try:
            response = self.session.get(f"{NIFI_BASE_URL}/process-groups/root", timeout=15)
            if response.status_code == 200:
                root_id = response.json()['id']
                logger.info(f"📂 Racine NiFi localisée: {root_id}")
                return root_id
            else:
                logger.error(f"❌ Impossible de récupérer root ID: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"❌ Erreur récupération root ID: {e}")
            return None

# --- GESTIONNAIRE D'INTÉGRITÉ DES COMPOSANTS AVANCÉ ---
class ComponentIntegrityManager:
    """Gère l'idempotence avec vérification des dépendances et intégrité complète"""

    def __init__(self, session_manager):
        self.sm = session_manager
        self.component_cache = {}
        self.enabled_services = set()

    def get_component_by_name(self, parent_id, name, component_type, refresh_cache=False):
        """Récupère un composant par son nom avec cache intelligent"""
        cache_key = f"{parent_id}_{name}_{component_type}"

        if not refresh_cache and cache_key in self.component_cache:
            return self.component_cache[cache_key]

        try:
            endpoint_map = {
                'controller-services': ('/flow/process-groups/{parent_id}/controller-services', 'controllerServices'),
                'process-groups': ('/process-groups/{parent_id}/process-groups', 'processGroups'),
                'processors': ('/process-groups/{parent_id}/processors', 'processors'),
                'input-ports': ('/process-groups/{parent_id}/input-ports', 'inputPorts'),
                'output-ports': ('/process-groups/{parent_id}/output-ports', 'outputPorts'),
                'connections': ('/process-groups/{parent_id}/connections', 'connections')
            }

            if component_type not in endpoint_map:
                logger.error(f"Type de composant non supporté: {component_type}")
                return None

            endpoint, key = endpoint_map[component_type]
            url = f"{NIFI_BASE_URL}{endpoint.format(parent_id=parent_id)}"

            response = self.sm.session.get(url, timeout=15)
            if response.status_code == 200:
                items = response.json().get(key, [])
                for item in items:
                    comp = item.get('component', item)
                    if comp.get('name') == name:
                        self.component_cache[cache_key] = item
                        return item

            self.component_cache[cache_key] = None
            return None

        except Exception as e:
            logger.warning(f"⚠️ Erreur recherche composant {name}: {e}")
            return None

    # --- TÂCHE 3.6 : Polling bloquant pour vérification ENABLED ---
    async def wait_for_service_enabled(self, service_id, timeout=60):
        """T3.6 : Attend qu'un service soit en état ENABLED"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            state = self.get_component_state(service_id)
            if state == "ENABLED":
                self.enabled_services.add(service_id)
                logger.info(f"✅ Service {service_id} confirmé ENABLED")
                return True
            elif state in ["DISABLED", "DISABLING"]:
                logger.warning(f"⚠️ Service {service_id} en état {state}")
                return False
            else:
                await asyncio.sleep(3)

        logger.error(f"❌ Timeout attente ENABLED pour {service_id}")
        return False

    def get_component_state(self, component_id, endpoint="controller-services"):
        """Récupère l'état actuel d'un composant"""
        try:
            url = f"{NIFI_BASE_URL}/{endpoint}/{component_id}"
            response = self.sm.session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()['component']['state']
            return None
        except Exception as e:
            logger.warning(f"Erreur récupération état {component_id}: {e}")
            return None

    def clear_cache(self):
        """Vide le cache"""
        self.component_cache.clear()

    def resolve_pg_id(self, parent_id, name):
        """Traduit un nom de Process Group en ID NiFi stable"""
        existing = self.get_component_by_name(parent_id, name, 'process-groups')
        if existing:
            return existing['id']
        return None

# --- DÉBUT DE LA LOGIQUE DE GOUVERNANCE V4.2 ---
class VertiFlowGovernanceMasterV42:
    def __init__(self):
        self.sm = NiFiSessionManager()
        self.integrity = ComponentIntegrityManager(self.sm)
        self.root_id = None
        self.master_pg_id = None
        self.zones = {}
        self.services = {}
        self.ports = {}
        self.connections = {}
        self.driver_path = None

    async def initialize_core(self):
        """Prépare le terrain et vérifie la disponibilité de l'API"""
        logger.info("🚀 Initialisation du Core VertiFlow v4.2...")

        # T1.1 & T1.2 : Création des répertoires
        create_directories()

        # T1.3 : Validation des ports
        if not validate_ports():
            logger.warning("⚠️ Certains services ne sont pas accessibles")

        # T1.5 : Vérification du driver
        self.driver_path = verify_clickhouse_driver()
        if not self.driver_path:
            logger.error("🛑 Driver ClickHouse requis. Installation:")
            logger.error("  wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar")
            return False

        if not await self.sm.authenticate():
            logger.error("🛑 Impossible d'initialiser le Core. Abandon.")
            return False

        # T2.1 : Extraction du Root ID
        self.root_id = self.sm.get_root_id()
        if not self.root_id:
            logger.error("🛑 Impossible de récupérer le root ID.")
            return False

        return True

    async def cleanup_system(self):
        """Nettoie complètement le système avant déploiement"""
        logger.info("🧹 Nettoyage complet du système...")

        try:
            # Arrêter tous les process groups
            response = self.sm.session.get(
                f"{NIFI_BASE_URL}/flow/process-groups/{self.root_id}",
                timeout=15
            )

            if response.status_code == 200:
                process_groups = response.json().get('processGroupFlow', {}).get('flow', {}).get('processGroups', [])
                for pg in process_groups:
                    if pg['status']['aggregateSnapshot']['activeThreadCount'] > 0:
                        stop_payload = {"id": pg['id'], "state": "STOPPED"}
                        self.sm.session.put(
                            f"{NIFI_BASE_URL}/flow/process-groups/{pg['id']}",
                            json=stop_payload,
                            timeout=15
                        )

            logger.info("✅ Nettoyage système terminé")
            await asyncio.sleep(5)

        except Exception as e:
            logger.warning(f"⚠️ Erreur lors du nettoyage: {e}")

    # --- TÂCHE 2.2 : Création du conteneur maître ---
    async def create_master_container(self):
        """T2.2 : Crée le conteneur maître VERTIFLOW_GOLD_V42"""
        existing = self.integrity.get_component_by_name(self.root_id, "VERTIFLOW_GOLD_V42", 'process-groups')
        if existing:
            self.master_pg_id = existing['id']
            logger.info(f"♻️ Réutilisation du conteneur maître: {self.master_pg_id}")
            return self.master_pg_id

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": "VERTIFLOW_GOLD_V42",
                "position": {"x": 0, "y": 0}
            }
        }

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(
                    self.sm.session.post,
                    f"{NIFI_BASE_URL}/process-groups/{self.root_id}/process-groups",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code == 201:
                self.master_pg_id = response.json()['id']
                logger.info(f"📦 Conteneur maître créé: {self.master_pg_id}")
                await asyncio.sleep(2)
                return self.master_pg_id
            else:
                logger.error(f"❌ Échec création conteneur: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ Exception création conteneur: {e}")
            return None

    async def create_standard_pg(self, parent_id, name, x=0, y=0):
        """Crée un Process Group avec gestion automatique des doublons"""
        existing_id = self.integrity.resolve_pg_id(parent_id, name)
        if existing_id:
            logger.info(f"♻️ Réutilisation du groupe existant: {name}")
            return existing_id

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": {"x": x, "y": y}
            }
        }

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(
                    self.sm.session.post,
                    f"{NIFI_BASE_URL}/process-groups/{parent_id}/process-groups",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code == 201:
                new_id = response.json()['id']
                logger.info(f"📁 Nouveau Process Group créé: {name} -> {new_id}")
                await asyncio.sleep(1)
                return new_id
            else:
                logger.error(f"❌ Échec création PG {name}: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ Exception création PG {name}: {e}")
            return None

    # --- GESTION DES PORTS D'INTERFACE ---
    async def create_port(self, pg_id, port_type, name, position=(0, 0)):
        """Crée un port d'entrée ou de sortie"""
        existing = self.integrity.get_component_by_name(
            pg_id,
            name,
            'input-ports' if port_type == "INPUT_PORT" else 'output-ports'
        )

        if existing:
            port_id = existing['id']
            logger.info(f"♻️ Port {name} déjà existant: {port_id}")
            return port_id

        endpoint = "input-ports" if port_type == "INPUT_PORT" else "output-ports"
        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": port_type,
                "position": {"x": position[0], "y": position[1]},
                "parentGroupId": pg_id,
                "allowRemoteAccess": True
            }
        }

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(
                    self.sm.session.post,
                    f"{NIFI_BASE_URL}/process-groups/{pg_id}/{endpoint}",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code == 201:
                port_id = response.json()['id']
                logger.info(f"🚪 Port {name} créé: {port_id}")
                await asyncio.sleep(1)
                return port_id
            else:
                logger.error(f"❌ Échec création port {name}: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ Exception création port {name}: {e}")
            return None

    # --- TÂCHE 2.3 : Liaison inter-zones ---
    async def establish_inter_zone_connection(self, parent_pg_id, source_zone_id, source_port_id,
                                              dest_zone_id, dest_port_id, connection_name):
        """T2.3 & T2.4 : Établit une connexion entre zones avec selectedRelationships = []"""

        existing = self.integrity.get_component_by_name(parent_pg_id, connection_name, 'connections')
        if existing:
            logger.info(f"♻️ Connexion inter-zone déjà existante: {connection_name}")
            return existing['id']

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": connection_name,
                "source": {
                    "id": source_port_id,
                    "groupId": source_zone_id,
                    "type": "OUTPUT_PORT"
                },
                "destination": {
                    "id": dest_port_id,
                    "groupId": dest_zone_id,
                    "type": "INPUT_PORT"
                },
                "selectedRelationships": [],  # T2.4 : Liste vide pour les ports
                "flowFileExpiration": "0 sec",
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB",
                "prioritizers": []
            }
        }

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(
                    self.sm.session.post,
                    f"{NIFI_BASE_URL}/process-groups/{parent_pg_id}/connections",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code == 201:
                conn_id = response.json()['id']
                logger.info(f"🌉 Connexion inter-zone établie: {connection_name}")
                self.connections[connection_name] = conn_id
                return conn_id
            elif response.status_code == 409:
                logger.info(f"🌉 Connexion déjà existante: {connection_name}")
                return None
            else:
                logger.error(f"❌ Impossible de créer la connexion: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ Erreur création connexion: {e}")
            return None

    # --- TÂCHE 3 : SÉQUENÇAGE BLOQUANT DES SERVICES ---
    async def deploy_controller_service(self, pg_id, svc_type, name, properties, comments=""):
        """Déploie un service avec vérification d'idempotence"""
        existing = self.integrity.get_component_by_name(pg_id, name, 'controller-services')
        if existing:
            svc_id = existing['id']
            logger.info(f"♻️ Service '{name}' déjà existant: {svc_id}")
            return svc_id

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": svc_type,
                "properties": properties,
                "comments": f"VERTIFLOW_V4.2: {comments} (Deployed: {datetime.now()})"
            }
        }

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(
                    self.sm.session.post,
                    f"{NIFI_BASE_URL}/process-groups/{pg_id}/controller-services",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code == 201:
                svc_id = response.json()['id']
                logger.info(f"🔧 Service '{name}' provisionné: {svc_id}")
                return svc_id
            else:
                logger.error(f"❌ Échec création service {name}: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"🚨 Exception création service {name}: {str(e)}")
            return None

    async def ensure_service_enabled_robust(self, svc_id, svc_name):
        """Active un service avec retry exponentiel et vérification finale"""
        max_retries = 5
        base_delay = 3

        for attempt in range(max_retries):
            try:
                url = f"{NIFI_BASE_URL}/controller-services/{svc_id}"
                loop = asyncio.get_event_loop()

                # Récupérer état actuel
                response = await loop.run_in_executor(
                    None,
                    partial(self.sm.session.get, url, timeout=15)
                )

                if response.status_code != 200:
                    await asyncio.sleep(base_delay * (2 ** attempt))
                    continue

                svc_data = response.json()
                current_state = svc_data['component']['state']
                current_ver = svc_data['revision']['version']

                if current_state == "ENABLED":
                    logger.info(f"🟢 Service {svc_name} déjà opérationnel")
                    return True

                # Activer le service
                enable_payload = {
                    "revision": {"version": current_ver},
                    "component": {
                        "id": svc_id,
                        "state": "ENABLED"
                    }
                }

                enable_response = await loop.run_in_executor(
                    None,
                    partial(self.sm.session.put, url, json=enable_payload, timeout=15)
                )

                if enable_response.status_code == 200:
                    logger.info(f"⚡ Service {svc_name} activé (tentative {attempt+1})")
                    await asyncio.sleep(base_delay)

                    # Vérification finale
                    check_response = await loop.run_in_executor(
                        None,
                        partial(self.sm.session.get, url, timeout=15)
                    )

                    if check_response.status_code == 200:
                        final_state = check_response.json()['component']['state']
                        if final_state == "ENABLED":
                            logger.info(f"✅ Service {svc_name} confirmé ENBALED")
                            return True

                await asyncio.sleep(base_delay * (2 ** attempt))

            except Exception as e:
                logger.warning(f"⚠️ Erreur activation {svc_name} (tentative {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)

        logger.error(f"❌ Échec d'activation du service {svc_name}")
        return False

    # --- CONFIGURATION DES SERVICES CRITIQUES ---
    async def configure_master_services(self):
        """T3.1 à T3.7 : Configuration de tous les services avec séquençage bloquant"""
        logger.info("🛠️ Phase de configuration des Services de Contrôle v4.2")

        # T3.1 : StandardSSLContextService
        ssl_props = {
            "Keystore Filename": "",
            "Keystore Password": "",
            "Key Password": "",
            "Keystore Type": "JKS",
            "Truststore Filename": "",
            "Truststore Password": "",
            "Truststore Type": "JKS"
        }

        self.services['ssl_context'] = await self.deploy_controller_service(
            self.root_id,
            "org.apache.nifi.ssl.StandardSSLContextService",
            "MASTER_SSL_CONTEXT_V42",
            ssl_props,
            "SSL Context for External APIs"
        )

        if self.services['ssl_context']:
            await self.ensure_service_enabled_robust(self.services['ssl_context'], "SSL_CONTEXT")

        # T3.2 : StandardHTTPContextMap
        http_context_props = {
            "SSL Context Service": f"controller-service://{self.services['ssl_context']}" if self.services.get('ssl_context') else "",
            "Connection Timeout": "30 secs",
            "Read Timeout": "30 secs"
        }

        self.services['http_context'] = await self.deploy_controller_service(
            self.root_id,
            "org.apache.nifi.http.StandardHttpContextMap",
            "MASTER_HTTP_CONTEXT_V42",
            http_context_props,
            "HTTP Context for External APIs"
        )

        if self.services['http_context']:
            await self.ensure_service_enabled_robust(self.services['http_context'], "HTTP_CONTEXT")

        # T3.3 : ClickHouse Pool avec driver moderne
        ck_props = {
            "Database Connection URL": f"jdbc:clickhouse://{CK_HOST}:{CK_PORT}/vertiflow",
            "Database Driver Class Name": "com.clickhouse.jdbc.ClickHouseDriver",
            "database-driver-locations": f"file://{self.driver_path}",
            "Database User": "default",
            "Password": "default",
            "Max Total Connections": "15",
            "Max Wait Time": "15000 millis",
            "Validation-query": "SELECT 1",
            "Max Idle Connections": "5",
            "Min Idle Connections": "1"
        }

        self.services['ck_pool'] = await self.deploy_controller_service(
            self.root_id,
            "org.apache.nifi.dbcp.DBCPConnectionPool",
            "MASTER_CLICKHOUSE_POOL_V42",
            ck_props,
            "OLAP Storage Connector v4.2"
        )

        if self.services['ck_pool']:
            await self.ensure_service_enabled_robust(self.services['ck_pool'], "CLICKHOUSE_POOL")

        # T3.4 : MongoDB Client Service
        mongo_props = {
            "Mongo URI": MONGO_URI,
            "Mongo Database Name": "vertiflow_ops",
            "Socket Timeout": "15000 ms",
            "Server Selection Timeout": "15000 ms",
            "Connect Timeout": "10000 ms",
            "Max Connection Idle Time": "60000 ms"
        }

        self.services['mongo_svc'] = await self.deploy_controller_service(
            self.root_id,
            "org.apache.nifi.mongodb.MongoDBControllerService",
            "MASTER_MONGODB_CLIENT_V42",
            mongo_props,
            "NoSQL Connector v4.2"
        )

        if self.services['mongo_svc']:
            await self.ensure_service_enabled_robust(self.services['mongo_svc'], "MONGODB_CLIENT")

        # T3.5 : JSON Reader/Writer
        json_reader_props = {
            "Schema Access Strategy": "infer-schema",
            "Schema Registry": "none",
            "Date Format": "yyyy-MM-dd",
            "Time Format": "HH:mm:ss",
            "Timestamp Format": "yyyy-MM-dd HH:mm:ss.SSS"
        }

        self.services['json_reader'] = await self.deploy_controller_service(
            self.root_id,
            "org.apache.nifi.json.JsonTreeReader",
            "MASTER_JSON_READER_V42",
            json_reader_props,
            "Unified JSON Parsing Engine v4.2"
        )

        if self.services['json_reader']:
            await self.ensure_service_enabled_robust(self.services['json_reader'], "JSON_READER")

        json_writer_props = {
            "Schema Write Strategy": "no-schema",
            "Schema Access Strategy": "inherit-record-schema",
            "Pretty Print JSON": "false",
            "Output Grouping": "output-array",
            "Timestamp Format": "yyyy-MM-dd HH:mm:ss.SSS"
        }

        self.services['json_writer'] = await self.deploy_controller_service(
            self.root_id,
            "org.apache.nifi.json.JsonRecordSetWriter",
            "MASTER_JSON_WRITER_V42",
            json_writer_props,
            "Unified JSON Output Engine v4.2"
        )

        if self.services['json_writer']:
            await self.ensure_service_enabled_robust(self.services['json_writer'], "JSON_WRITER")

        # T3.7 : MongoDB Lookup Service
        if self.services.get('mongo_svc'):
            lookup_props = {
                "Mongo Lookup Client Service": f"controller-service://{self.services['mongo_svc']}",
                "Mongo Database Name": "vertiflow_ops",
                "Mongo Collection Name": "plant_recipes",
                "Lookup Value Field": "recipe_data",
                "Lookup Keys": "plant_type"
            }

            self.services['mongo_lookup'] = await self.deploy_controller_service(
                self.root_id,
                "org.apache.nifi.mongodb.MongoDBLookupService",
                "MASTER_RECIPE_LOOKUP_V42",
                lookup_props,
                "Real-time enrichment engine v4.2"
            )

            if self.services['mongo_lookup']:
                await self.ensure_service_enabled_robust(self.services['mongo_lookup'], "RECIPE_LOOKUP")

        # Vérification finale des services
        await self.validate_all_services()
        logger.info("🏁 Tous les services de contrôle v4.2 sont configurés")

    async def validate_all_services(self):
        """Valide que tous les services sont activés"""
        logger.info("🔍 Validation de l'état des services...")

        all_enabled = True
        for svc_name, svc_id in self.services.items():
            if svc_id:
                state = self.integrity.get_component_state(svc_id)
                if state == "ENABLED":
                    logger.info(f"  ✅ {svc_name}: {state}")
                else:
                    logger.warning(f"  ⚠️ {svc_name}: {state}")
                    all_enabled = False

        if all_enabled:
            logger.info("🎉 Tous les services sont activés!")
        else:
            logger.warning("⚠️ Certains services ne sont pas activés")

        return all_enabled

    # --- MOTEUR DE DÉPLOIEMENT DES PROCESSEURS ---
    async def create_processor_instance(self, pg_id, p_type, name, pos=(0, 0), props=None, auto_terminate=None):
        """Crée un processeur avec vérification des dépendances"""

        # T3.6 : Vérification bloquante des services
        if props:
            for prop_name, prop_value in props.items():
                if prop_value and isinstance(prop_value, str):
                    if 'controller-service://' in prop_value:
                        svc_id = prop_value.split('/')[-1]
                        if svc_id not in self.integrity.enabled_services:
                            logger.warning(f"⚠️ Service {svc_id} référencé par {name} n'est pas activé")
                            # On attend que le service soit activé
                            await self.integrity.wait_for_service_enabled(svc_id)

        existing = self.integrity.get_component_by_name(pg_id, name, 'processors')
        if existing:
            proc_id = existing['id']
            logger.info(f"♻️ Processeur '{name}' déjà présent: {proc_id}")
            return proc_id

        processor_config = {}
        if props:
            processor_config["properties"] = props
        if auto_terminate:
            processor_config["autoTerminatedRelationships"] = auto_terminate

        processor_config["schedulingStrategy"] = "TIMER_DRIVEN"
        processor_config["schedulingPeriod"] = "0 sec"
        processor_config["penaltyDuration"] = "30 sec"
        processor_config["yieldDuration"] = "10 sec"
        processor_config["bulletinLevel"] = "WARN"
        processor_config["runDurationMillis"] = 0

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": p_type,
                "position": {"x": pos[0], "y": pos[1]},
                "config": processor_config
            }
        }

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(
                    self.sm.session.post,
                    f"{NIFI_BASE_URL}/process-groups/{pg_id}/processors",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code == 201:
                proc_id = response.json()['id']
                logger.info(f"⚙️ Processeur '{name}' déployé: {proc_id}")
                await asyncio.sleep(1)
                return proc_id
            else:
                logger.error(f"❌ Échec création processeur {name}: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ Exception création processeur {name}: {e}")
            return None

    async def establish_connection(self, pg_id, src_id, dst_id, relationships, name=""):
        """Moteur de liaison entre processeurs"""

        existing = self.integrity.get_component_by_name(pg_id, name, 'connections')
        if existing:
            logger.info(f"🔗 Connexion déjà existante: {name}")
            return existing['id']

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "source": {"id": src_id, "type": "PROCESSOR", "groupId": pg_id},
                "destination": {"id": dst_id, "type": "PROCESSOR", "groupId": pg_id},
                "selectedRelationships": relationships,
                "flowFileExpiration": "0 sec",
                "backPressureObjectThreshold": 5000,
                "backPressureDataSizeThreshold": "1 GB",
                "prioritizers": []
            }
        }

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                partial(
                    self.sm.session.post,
                    f"{NIFI_BASE_URL}/process-groups/{pg_id}/connections",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code == 201:
                conn_id = response.json()['id']
                logger.info(f"🔗 Connexion établie: {name}")
                return conn_id
            elif response.status_code == 409:
                logger.info(f"🔗 Connexion déjà existante: {name}")
                return None
            else:
                logger.error(f"❌ Impossible de créer la connexion: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ Erreur création connexion: {e}")
            return None

    # --- ZONE 0 : EXTERNAL DATA HARVESTING ---
    async def deploy_zone_0(self):
        """T4.1 : Configuration de la Zone 0 avec APIs externes"""
        logger.info("🌐 Déploiement Zone 0 - APIs Externes v4.2")

        z0_id = await self.create_standard_pg(self.master_pg_id, "Zone 0 - External Data APIs v4.2", -800, 0)
        if not z0_id:
            return None

        self.zones['z0'] = z0_id

        # NASA POWER API
        nasa_props = {
            "HTTP Method": "GET",
            "Remote URL": "https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,RH2M&community=AG&longitude=-7.58&latitude=33.57&format=JSON&start=20250101&end=20250131",
            "Connection Timeout": "45 secs",
            "Read Timeout": "45 secs",
            "SSL Context Service": f"controller-service://{self.services['ssl_context']}" if self.services.get('ssl_context') else "",
            "Content-Type": "application/json",
            "use-chunked-encoding": "false",
            "disable-peer-verification": "true"
        }

        nasa_fetcher = await self.create_processor_instance(
            z0_id,
            "org.apache.nifi.processors.standard.InvokeHTTP",
            "Z0_NASA_POWER_Harvester_V42",
            (0, 0),
            nasa_props,
            ["failure", "no retry", "retry"]
        )

        # Open-Meteo API
        meteo_url = "https://api.open-meteo.com/v1/forecast?latitude=33.57&longitude=-7.58&hourly=temperature_2m,relativehumidity_2m&timezone=auto"
        meteo_props = {
            "HTTP Method": "GET",
            "Remote URL": meteo_url,
            "Connection Timeout": "15 secs",
            "Read Timeout": "15 secs",
            "SSL Context Service": f"controller-service://{self.services['ssl_context']}" if self.services.get('ssl_context') else "",
            "Content-Type": "application/json",
            "use-chunked-encoding": "false"
        }

        meteo_fetcher = await self.create_processor_instance(
            z0_id,
            "org.apache.nifi.processors.standard.InvokeHTTP",
            "Z0_OpenMeteo_Realtime_V42",
            (0, 200),
            meteo_props,
            ["failure", "no retry", "retry"]
        )

        # MergeContent pour combiner les réponses
        merge_props = {
            "Merge Strategy": "Bin-Packing Algorithm",
            "Merge Format": "Binary Concatenation",
            "Correlation Attribute Name": "weather.source",
            "Minimum Number of Entries": "1",
            "Maximum Number of Entries": "10",
            "Max Bin Age": "30 sec",
            "Max Bin Size": "10 MB",
            "Delimiter Strategy": "Text",
            "Header": "[",
            "Footer": "]",
            "Demarcator": ","
        }

        weather_merger = await self.create_processor_instance(
            z0_id,
            "org.apache.nifi.processors.standard.MergeContent",
            "Z0_Weather_Data_Merger",
            (300, 100),
            merge_props,
            ["failure", "original"]
        )

        # OUTPUT PORT vers Zone 1
        output_port = await self.create_port(z0_id, "OUTPUT_PORT", "Z0_To_Z1_External_V42", (500, 100))
        self.ports['z0_output'] = output_port

        # Connexions internes
        if nasa_fetcher and weather_merger:
            await self.establish_connection(z0_id, nasa_fetcher, weather_merger, ["success"], "NASA_To_Merger")

        if meteo_fetcher and weather_merger:
            await self.establish_connection(z0_id, meteo_fetcher, weather_merger, ["success"], "Meteo_To_Merger")

        if weather_merger and output_port:
            await self.establish_connection(z0_id, weather_merger, output_port, ["merged"], "Merger_To_Output")

        logger.info("✅ Zone 0 v4.2 opérationnelle")
        return z0_id

    # --- ZONE 1 : INGESTION HYBRIDE & DRIFT DETECTION ---
    async def deploy_zone_1(self):
        """T4.2 à T4.5 : Ingestion hybride avec détection de dérives"""
        logger.info("📥 Déploiement Zone 1 - Ingestion & Drift Detection v4.2")

        z1_id = await self.create_standard_pg(self.master_pg_id, "Zone 1 - Ingestion & Drift Detection v4.2", 0, 0)
        if not z1_id:
            return None

        self.zones['z1'] = z1_id

        # INPUT PORT depuis Zone 0
        input_port_z0 = await self.create_port(z1_id, "INPUT_PORT", "Z1_From_Z0_External_V42", (-200, 100))
        self.ports['z1_input_external'] = input_port_z0

        # T4.2 : Consumer Kafka pour le simulateur IoT
        kafka_props = {
            "bootstrap.servers": KAFKA_BROKERS,
            "topic": "vertiflow.ingestion.raw",
            "group.id": "nifi-ingestion-group-v42",
            "auto.offset.reset": "latest",
            "key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "max.poll.records": "100",
            "session.timeout.ms": "30000",
            "Heartbeat Interval": "3000 ms"
        }

        kafka_consumer = await self.create_processor_instance(
            z1_id,
            "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
            "Z1_Kafka_IoT_Consumer_V42",
            (0, 0),
            kafka_props
        )

        # HTTP Ingestor
        http_props = {
            "Listening Port": "8082",
            "Base Path": "/vertiflow/v42/ingest",
            "HTTP Method": "POST",
            "Return Code": "201",
            "Max Data to Receive per Second": "50 MB",
            "Max Request Size": "10 MB",
            "Header Character Set": "UTF-8"
        }

        http_ingestor = await self.create_processor_instance(
            z1_id,
            "org.apache.nifi.processors.standard.ListenHTTP",
            "Z1_HTTP_Edge_Ingestor_V42",
            (0, 200),
            http_props
        )

        # T4.4 : UpdateAttribute pour mapper drift_type
        drift_mapper = await self.create_processor_instance(
            z1_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            "Z1_Drift_Type_Mapper_V42",
            (200, 0),
            {
                "drift_type": "${drift_type:toUpper()}",
                "drift_intensity": "${drift_intensity:toNumber()}",
                "data.source": "${kafka.topic:equals('vertiflow.ingestion.raw'):ifElse('iot_simulator','http_api')}",
                "lineage_uuid": "${uuid()}"  # T9.4 : UUID de traçabilité
            }
        )

        # T4.3 : MergeContent pour fusionner les flux
        merge_props = {
            "Merge Strategy": "Bin-Packing Algorithm",
            "Merge Format": "Binary Concatenation",
            "Correlation Attribute Name": "data.source",
            "Minimum Number of Entries": "1",
            "Maximum Number of Entries": "100",
            "Max Bin Age": "10 sec",
            "Max Bin Size": "5 MB"
        }

        data_merger = await self.create_processor_instance(
            z1_id,
            "org.apache.nifi.processors.standard.MergeContent",
            "Z1_Data_Merger_V42",
            (400, 100),
            merge_props,
            ["failure"]
        )

        # Validation
        if self.services.get('json_reader') and self.services.get('json_writer'):
            validate_props = {
                "Record Reader": f"controller-service://{self.services['json_reader']}",
                "Record Writer": f"controller-service://{self.services['json_writer']}"
            }

            schema_validator = await self.create_processor_instance(
                z1_id,
                "org.apache.nifi.processors.standard.ValidateRecord",
                "Z1_Technical_Governance_Validator_V42",
                (600, 100),
                validate_props,
                ["invalid", "failure"]
            )
        else:
            schema_validator = None

        # T4.5 : OUTPUT PORT vers Zone 2
        output_port = await self.create_port(z1_id, "OUTPUT_PORT", "Z1_To_Z2_Validated_V42", (800, 100))
        self.ports['z1_output_validated'] = output_port

        # Connexions
        if input_port_z0 and data_merger:
            await self.establish_connection(z1_id, input_port_z0, data_merger, ["success"], "External_To_Merger")

        if kafka_consumer and drift_mapper:
            await self.establish_connection(z1_id, kafka_consumer, drift_mapper, ["success"], "Kafka_To_DriftMapper")

        if drift_mapper and data_merger:
            await self.establish_connection(z1_id, drift_mapper, data_merger, ["success"], "DriftMapper_To_Merger")

        if http_ingestor and data_merger:
            await self.establish_connection(z1_id, http_ingestor, data_merger, ["success"], "HTTP_To_Merger")

        if data_merger and schema_validator:
            await self.establish_connection(z1_id, data_merger, schema_validator, ["merged"], "Merger_To_Validator")

        if schema_validator and output_port:
            await self.establish_connection(z1_id, schema_validator, output_port, ["valid"], "Validator_To_Output")

        logger.info("✅ Zone 1 v4.2 opérationnelle avec détection de dérives")
        return z1_id

    # --- ZONE 2 : MOTEUR SCIENTIFIQUE VPD & INTELLIGENCE CONTEXTUELLE ---
    async def deploy_zone_2(self):
        """T5.1 à T6.5 : Moteur VPD et intelligence contextuelle"""
        logger.info("🔄 Déploiement Zone 2 - VPD Engine & Context Intelligence v4.2")

        z2_id = await self.create_standard_pg(self.master_pg_id, "Zone 2 - VPD Engine & Context Intelligence v4.2", 800, 0)
        if not z2_id:
            return None

        self.zones['z2'] = z2_id

        # INPUT PORT depuis Zone 1
        input_port_z1 = await self.create_port(z2_id, "INPUT_PORT", "Z2_From_Z1_Validated_V42", (-200, 0))
        self.ports['z2_input_validated'] = input_port_z1

        # T5.1 à T5.4 : Moteur Groovy pour calcul VPD
        groovy_vpd_logic = '''
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback

session.write(flowFile, { inputStream, outputStream ->
    try {
        def reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream, "UTF-8"))
        def text = reader.readLine()
        def content = new JsonSlurper().parseText(text)
        
        // T5.2 : Extraction et conversion des types
        double temp = (content.air_temp_internal instanceof Number) ? content.air_temp_internal : 
                     (content.air_temp_internal?.toString()?.isNumber() ? content.air_temp_internal.toString().toDouble() : 25.0)
        
        double hum = (content.air_humidity instanceof Number) ? content.air_humidity : 
                    (content.air_humidity?.toString()?.isNumber() ? content.air_humidity.toString().toDouble() : 60.0)
        
        // T5.3 : Calcul physique du VPD (formule de Tetens)
        // Pression de vapeur saturante (es) en kPa
        double es = 0.6108 * Math.exp((17.27 * temp) / (temp + 237.3))
        
        // Pression de vapeur actuelle (ea) en kPa
        double ea = es * (hum / 100.0)
        
        // Déficit de pression de vapeur (VPD) en kPa
        double vpd = es - ea
        
        // T5.4 : Injection du résultat calculé
        content.calculated_vpd = Math.round(vpd * 1000) / 1000.0
        content.calculated_vpd_kpa = content.calculated_vpd
        content.vpd_status = vpd < 0.4 ? "LOW" : (vpd > 1.6 ? "HIGH" : "OPTIMAL")
        
        // Remplace le 0.0 du simulateur par la valeur calculée
        if (content.vpd == 0.0) {
            content.vpd = content.calculated_vpd
        }
        
        def output = JsonOutput.toJson(content)
        outputStream.write(output.getBytes("UTF-8"))
    } catch (Exception e) {
        throw new RuntimeException("Erreur calcul VPD: " + e.getMessage(), e)
    }
} as StreamCallback)
'''

        vpd_calculator = await self.create_processor_instance(
            z2_id,
            "org.apache.nifi.processors.script.ExecuteScript",
            "Z2_VPD_Scientific_Calculator_V42",
            (0, 0),
            {
                "Script Engine": "Groovy",
                "Script Body": groovy_vpd_logic,
                "Module Directory": ""
            },
            ["failure"]
        )

        # T6.1 : LookupRecord pour récupérer les recettes botaniques
        if self.services.get('json_reader') and self.services.get('json_writer') and self.services.get('mongo_lookup'):
            lookup_props = {
                "Record Reader": f"controller-service://{self.services['json_reader']}",
                "Record Writer": f"controller-service://{self.services['json_writer']}",
                "Lookup Service": f"controller-service://{self.services['mongo_lookup']}",
                "Lookup Strategy": "MongoDB Lookup",
                "Result RecordPath": "/recipe_data"
            }

            recipe_lookup = await self.create_processor_instance(
                z2_id,
                "org.apache.nifi.processors.standard.LookupRecord",
                "Z2_Plant_Recipe_Lookup_V42",
                (200, 0),
                lookup_props,
                ["failure", "unmatched"]
            )
        else:
            recipe_lookup = None

        # T6.2 à T6.5 : UpdateRecord pour enrichissement contextuel
        update_record_logic = '''[
    {
        "operation": "default",
        "field": "/processing_timestamp",
        "value": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}"
    },
    {
        "operation": "copy",
        "field": "/ref_temp_opt",
        "value": "/recipe_data/optimal_temperature"
    },
    {
        "operation": "copy",
        "field": "/ref_ph_target",
        "value": "/recipe_data/target_ph"
    },
    {
        "operation": "copy",
        "field": "/ref_ec_target",
        "value": "/recipe_data/target_ec"
    },
    {
        "operation": "copy",
        "field": "/ref_light_hours",
        "value": "/recipe_data/light_hours"
    },
    {
        "operation": "modify",
        "field": "/temp_deviation",
        "value": "${air_temp_internal:toNumber() - ref_temp_opt:toNumber()}"
    },
    {
        "operation": "modify",
        "field": "/ph_deviation",
        "value": "${water_ph:toNumber() - ref_ph_target:toNumber()}"
    },
    {
        "operation": "modify",
        "field": "/health_score",
        "value": "${100 - (math:abs(temp_deviation) * 2) - (math:abs(ph_deviation) * 10) - (math:abs(calculated_vpd - 1.0) * 5)}"
    },
    {
        "operation": "modify",
        "field": "/plant_variety",
        "value": "${plant_type:equals('Ocimum basilicum'):ifElse('Genovese', plant_type)}"
    }
]'''

        context_enricher = await self.create_processor_instance(
            z2_id,
            "org.apache.nifi.processors.standard.UpdateRecord",
            "Z2_Context_Enricher_V42",
            (400, 0),
            {
                "Record Reader": f"controller-service://{self.services['json_reader']}",
                "Record Writer": f"controller-service://{self.services['json_writer']}",
                "Replacement Value Strategy": "Record Path Value",
                "/properties": update_record_logic
            },
            ["failure"]
        )

        # RouteOnAttribute pour router les données
        split_props = {
            "Routing Strategy": "Route to Property name",
            "data.type": "${vpd_status}",
            "has.anomaly": "${health_score:lt(80):or(${drift_type:isPresent()}):or(${water_ph:lt(5.5)}):or(${water_ph:gt(7.0)})}"
        }

        data_router = await self.create_processor_instance(
            z2_id,
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            "Z2_Data_Router_V42",
            (600, 0),
            split_props
        )

        # OUTPUT PORT vers Zone 3 (Persistance)
        output_port_z3 = await self.create_port(z2_id, "OUTPUT_PORT", "Z2_To_Z3_Persistence_V42", (800, -100))
        self.ports['z2_output_persistence'] = output_port_z3

        # OUTPUT PORT vers Zone 4 (Feedback)
        output_port_z4 = await self.create_port(z2_id, "OUTPUT_PORT", "Z2_To_Z4_Feedback_V42", (800, 100))
        self.ports['z2_output_feedback'] = output_port_z4

        # Connexions
        if input_port_z1 and vpd_calculator:
            await self.establish_connection(z2_id, input_port_z1, vpd_calculator, ["success"], "Input_To_VPD")

        if vpd_calculator and recipe_lookup:
            await self.establish_connection(z2_id, vpd_calculator, recipe_lookup, ["success"], "VPD_To_Lookup")

        if recipe_lookup and context_enricher:
            await self.establish_connection(z2_id, recipe_lookup, context_enricher, ["matched"], "Lookup_To_Enricher")
        elif vpd_calculator and context_enricher:
            await self.establish_connection(z2_id, vpd_calculator, context_enricher, ["success"], "VPD_To_Enricher")

        if context_enricher and data_router:
            await self.establish_connection(z2_id, context_enricher, data_router, ["success"], "Enricher_To_Router")

        if data_router and output_port_z3:
            await self.establish_connection(z2_id, data_router, output_port_z3, ["OPTIMAL", "LOW", "HIGH"], "All_To_Persistence")

        if data_router and output_port_z4:
            await self.establish_connection(z2_id, data_router, output_port_z4, ["has.anomaly"], "Anomalies_To_Feedback")

        logger.info("✅ Zone 2 v4.2 opérationnelle avec moteur VPD et intelligence")
        return z2_id

    # --- ZONE 3 : DOUBLE PERSISTANCE & DIGITAL TWIN ---
    async def deploy_zone_3(self):
        """T7.1 à T7.4 : Double persistance ClickHouse + MongoDB Digital Twin"""
        logger.info("💾 Déploiement Zone 3 - Double Persistance & Digital Twin v4.2")

        z3_id = await self.create_standard_pg(self.master_pg_id, "Zone 3 - Persistance & Digital Twin v4.2", 1600, 0)
        if not z3_id:
            return None

        self.zones['z3'] = z3_id

        # INPUT PORT depuis Zone 2
        input_port_z2 = await self.create_port(z3_id, "INPUT_PORT", "Z3_From_Z2_Persistence_V42", (-200, 0))
        self.ports['z3_input_persistence'] = input_port_z2

        # SplitJson pour séparer les données
        split_props = {
            "JsonPath Expression": "$[*]",
            "Keep Array Elements Together": "true"
        }

        json_splitter = await self.create_processor_instance(
            z3_id,
            "org.apache.nifi.processors.standard.SplitJson",
            "Z3_JSON_Splitter_V42",
            (0, 0),
            split_props,
            ["failure"]
        )

        # T7.1 : ClickHouse Writer pour historique complet
        if self.services.get('json_reader') and self.services.get('ck_pool'):
            clickhouse_props = {
                "Record Reader": f"controller-service://{self.services['json_reader']}",
                "Database Connection Pooling Service": f"controller-service://{self.services['ck_pool']}",
                "Statement Type": "INSERT",
                "Table Name": "vertiflow.basil_ultimate_realtime",
                "Field Names Included": "false",
                "Quote Column Identifiers": "false",
                "Translate Field Names": "false",
                "Unmatched Field Behaviour": "ignore",
                "Update Keys": ""
            }

            clickhouse_writer = await self.create_processor_instance(
                z3_id,
                "org.apache.nifi.processors.standard.PutDatabaseRecord",
                "Z3_ClickHouse_OLAP_Writer_V42",
                (200, -100),
                clickhouse_props,
                ["failure", "success"]
            )
        else:
            clickhouse_writer = None

        # T7.2 & T7.3 : MongoDB Digital Twin
        mongo_dt_props = {
            "Mongo URI": MONGO_URI,
            "Mongo Database Name": "vertiflow_ops",
            "Mongo Collection Name": "live_state",
            "Mode": "upsert",
            "Update Keys": "farm_id,rack_id,module_id",  # T7.3 : Clé d'idempotence
            "Update Query Key": "",
            "Upsert": "true"
        }

        mongo_dt_writer = await self.create_processor_instance(
            z3_id,
            "org.apache.nifi.processors.mongodb.PutMongo",
            "Z3_MongoDB_Digital_Twin_V42",
            (200, 100),
            mongo_dt_props,
            ["success", "failure"]
        )

        # T10.1 : Création de la vue SQL pour Power BI
        if clickhouse_writer:
            # Note: Cette étape serait normalement exécutée via une requête ClickHouse
            # Nous l'ajoutons comme étape logique
            logger.info("📊 Vue Power BI: view_pbi_operational_cockpit à créer dans ClickHouse")

        # Connexions
        if input_port_z2 and json_splitter:
            await self.establish_connection(z3_id, input_port_z2, json_splitter, ["success"], "Input_To_Splitter")

        # T7.4 : Flux parallèle vers ClickHouse et MongoDB
        if json_splitter and clickhouse_writer:
            await self.establish_connection(z3_id, json_splitter, clickhouse_writer, ["splits"], "Splitter_To_ClickHouse")

        if json_splitter and mongo_dt_writer:
            await self.establish_connection(z3_id, json_splitter, mongo_dt_writer, ["splits"], "Splitter_To_MongoDT")

        logger.info("✅ Zone 3 v4.2 opérationnelle avec Digital Twin")
        return z3_id

    # --- ZONE 4 : RÉTROACTION IA & ALERTES ---
    async def deploy_zone_4(self):
        """T8.1 à T8.5 : Rétroaction IA et système d'alertes"""
        logger.info("🔔 Déploiement Zone 4 - IA Feedback & Alert System v4.2")

        z4_id = await self.create_standard_pg(self.master_pg_id, "Zone 4 - IA Feedback & Alert System v4.2", 2400, 0)
        if not z4_id:
            return None

        self.zones['z4'] = z4_id

        # INPUT PORT depuis Zone 2
        input_port_z2 = await self.create_port(z4_id, "INPUT_PORT", "Z4_From_Z2_Feedback_V42", (-200, 0))
        self.ports['z4_input_feedback'] = input_port_z2

        # T8.1 : QueryRecord pour isoler les anomalies critiques
        query_expression = "SELECT * FROM FLOWFILE WHERE water_ph < 5.5 OR water_ph > 7.0 OR health_score < 80 OR drift_type IS NOT NULL"

        anomaly_filter = await self.create_processor_instance(
            z4_id,
            "org.apache.nifi.processors.standard.QueryRecord",
            "Z4_Anomaly_Filter_V42",
            (0, 0),
            {
                "Record Reader": f"controller-service://{self.services['json_reader']}",
                "Record Writer": f"controller-service://{self.services['json_writer']}",
                "Include Zero Record FlowFiles": "false",
                "Query": query_expression
            },
            ["failure", "original"]
        )

        # T8.2 & T8.3 : Formatage des alertes
        alert_formatter = await self.create_processor_instance(
            z4_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            "Z4_Alert_Formatter_V42",
            (200, 0),
            {
                "alert.severity": "${health_score:lt(70):or(${water_ph:lt(5.5)}):ifElse('CRITICAL','WARNING')}",
                "alert.timestamp": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
                "alert.message": "${'Anomalie détectée: '}${'PH=' + water_ph:toString() + ', '}${'VPD=' + vpd:toString() + ', '}${'Health=' + health_score:toString()}",
                "alert.action": "${water_ph:lt(5.5):ifElse('PH_UP', ${water_ph:gt(7.0):ifElse('PH_DOWN', ${vpd:lt(0.4):ifElse('HUMIDIFY', ${vpd:gt(1.6):ifElse('VENTILATE', 'MONITOR')})})})}",
                "alert.zone": "${farm_id + '_' + rack_id + '_' + module_id}",
                "alert.intensity": "${drift_intensity:defaultValue(1)}",
                # T8.5 : Corrélation avec drift_intensity
                "corrective.power": "${drift_intensity:defaultValue(1):multiply(10)}",
                "mqtt.topic": "vertiflow/actuators/control/${alert.severity:toLower()}"
            },
            ["failure"]
        )

        # T8.4 : Publication MQTT pour commandes d'action
        actuator_props = {
            "Broker URI": "tcp://localhost:1883",
            "Client ID": "nifi-actuator-v42-${now():format('yyyyMMdd')}",
            "Topic": "${mqtt.topic}",
            "Quality of Service(QoS)": "1",
            "Retain": "false",
            "Max Message Size": "1 MB",
            "Connection Timeout": "15 secs",
            "Message Body": "${alert.message} - Action: ${alert.action} - Zone: ${alert.zone} - Power: ${corrective.power}%"
        }

        actuator_publisher = await self.create_processor_instance(
            z4_id,
            "org.apache.nifi.processors.mqtt.PublishMQTT",
            "Z4_MQTT_Actuator_Commander_V42",
            (400, 0),
            actuator_props,
            ["failure", "success"]
        )

        # T9.1 & T9.2 : Archivage des incidents dans MongoDB
        mongo_incident_props = {
            "Mongo URI": MONGO_URI,
            "Mongo Database Name": "vertiflow_ops",
            "Mongo Collection Name": "incident_logs",
            "Mode": "insert",
            "Update Keys": "",
            "Update Query Key": ""
        }

        incident_logger = await self.create_processor_instance(
            z4_id,
            "org.apache.nifi.processors.mongodb.PutMongo",
            "Z4_Incident_Logger_V42",
            (200, 150),
            mongo_incident_props,
            ["success", "failure"]
        )

        # Connexions
        if input_port_z2 and anomaly_filter:
            await self.establish_connection(z4_id, input_port_z2, anomaly_filter, ["success"], "Input_To_Filter")

        if anomaly_filter and alert_formatter:
            await self.establish_connection(z4_id, anomaly_filter, alert_formatter, ["matched"], "Filter_To_Formatter")

        if alert_formatter and actuator_publisher:
            await self.establish_connection(z4_id, alert_formatter, actuator_publisher, ["success"], "Formatter_To_Actuator")

        if alert_formatter and incident_logger:
            await self.establish_connection(z4_id, alert_formatter, incident_logger, ["success"], "Formatter_To_Logger")

        logger.info("✅ Zone 4 v4.2 opérationnelle avec système d'alerte IA")
        return z4_id

    # --- ZONE 5 : DONNÉES STATIQUES ---
    async def deploy_zone_5(self):
        """Zone pour le chargement des données statiques"""
        logger.info("📂 Déploiement Zone 5 - Static Data Loaders v4.2")

        z5_id = await self.create_standard_pg(self.master_pg_id, "Zone 5 - Static Data Loaders v4.2", 3200, 0)
        if not z5_id:
            return None

        self.zones['z5'] = z5_id

        # GetFile pour fichiers CSV/JSON
        file_props = {
            "Input Directory": "/opt/nifi/nifi-current/exchange/input",
            "File Filter": "[^.]*",
            "Keep Source File": "false",
            "Minimum File Age": "0 sec",
            "Maximum File Age": "365 days",
            "Polling Interval": "60 sec",
            "Ignore Hidden Files": "true"
        }

        file_ingestor = await self.create_processor_instance(
            z5_id,
            "org.apache.nifi.processors.standard.GetFile",
            "Z5_File_Ingestor_V42",
            (0, 0),
            file_props
        )

        # RouteOnContent pour router par type
        route_props = {
            "Routing Strategy": "Route to Property name",
            "file.type": "${filename:endsWith('.json'):ifElse('json','csv'):ifElse(${filename:endsWith('.csv')},'unknown')}"
        }

        file_router = await self.create_processor_instance(
            z5_id,
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            "Z5_File_Router_V42",
            (150, 0),
            route_props
        )

        # ConvertRecord pour JSON vers MongoDB
        if self.services.get('json_reader'):
            json_converter = await self.create_processor_instance(
                z5_id,
                "org.apache.nifi.processors.standard.ConvertRecord",
                "Z5_JSON_To_Mongo_V42",
                (300, -50),
                {
                    "Record Reader": f"controller-service://{self.services['json_reader']}",
                    "Record Writer": "org.apache.nifi.mongodb.MongoDBWriter"
                },
                ["failure"]
            )
        else:
            json_converter = None

        # MongoDB Writer pour recettes
        mongo_writer_props = {
            "Mongo URI": MONGO_URI,
            "Mongo Database Name": "vertiflow_ops",
            "Mongo Collection Name": "plant_recipes_v42",
            "Mode": "insert",
            "Update Keys": "plant_type,recipe_name",
            "Update Query Key": ""
        }

        recipe_loader = await self.create_processor_instance(
            z5_id,
            "org.apache.nifi.processors.mongodb.PutMongo",
            "Z5_Plant_Recipe_Seeder_V42",
            (450, 0),
            mongo_writer_props,
            ["success", "failure"]
        )

        # Connexions
        if file_ingestor and file_router:
            await self.establish_connection(z5_id, file_ingestor, file_router, ["success"], "File_To_Router")

        if file_router and json_converter:
            await self.establish_connection(z5_id, file_router, json_converter, ["json"], "Router_JSON_To_Converter")

        if json_converter and recipe_loader:
            await self.establish_connection(z5_id, json_converter, recipe_loader, ["success"], "Converter_To_Mongo")

        logger.info("✅ Zone 5 v4.2 opérationnelle")
        return z5_id

    # --- ÉTABLISSEMENT DES LIAISONS INTER-ZONES ---
    async def establish_inter_zone_connections(self):
        """Établit toutes les connexions entre les zones"""
        logger.info("🌉 Établissement des liaisons inter-zones...")

        connections_established = 0

        # 1. Flux Externe : Zone 0 → Zone 1
        if self.zones.get('z0') and self.ports.get('z0_output') and \
                self.zones.get('z1') and self.ports.get('z1_input_external'):

            conn_id = await self.establish_inter_zone_connection(
                self.master_pg_id,
                self.zones['z0'],
                self.ports['z0_output'],
                self.zones['z1'],
                self.ports['z1_input_external'],
                "Flux_Externe_Z0_vers_Z1_V42"
            )
            if conn_id:
                connections_established += 1
                logger.info("  ✅ Connecté: Z0 → Z1")

        # 2. Flux Principal : Zone 1 → Zone 2
        if self.zones.get('z1') and self.ports.get('z1_output_validated') and \
                self.zones.get('z2') and self.ports.get('z2_input_validated'):

            conn_id = await self.establish_inter_zone_connection(
                self.master_pg_id,
                self.zones['z1'],
                self.ports['z1_output_validated'],
                self.zones['z2'],
                self.ports['z2_input_validated'],
                "Flux_Principal_Z1_vers_Z2_V42"
            )
            if conn_id:
                connections_established += 1
                logger.info("  ✅ Connecté: Z1 → Z2")

        # 3. Flux de Persistance : Zone 2 → Zone 3
        if self.zones.get('z2') and self.ports.get('z2_output_persistence') and \
                self.zones.get('z3') and self.ports.get('z3_input_persistence'):

            conn_id = await self.establish_inter_zone_connection(
                self.master_pg_id,
                self.zones['z2'],
                self.ports['z2_output_persistence'],
                self.zones['z3'],
                self.ports['z3_input_persistence'],
                "Flux_Persistance_Z2_vers_Z3_V42"
            )
            if conn_id:
                connections_established += 1
                logger.info("  ✅ Connecté: Z2 → Z3")

        # 4. Flux de Rétroaction : Zone 2 → Zone 4
        if self.zones.get('z2') and self.ports.get('z2_output_feedback') and \
                self.zones.get('z4') and self.ports.get('z4_input_feedback'):

            conn_id = await self.establish_inter_zone_connection(
                self.master_pg_id,
                self.zones['z2'],
                self.ports['z2_output_feedback'],
                self.zones['z4'],
                self.ports['z4_input_feedback'],
                "Flux_Retroaction_Z2_vers_Z4_V42"
            )
            if conn_id:
                connections_established += 1
                logger.info("  ✅ Connecté: Z2 → Z4")

        logger.info(f"📊 {connections_established}/4 liaisons inter-zones établies")
        return connections_established

    # --- DÉMARRAGE DU PIPELINE ---
    async def start_pipeline_robust(self):
        """Démarre le pipeline principal de manière robuste"""
        logger.info(f"🚀 Démarrage robuste du pipeline...")

        try:
            # Démarrer le conteneur maître
            start_payload = {
                "id": self.master_pg_id,
                "state": "RUNNING"
            }

            start_response = self.sm.session.put(
                f"{NIFI_BASE_URL}/flow/process-groups/{self.master_pg_id}",
                json=start_payload,
                timeout=30
            )

            if start_response.status_code == 200:
                logger.info("✅ Commande de démarrage envoyée")

                # Attendre et vérifier l'état
                await asyncio.sleep(10)

                final_response = self.sm.session.get(
                    f"{NIFI_BASE_URL}/flow/process-groups/{self.master_pg_id}",
                    timeout=15
                )

                if final_response.status_code == 200:
                    status_data = final_response.json()
                    aggregate_snapshot = status_data.get('processGroupFlow', {}).get('aggregateSnapshot', {})
                    running_count = aggregate_snapshot.get('runningCount', 0)
                    stopped_count = aggregate_snapshot.get('stoppedCount', 0)

                    logger.info(f"📊 État final: {running_count} actifs, {stopped_count} arrêtés")

                    if running_count > 30:  # T10.4 : Audit final
                        logger.info("🎉 Pipeline VertiFlow v4.2 ACTIF! (>30 composants)")
                        return True
                    else:
                        logger.warning(f"⚠️ Seulement {running_count} composants actifs")
                        return False
                else:
                    logger.warning("⚠️ Impossible de vérifier l'état final")
                    return True
            else:
                logger.error(f"❌ Échec démarrage pipeline: {start_response.status_code}")
                return False

        except Exception as e:
            logger.error(f"❌ Erreur démarrage pipeline: {e}")
            return False

    # --- AUDIT FINAL ---
    async def verify_pipeline_health_v42(self):
        """T10.4 & T10.5 : Audit final de la structure déployée"""
        print("\n" + "═"*80)
        logger.info("⭐ RAPPORT DE GOUVERNANCE TECHNIQUE VERTIFLOW v4.2 ⭐")
        print("═"*80)

        services_count = len([v for v in self.services.values() if v])
        zones_count = len([v for v in self.zones.values() if v])
        ports_count = len([v for v in self.ports.values() if v])
        connections_count = len(self.connections)

        summary = {
            "Infrastructure Core": "✅ CONNECTÉ",
            "Controller Services": f"✅ {services_count} Services Déployés",
            "Zones de Données": f"✅ {zones_count} Unités Déployées",
            "Ports d'Interface": f"✅ {ports_count} Ports Créés",
            "Connexions Inter-Zones": f"✅ {connections_count} Connexions Établies",
            "Architecture": "✅ Pipeline Gold Standard v4.2"
        }

        for k, v in summary.items():
            logger.info(f"{k:<35} : {v}")

        # Diagramme du flux
        logger.info("\n📊 DIAGRAMME DU FLUX DE DONNÉES v4.2:")
        logger.info("   NASA/Météo APIs → Zone 0 → Zone 1 (Drift Detection)")
        logger.info("   Kafka IoT Simulator → Zone 1 → Zone 2 (VPD Engine)")
        logger.info("   Zone 2 (Context Intel) → Zone 3 (ClickHouse + MongoDB DT)")
        logger.info("   Zone 2 (Anomalies) → Zone 4 (IA Feedback → MQTT Actuators)")
        logger.info("   Zone 5 (Static Data) → MongoDB Recipes")

        # État des services critiques
        logger.info("\n🔍 ÉTAT DES SERVICES CRITIQUES:")
        critical_services = [
            ('SSL Context', self.services.get('ssl_context')),
            ('HTTP Context', self.services.get('http_context')),
            ('ClickHouse Pool', self.services.get('ck_pool')),
            ('MongoDB Client', self.services.get('mongo_svc')),
            ('JSON Reader', self.services.get('json_reader')),
            ('JSON Writer', self.services.get('json_writer')),
            ('Recipe Lookup', self.services.get('mongo_lookup'))
        ]

        for svc_name, svc_id in critical_services:
            if svc_id:
                state = self.integrity.get_component_state(svc_id)
                status = "✅" if state == "ENABLED" else "❌"
                logger.info(f"   {status} {svc_name:<20}: {state if state else 'N/A'}")

        print("═"*80)
        logger.info("🚀 SYSTÈME VERTIFLOW v4.2 CONFIGURÉ - PRÊT POUR L'ACTIVATION")
        print("═"*80 + "\n")

        # T10.5 : Confirmation du flux
        logger.info("📡 Flux de données configuré:")
        logger.info("   NASA/Météo APIs → Kafka → ClickHouse/MongoDB Digital Twin")
        logger.info("   IoT Simulator → VPD Calculator → IA Feedback → MQTT Actuators")

    # --- ORCHESTRATEUR SUPRÊME ---
    async def execute_full_deployment_v42(self):
        """Orchestrateur Suprême : Déploiement complet v4.2"""
        start_time = time.time()

        try:
            # PHASE 1 : INITIALISATION
            logger.info("="*60)
            logger.info("PHASE 1 : INITIALISATION DU CORE v4.2")
            logger.info("="*60)
            if not await self.initialize_core():
                return False

            await self.cleanup_system()

            # PHASE 2 : CONTENEUR MAÎTRE
            logger.info("\n" + "="*60)
            logger.info("PHASE 2 : CONTENEUR MAÎTRE VERTIFLOW_GOLD_V42")
            logger.info("="*60)
            if not await self.create_master_container():
                return False

            # PHASE 3 : SERVICES DE CONTRÔLE
            logger.info("\n" + "="*60)
            logger.info("PHASE 3 : SERVICES DE CONTRÔLE v4.2 (Séquençage Bloquant)")
            logger.info("="*60)
            await self.configure_master_services()
            await asyncio.sleep(10)

            # PHASE 4 : DÉPLOIEMENT DES ZONES
            logger.info("\n" + "="*60)
            logger.info("PHASE 4 : DÉPLOIEMENT DES ZONES v4.2")
            logger.info("="*60)

            zones_to_deploy = [
                ("Zone 0 - APIs Externes", self.deploy_zone_0()),
                ("Zone 1 - Drift Detection", self.deploy_zone_1()),
                ("Zone 2 - VPD Engine", self.deploy_zone_2()),
                ("Zone 3 - Digital Twin", self.deploy_zone_3()),
                ("Zone 4 - IA Feedback", self.deploy_zone_4()),
                ("Zone 5 - Static Data", self.deploy_zone_5())
            ]

            for zone_name, task in zones_to_deploy:
                logger.info(f"\n📍 {zone_name}...")
                try:
                    await task
                    await asyncio.sleep(3)
                except Exception as e:
                    logger.error(f"❌ Erreur dans {zone_name}: {e}")

            # PHASE 5 : LIAISONS INTER-ZONES
            logger.info("\n" + "="*60)
            logger.info("PHASE 5 : LIAISONS INTER-ZONES")
            logger.info("="*60)
            connections = await self.establish_inter_zone_connections()

            if connections < 4:
                logger.warning(f"⚠️ {connections}/4 liaisons établies")
            else:
                logger.info(f"✅ {connections}/4 liaisons inter-zones opérationnelles")

            # PHASE 6 : AUDIT FINAL
            logger.info("\n" + "="*60)
            logger.info("PHASE 6 : AUDIT FINAL v4.2")
            logger.info("="*60)
            await self.verify_pipeline_health_v42()

            # PHASE 7 : DÉMARRAGE
            logger.info("\n" + "="*60)
            logger.info("PHASE 7 : DÉMARRAGE DU PIPELINE")
            logger.info("="*60)
            pipeline_active = await self.start_pipeline_robust()

            elapsed = time.time() - start_time
            logger.info(f"⏱️ Déploiement complet v4.2 exécuté en {elapsed:.2f} secondes")

            # Rapport final
            if pipeline_active:
                logger.info("\n" + "🎉" * 40)
                logger.info("🎉 DÉPLOIEMENT VERTIFLOW v4.2 RÉUSSI!")
                logger.info("🎉" * 40)
            else:
                logger.info("\n" + "⚠️" * 40)
                logger.info("⚠️ DÉPLOIEMENT VERTIFLOW v4.2 PARTIEL")
                logger.info("⚠️" * 40)

            return pipeline_active

        except Exception as e:
            logger.critical(f"🚨 ERREUR CRITIQUE DU DÉPLOIEMENT v4.2: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

# --- POINT D'ENTRÉE ---
if __name__ == "__main__":
    print("\n" + "="*80)
    print("🚀 VERTIFLOW GOVERNANCE MASTER - VERSION 4.2.0 (GOLD STANDARD)")
    print("="*80)
    print(f"📅 Démarrage: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

    # Configuration du logging vers fichier
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler("logs/governance_v42_deploy.log")
    file_handler.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s] - [VERTIFLOW_V4.2] - %(message)s'))
    logger.addHandler(file_handler)

    master = VertiFlowGovernanceMasterV42()

    try:
        success = asyncio.run(master.execute_full_deployment_v42())

        if success:
            print("\n" + "✅" * 40)
            print("✅ DÉPLOIEMENT VERTIFLOW v4.2 RÉUSSI - SYSTÈME ACTIF")
            print("✅" * 40)
            print("\n🔗 Accès NiFi: https://localhost:8443/nifi")
            print("👤 Identifiants: admin / ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB")
            print("📊 Données: basil_ultimate_realtime -> ClickHouse")
            print("🤖 Digital Twin: live_state -> MongoDB")
            print("🎯 VPD: Calculé en temps réel (remplace le 0.0 du simulateur)")
            print("🔔 Alertes: MQTT sur vertiflow/actuators/control")
            print("📈 Power BI: view_pbi_operational_cockpit dans ClickHouse")
            print("\n📋 Prochaines étapes:")
            print("1. Lancer le simulateur IoT: python3 vertiflow_simulator_v42.py")
            print("2. Surveiller les logs: tail -f logs/governance_v42_deploy.log")
            print("3. Vérifier le flux dans NiFi UI")
            print("4. Consulter les données dans ClickHouse et MongoDB")
        else:
            print("\n" + "⚠️" * 40)
            print("⚠️ DÉPLOIEMENT VERTIFLOW v4.2 PARTIEL")
            print("⚠️" * 40)
            print("\n📄 Consultez les logs: logs/governance_v42_deploy.log")
            print("🔧 Actions manuelles possibles:")
            print("1. Connectez-vous à NiFi (https://localhost:8443/nifi)")
            print("2. Activez manuellement les services controller")
            print("3. Vérifiez les connexions inter-zones")
            print("4. Démarrez manuellement 'VERTIFLOW_GOLD_V42'")

    except KeyboardInterrupt:
        logger.info("🛑 Déploiement v4.2 interrompu par l'utilisateur.")
        print("\n🛑 Déploiement v4.2 interrompu.")
    except Exception as e:
        logger.critical(f"🚨 ERREUR FATALE v4.2: {str(e)}")
        print(f"\n❌ ERREUR FATALE v4.2: {str(e)}")
        sys.exit(1)