"""
BRONZE - Offres d'emploi freehire (appel d'API REST).
Source : https://freehire.me/api/v1/jobs/search (pagination limit / offset, un pays à la fois)
Cible  : TFE_Bronze.jobs_freehire

Chargement INCRÉMENTAL : seules les offres absentes de la table sont insérées
(clés : public_slug et external_id). Les offres déjà présentes ne sont jamais
modifiées : le Bronze n'est ni mis à jour ni supprimé, il ne fait qu'ajouter.

Les colonnes de l'API sont gardées telles quelles. Les listes et dictionnaires
sont stockés en texte JSON. Si l'API renvoie une colonne absente de la table,
elle est ignorée (avec un avertissement) pour ne pas casser le schéma existant.
"""
import json
import time

import pandas as pd
import requests
from sqlalchemy import create_engine
from db_config import build_connection_url

API_URL = "https://freehire.me/api/v1/jobs/search"
COUNTRIES = ["fr", "us", "gb", "de", "be", "nl", "es", "it", "ca", "ch",
             "ie", "se", "pt", "pl", "au", "in", "sg", "lu", "at", "dk"]
CATEGORIES = "data_engineering,data_analytics,data_science"
PAGE_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 60
MAX_ATTEMPTS = 5          # tentatives par page en cas de coupure réseau
RETRY_WAIT_SECONDS = 10   # attente de base, multipliée à chaque nouvel essai
MAX_DEPTH_ERROR = "pagination too deep"  # message de l'API au-delà de 10 000 résultats

TABLE_NAME = "jobs_freehire"
CONNECTION_STRING = (
    build_connection_url("TFE_Bronze")
)


def to_json_text(value):
    """Liste ou dictionnaire -> texte JSON ; les autres valeurs restent inchangées."""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def get_page(params):
    """Appelle l'API avec plusieurs tentatives si la connexion est coupée."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as error:
            if attempt == MAX_ATTEMPTS:
                raise
            wait_seconds = RETRY_WAIT_SECONDS * attempt
            print("Connexion coupée (essai", attempt, "/", MAX_ATTEMPTS, ") : nouvel essai dans",
                  wait_seconds, "s -", type(error).__name__)
            time.sleep(wait_seconds)


def collect_offers():
    """Appelle l'API pour chaque pays et renvoie la liste de toutes les offres."""
    all_offers = []
    for country_code in COUNTRIES:
        offset = 0  # réinitialisé pour chaque pays
        while True:
            params = {
                "category": CATEGORIES,
                "countries": country_code,
                "limit": PAGE_SIZE,
                "offset": offset,
            }
            response = get_page(params)
            if response.status_code == 400 and MAX_DEPTH_ERROR in response.text:
                # L'API refuse d'aller au-delà de 10 000 résultats par recherche :
                # on garde ce qui est collecté pour ce pays et on passe au suivant.
                print("Attention :", country_code, "limité à", offset,
                      "offres par l'API (pagination trop profonde)")
                break
            if response.status_code != 200:
                raise RuntimeError(
                    "Erreur API pour " + country_code + " (offset " + str(offset) + ") : "
                    + str(response.status_code) + " " + response.text[:200]
                )
            data = response.json()
            all_offers.extend(data["data"])
            total = data["meta"]["total"]
            if offset % 1000 == 0:
                print(country_code, "-- offset", offset, "/ total", total, "--",
                      len(all_offers), "offres collectées au total")
            offset += PAGE_SIZE
            if offset >= total:
                break
    return all_offers


all_offers = collect_offers()

df = pd.json_normalize(all_offers)
print("Offres collectées :", len(df))
df = df.drop_duplicates(subset="public_slug")  # une offre remote peut apparaître dans plusieurs pays
print("Offres uniques (public_slug) :", len(df))

for column in df.columns:
    df[column] = df[column].apply(to_json_text)

source_engine = create_engine(CONNECTION_STRING)

# Offres déjà dans le Bronze
try:
    existing_keys = pd.read_sql(
        "SELECT public_slug, external_id FROM " + TABLE_NAME, con=source_engine
    )
    table_columns = list(pd.read_sql("SELECT TOP 0 * FROM " + TABLE_NAME, con=source_engine).columns)
    table_exists = True
except Exception:
    existing_keys = pd.DataFrame({"public_slug": [], "external_id": []})
    table_columns = list(df.columns)
    table_exists = False
print("Offres déjà dans le Bronze :", len(existing_keys))

# Comparaison sans tenir compte de la casse : l'API écrit parfois le même
# identifiant de deux façons (AccorCorp:744... et accorcorp:744...).
df_key = df["external_id"].astype("string").str.strip().str.lower()
existing_key = existing_keys["external_id"].astype("string").str.strip().str.lower()
is_known = df["public_slug"].isin(existing_keys["public_slug"]) | df_key.isin(existing_key)
new_offers = df[~is_known].copy()

# Aligne les colonnes sur la table existante (ordre et noms identiques)
ignored_columns = [column for column in new_offers.columns if column not in table_columns]
if ignored_columns:
    print("Attention : colonnes de l'API absentes de la table, ignorées :", ignored_columns)
absent_columns = [column for column in table_columns if column not in new_offers.columns]
if absent_columns:
    print("Attention : colonnes de la table absentes de l'API, laissées vides :", absent_columns)
new_offers = new_offers.reindex(columns=table_columns)

if len(new_offers) == 0:
    print("Aucune nouvelle offre : le Bronze est déjà à jour.")
else:
    new_offers.to_sql(TABLE_NAME, con=source_engine, if_exists="append", index=False)

print(len(new_offers), "nouvelles offres insérées sur", len(df), "collectées au total.")
