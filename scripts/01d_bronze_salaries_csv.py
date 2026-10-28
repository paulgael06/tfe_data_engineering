"""
BRONZE - Salaires Kaggle (fichier plat CSV).
Source : salaries.csv (téléchargé manuellement sur Kaggle, séparateur ";")
Cible  : TFE_Bronze.salaries (copie brute, aucune transformation)

Le fichier est cherché dans le sous-dossier "data" à côté de ce script :
    TFE_scripts\\data\\salaries.csv
Un autre chemin peut être donné en argument :
    python 01d_bronze_salaries_csv.py "E:\\autre\\dossier\\salaries.csv"

Chargement unique : le fichier est fixe, il n'est pas relancé dans le pipeline
quotidien. Un nouveau chargement remplace la table (if_exists="replace").
"""
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from db_config import build_connection_url

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CSV_PATH = SCRIPT_DIR / "data" / "salaries.csv"

# Colonnes dont le Silver et le Gold ont besoin
REQUIRED_COLUMNS = [
    "work_year", "experience_level", "employment_type", "job_title", "salary",
    "salary_currency", "salary_in_usd", "employee_residence", "remote_ratio",
    "company_location", "company_size",
]

csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV_PATH
if not csv_path.exists():
    raise SystemExit(
        "Fichier introuvable : " + str(csv_path)
        + "\nCopie salaries.csv dans le dossier 'data' à côté de ce script,"
        " ou donne son chemin en argument."
    )

df_salaries = pd.read_csv(csv_path, sep=";")
print("Fichier lu :", csv_path)
print("Lignes :", len(df_salaries), "| colonnes :", list(df_salaries.columns))

missing_columns = [column for column in REQUIRED_COLUMNS if column not in df_salaries.columns]
if missing_columns:
    raise SystemExit("Colonnes manquantes dans le CSV : " + str(missing_columns)
                     + " (séparateur attendu : ';')")

db_bronze = create_engine(
    build_connection_url("TFE_Bronze")
)
df_salaries.to_sql("salaries", con=db_bronze, if_exists="replace", index=False)
print("Table TFE_Bronze.salaries écrite :", len(df_salaries), "lignes")
