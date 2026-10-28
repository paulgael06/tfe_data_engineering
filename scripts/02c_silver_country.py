"""
SILVER - Pays.
Source : TFE_Bronze.dim_country (script 01b, API RestCountries)
Cible  : TFE_Silver.country (table lue par le chargement du Gold -> Dim_country)

Transformations :
  - identifiant ID (1..n) ;
  - code ISO alpha-2 en majuscules, espaces retirés ;
  - code vide ou absent -> "XX".
"""
import pandas as pd
from sqlalchemy import create_engine
from db_config import build_connection_url

db_bronze = create_engine(
    build_connection_url("TFE_Bronze")
)
db__silver = create_engine(
    build_connection_url("TFE_Silver")
)

df_country = pd.read_sql("SELECT * FROM dim_country", con=db_bronze)
print("Pays lus dans le Bronze :", len(df_country))

df_country.insert(0, "ID", range(1, len(df_country) + 1))

# astype("string") conserve les valeurs manquantes (astype(str) les transformerait en "nan")
iso_code = df_country["iso_alpha2_clean"].astype("string").str.strip().str.upper()
iso_code = iso_code.replace("", pd.NA).fillna("XX")
df_country["iso_alpha2_clean"] = iso_code.astype(object)

print("Codes ISO remplacés par XX :", int((df_country["iso_alpha2_clean"] == "XX").sum()))
print("Codes ISO distincts :", df_country["iso_alpha2_clean"].nunique(), "sur", len(df_country), "lignes")

df_country.to_sql("country", con=db__silver, if_exists="replace", index=False)
print("Table TFE_Silver.country écrite :", len(df_country), "lignes")
