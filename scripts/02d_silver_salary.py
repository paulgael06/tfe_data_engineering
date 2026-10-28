"""
SILVER - Salaires Kaggle.
Source : TFE_Bronze.salaries (script 01d)
Cible  : TFE_Silver.salary (table lue par le chargement du Gold -> Fact_salary)

Transformations :
  - suppression des lignes entièrement identiques (151 000 -> 71 913 environ) ;
  - identifiant ID (1..n) ;
  - company_location et employee_residence : espaces retirés, majuscules.
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

df_salary = pd.read_sql("SELECT * FROM salaries", con=db_bronze)
rows_before = len(df_salary)

df_salary = df_salary.drop_duplicates()
print("Lignes avant / après suppression des doublons :", rows_before, "/", len(df_salary))

df_salary.insert(0, "ID", range(1, len(df_salary) + 1))
df_salary["company_location"] = df_salary["company_location"].str.strip().str.upper()
df_salary["employee_residence"] = df_salary["employee_residence"].str.strip().str.upper()

print(df_salary.info())
df_salary.to_sql("salary", con=db__silver, if_exists="replace", index=False)
print("Table TFE_Silver.salary écrite :", len(df_salary), "lignes")
