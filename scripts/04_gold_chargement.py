"""
GOLD_TFE - ÉTAPE 2 : CHARGEMENT DES DIMENSIONS PUIS DES FAITS
Source  : base TFE_Silver (jobs_freehire, skills, bridge, country, salary)
Cible   : base TFE_Gold (tables créées par gold_01_creation_tables.sql)

Ordre de chargement (imposé par les clés étrangères) :
  Dim_date, Dim_country, Junk_dim, Dim_company, dim_job, skills,
  Fact_job, Fact_salary, bridge

Règles :
  - valeurs manquantes des colonnes texte des dimensions -> "ND"
  - séniorité Kaggle ramenée au vocabulaire freehire :
      EN -> junior, MI -> middle, SE -> senior, EX -> c_level
  - pays de Fact_salary = employee_residence ; code absent de Dim_country -> "ND"
  - date de Fact_salary = work_year * 100 (AAAA00, mois inconnu)
  - bridge : une ligne par (ligne de Fact_job, compétence)
"""
import pandas as pd
from sqlalchemy import create_engine, text
from db_config import build_connection_url

# =========================================================================
# 0. CONNEXIONS
# =========================================================================
db_silver = create_engine(
    build_connection_url("TFE_Silver")
)
db_gold = create_engine(
    build_connection_url("TFE_Gold"),
    fast_executemany=True,
)
# Moteur sans fast_executemany pour dim_job (colonne description NVARCHAR(MAX))
db_gold_slow = create_engine(
    build_connection_url("TFE_Gold")
)

ND = "ND"
GOLD_TABLES = [
    "Dim_date", "Dim_country", "Junk_dim", "Dim_company", "dim_job",
    "skills", "Fact_job", "Fact_salary", "bridge",
]
KAGGLE_SENIORITY_MAPPING = {
    "EN": "junior",
    "MI": "middle",
    "SE": "senior",
    "EX": "c_level",
}


# =========================================================================
# FONCTIONS UTILITAIRES
# =========================================================================
def clean_text(series, max_length):
    """Texte propre : espaces retirés, vide / nan / None / NULL -> ND,
    tronqué à la longueur de la colonne SQL (avec avertissement)."""
    cleaned_text = series.astype("string").str.strip()
    cleaned_text = cleaned_text.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NULL": pd.NA})
    cleaned_text = cleaned_text.fillna(ND)
    too_long_count = int((cleaned_text.str.len() > max_length).sum())
    if too_long_count > 0:
        print("  Attention :", too_long_count, "valeurs tronquées à", max_length)
    return cleaned_text.str.slice(0, max_length).astype(object)


def truncate_text(series, max_length):
    """Texte nullable tronqué (NULL conservé)."""
    cleaned_text = series.astype("string").str.slice(0, max_length)
    return cleaned_text.astype(object).where(cleaned_text.notna(), None)


def validate_country(series, table_name, known_codes):
    """Code pays en majuscules ; absent de Dim_country -> ND."""
    code = series.astype("string").str.strip().str.upper().fillna(ND)
    unknown_codes = ~code.isin(known_codes)
    unknown_list = sorted(code[unknown_codes].unique())
    print(" ", table_name, ": codes pays inconnus remplacés par ND :",
          int(unknown_codes.sum()), unknown_list[:20])
    return code.where(~unknown_codes, ND).astype(object)


def load_table(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists="append", index=False, chunksize=5000)
    print("  ", table_name, ": lignes chargées :", len(df))


run_start = pd.Timestamp.now().floor("s")

# =========================================================================
# 1. LECTURE DU SILVER
# =========================================================================
print("--- 1. Lecture du Silver ---")
df_job = pd.read_sql("SELECT * FROM jobs_freehire", con=db_silver)
df_skills_silver = pd.read_sql("SELECT * FROM skills", con=db_silver)
df_bridge_silver = pd.read_sql("SELECT * FROM bridge", con=db_silver)
df_country_silver = pd.read_sql("SELECT * FROM country", con=db_silver)
df_salary = pd.read_sql("SELECT * FROM salary", con=db_silver)
print("jobs_freehire :", len(df_job), "lignes |", df_job["ID"].nunique(), "offres")
print("salary        :", len(df_salary), "lignes")

# =========================================================================
# 2. LES TABLES DU GOLD DOIVENT ÊTRE VIDES
# =========================================================================
print("\n--- 2. Vérification : tables Gold vides ---")
with db_gold.connect() as connection:
    for table_name in GOLD_TABLES:
        nb = connection.execute(text("SELECT COUNT(*) FROM " + table_name)).scalar()
        if nb > 0:
            raise RuntimeError(
                table_name + " contient déjà " + str(nb) + " lignes : "
                "relance d'abord gold_01_creation_tables.sql"
            )
print("OK : toutes les tables Gold sont vides.")

if "silver_loaded_at" not in df_job.columns:
    raise RuntimeError("silver_loaded_at absent : relance d'abord 02b_silver_jobs_freehire.py full")
if df_job["year_month"].isna().sum() > 0:
    raise RuntimeError("year_month contient des valeurs manquantes")

# =========================================================================
# 3. Dim_date
# =========================================================================
print("\n--- 3. Dim_date ---")
freehire_dates = df_job["year_month"].astype("int64")
kaggle_dates = df_salary["work_year"].astype("int64") * 100
date_ids = pd.concat([freehire_dates, kaggle_dates]).drop_duplicates().sort_values()
df_dim_date = pd.DataFrame({"date_id": date_ids.to_numpy()})
df_dim_date["year"] = df_dim_date["date_id"] // 100
df_dim_date["month"] = df_dim_date["date_id"] % 100
load_table(df_dim_date, "Dim_date", db_gold)

# =========================================================================
# 4. Dim_country (+ membre ND)
# =========================================================================
print("\n--- 4. Dim_country ---")
df_dim_country = pd.DataFrame(
    {
        "country_code": df_country_silver["iso_alpha2_clean"]
        .astype("string").str.strip().str.upper(),
        "country_name": clean_text(df_country_silver["names.common"], 200),
        "region": clean_text(df_country_silver["region"], 100),
        "sub_region": clean_text(df_country_silver["subregion"], 100),
    }
)
df_dim_country = df_dim_country.dropna(subset=["country_code"])
df_dim_country = df_dim_country.drop_duplicates(subset=["country_code"])
df_nd_member = pd.DataFrame(
    [{"country_code": ND, "country_name": "Non déterminé",
      "region": ND, "sub_region": ND}]
)
df_dim_country = pd.concat([df_dim_country, df_nd_member], ignore_index=True)
df_dim_country["country_code"] = df_dim_country["country_code"].astype(object)
load_table(df_dim_country, "Dim_country", db_gold)
known_codes = set(df_dim_country["country_code"])

# =========================================================================
# 5. Junk_dim (séniorité commune aux deux sources)
# =========================================================================
print("\n--- 5. Junk_dim ---")
freehire_seniority = clean_text(df_job["seniority_final"], 20)
freehire_seniority = freehire_seniority.where(
    freehire_seniority == ND, freehire_seniority.str.lower()
)
df_job["seniority"] = freehire_seniority
df_job["job_category"] = clean_text(df_job["enrichment.category"], 50)
df_job["education_lvl"] = clean_text(df_job["education_final"], 50)

kaggle_level = df_salary["experience_level"].astype("string").str.strip().str.upper()
df_salary["seniority"] = kaggle_level.map(KAGGLE_SENIORITY_MAPPING).fillna(ND)
df_salary["job_category"] = ND
df_salary["education_lvl"] = ND

print("  Séniorité freehire :", sorted(df_job["seniority"].unique()))
print("  Séniorité Kaggle   :", sorted(df_salary["seniority"].unique()))

JUNK_COLUMNS = ["seniority", "job_category", "education_lvl"]
df_dim_junk = pd.concat(
    [df_job[JUNK_COLUMNS], df_salary[JUNK_COLUMNS]], ignore_index=True
).drop_duplicates()
load_table(df_dim_junk, "Junk_dim", db_gold)
df_junk_db = pd.read_sql(
    "SELECT Id_Junk_dim, seniority, job_category, education_lvl FROM Junk_dim",
    con=db_gold,
)
# Noms de colonnes fixés (SQL Server peut renvoyer une autre casse)
df_junk_db.columns = ["Id_Junk_dim", "seniority", "job_category", "education_lvl"]

# =========================================================================
# 6. Dim_company
# =========================================================================
print("\n--- 6. Dim_company ---")
df_job["company_name"] = clean_text(df_job["company"], 300)
df_job["company_location"] = clean_text(df_job["location"], 300)
df_job["company_size"] = clean_text(df_job["enrichment.company_size"], 50)
df_job["domain"] = clean_text(df_job["enrichment.domains"], 500)
COMPANY_COLUMNS = ["company_name", "company_location", "company_size", "domain"]
df_dim_company = df_job[COMPANY_COLUMNS].drop_duplicates().reset_index(drop=True)
load_table(df_dim_company, "Dim_company", db_gold)
df_company_db = pd.read_sql(
    "SELECT company_id, company_name, company_location, company_size, domain "
    "FROM Dim_company",
    con=db_gold,
)
df_company_db.columns = [
    "company_id", "company_name", "company_location", "company_size", "domain"
]

# =========================================================================
# 7. dim_job (une ligne par offre) et skills
# =========================================================================
print("\n--- 7. dim_job et skills ---")
df_offers = df_job.drop_duplicates(subset=["ID"])
df_dim_job = pd.DataFrame(
    {
        "Id_dim_job": df_offers["ID"],
        "title": truncate_text(df_offers["title"], 500),
        "url": truncate_text(df_offers["url"], 2000),
        "source": truncate_text(df_offers["source"], 100),
        "description": df_offers["description"],
    }
)
load_table(df_dim_job, "dim_job", db_gold_slow)

df_skills_gold = pd.DataFrame(
    {
        "Id_skills": df_skills_silver["Id_skills"],
        "skills_name": truncate_text(df_skills_silver["skill_name"], 200),
        "skill_type": df_skills_silver["skill_type"],
    }
)
load_table(df_skills_gold, "skills", db_gold)

# =========================================================================
# 8. Fact_job (grain : une ligne par offre x pays)
# =========================================================================
print("\n--- 8. Fact_job ---")
df_fact_job = pd.DataFrame(
    {
        "job_id": df_job["Id_job_country"],
        "external_id": truncate_text(df_job["external_id"], 200),
        "public_slug": truncate_text(df_job["public_slug"], 500),
        "Id_dim_job": df_job["ID"],
        "date_id": df_job["year_month"].astype("int64"),
        "country_code": validate_country(df_job["country_code"], "Fact_job", known_codes),
        "salary_min_annual": df_job["salary_min_annual"],
        "salary_max_annual": df_job["salary_max_annual"],
        "salary_ref_annual": df_job["salary_ref_annual"],
        "salary_in_usd": df_job["salary_in_usd"],
        "salary_currency": clean_text(df_job["enrichment.salary_currency"], 10),
        "load_date": run_start,
        "seniority": df_job["seniority"],
        "job_category": df_job["job_category"],
        "education_lvl": df_job["education_lvl"],
        "company_name": df_job["company_name"],
        "company_location": df_job["company_location"],
        "company_size": df_job["company_size"],
        "domain": df_job["domain"],
    }
)
df_fact_job = pd.merge(df_fact_job, df_company_db, on=COMPANY_COLUMNS, how="left")
df_fact_job = pd.merge(df_fact_job, df_junk_db, on=JUNK_COLUMNS, how="left")
if df_fact_job["company_id"].isna().any() or df_fact_job["Id_Junk_dim"].isna().any():
    raise RuntimeError("Jointure Fact_job : clé company_id ou Id_Junk_dim manquante")

before = len(df_fact_job)
df_fact_job = df_fact_job.drop_duplicates(subset=["Id_dim_job", "country_code"])
print("  Lignes supprimées (doublon offre + pays) :", before - len(df_fact_job))

FACT_JOB_COLUMNS = [
    "job_id", "external_id", "public_slug", "Id_dim_job", "date_id",
    "company_id", "country_code", "Id_Junk_dim", "salary_min_annual",
    "salary_max_annual", "salary_ref_annual", "salary_in_usd",
    "salary_currency", "load_date",
]
df_fact_job = df_fact_job[FACT_JOB_COLUMNS]
if not df_fact_job["job_id"].is_unique:
    raise RuntimeError("Fact_job.job_id n'est pas unique")
load_table(df_fact_job, "Fact_job", db_gold)

# =========================================================================
# 9. Fact_salary (Kaggle)
# =========================================================================
print("\n--- 9. Fact_salary ---")
df_salary["country_code"] = validate_country(
    df_salary["employee_residence"], "Fact_salary", known_codes
)
df_fact_salary = pd.DataFrame(
    {
        "salary_id": df_salary["ID"],
        "date_id": df_salary["work_year"].astype("int64") * 100,
        "country_code": df_salary["country_code"],
        "seniority": df_salary["seniority"],
        "job_category": df_salary["job_category"],
        "education_lvl": df_salary["education_lvl"],
        "salary": df_salary["salary"].round().astype("int64"),
        "salary_in_usd": pd.to_numeric(df_salary["salary_in_usd"], errors="coerce").round().astype("Int64"),
        "salary_currency": clean_text(df_salary["salary_currency"], 10),
    }
)
df_fact_salary = pd.merge(df_fact_salary, df_junk_db, on=JUNK_COLUMNS, how="left")
if df_fact_salary["Id_Junk_dim"].isna().any():
    raise RuntimeError("Jointure Fact_salary : Id_Junk_dim manquant")
df_fact_salary = df_fact_salary[
    ["salary_id", "date_id", "country_code", "Id_Junk_dim",
     "salary", "salary_in_usd", "salary_currency"]
]
load_table(df_fact_salary, "Fact_salary", db_gold)

# =========================================================================
# 10. bridge : une ligne par (ligne de Fact_job, compétence)
# =========================================================================
print("\n--- 10. bridge ---")
df_bridge_gold = df_bridge_silver.rename(columns={"job_id": "Id_dim_job"})
df_bridge_gold = pd.merge(
    df_bridge_gold,
    df_fact_job[["job_id", "Id_dim_job"]],
    on="Id_dim_job",
    how="inner",
)
df_bridge_gold = df_bridge_gold[["Id_skills", "job_id"]].drop_duplicates()
df_bridge_gold = df_bridge_gold.sort_values(by=["job_id", "Id_skills"])
df_bridge_gold = df_bridge_gold.reset_index(drop=True)
df_bridge_gold.insert(0, "Id_bridge", range(1, len(df_bridge_gold) + 1))
print("  Lignes bridge Silver (offre x compétence) :", len(df_bridge_silver))
load_table(df_bridge_gold, "bridge", db_gold)

# =========================================================================
# 11. CONTRÔLES FINAUX
# =========================================================================
print("\n--- 11. Contrôles finaux ---")
with db_gold.connect() as connection:
    for table_name in GOLD_TABLES:
        nb = connection.execute(text("SELECT COUNT(*) FROM " + table_name)).scalar()
        print(" ", table_name, ":", nb, "lignes")
    offers_count = connection.execute(
        text("SELECT COUNT(DISTINCT Id_dim_job) FROM Fact_job")
    ).scalar()

print("\nRapprochements attendus :")
print("  Fact_job     = jobs_freehire :", len(df_fact_job), "=", len(df_job))
print("  offres       = ID distincts  :", offers_count, "=", df_job["ID"].nunique())
print("  Fact_salary  = salary Silver :", len(df_fact_salary), "=", len(df_salary))
# =========================================================================
# 12. JOURNAL DU CHARGEMENT (point de départ des chargements incrémentaux)
# =========================================================================
df_log = pd.DataFrame(
    {
        "run_start": [run_start],
        "run_end": [pd.Timestamp.now().floor("s")],
        "load_mode": ["full"],
        "offers_new": [int(offers_count)],
        "offers_updated": [0],
        "fact_rows_loaded": [len(df_fact_job)],
        "bridge_rows_loaded": [len(df_bridge_gold)],
        "watermark": [df_job["silver_loaded_at"].max()],
    }
)
load_table(df_log, "etl_load_log", db_gold)
print("Repère enregistré (watermark) :", df_job["silver_loaded_at"].max())
print("\nChargement du Gold terminé.")
