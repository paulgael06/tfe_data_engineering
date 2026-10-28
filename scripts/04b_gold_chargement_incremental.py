"""
GOLD_TFE - CHARGEMENT INCRÉMENTAL
Source : base TFE_Silver (jobs_freehire, skills, bridge)
Cible  : base TFE_Gold (déjà chargée une première fois par 04_gold_chargement.py)

Principe :
  - le Gold garde un repère (watermark) dans etl_load_log : la plus grande
    date silver_loaded_at déjà reprise ;
  - on ne lit dans le Silver que les lignes plus récentes que ce repère ;
  - offre NOUVELLE  (Id_dim_job absent du Gold) -> insertion ;
  - offre MODIFIÉE  (Id_dim_job déjà dans le Gold) -> mise à jour : ses lignes
    (dim_job, Fact_job, bridge) sont remplacées par la nouvelle version ;
  - offre INCHANGÉE -> jamais relue, donc jamais dupliquée ;
  - dimensions : on n'ajoute que les valeurs qui n'existent pas encore ;
  - tout se passe dans UNE transaction : en cas d'erreur, le Gold reste intact.

Garanties contre les doublons :
  - clé primaire Fact_job.job_id et contrainte UNIQUE (Id_dim_job, country_code) ;
  - contrôle avant écriture : un external_id déjà présent ne peut pas revenir
    sous un autre Id_dim_job ;
  - contrôles après écriture (doublons offre x pays, external_id).

Fact_salary (Kaggle, fichier fixe), Dim_country ne sont pas touchées.
"""
import pandas as pd
from sqlalchemy import DateTime, bindparam, create_engine, text
from db_config import build_connection_url

# =========================================================================
# 0. CONNEXIONS
# =========================================================================
db_silver = create_engine(
    build_connection_url("TFE_Silver")
)
# Pas de fast_executemany : le lot incrémental est petit et dim_job contient
# une colonne NVARCHAR(MAX) (description).
db_gold = create_engine(
    build_connection_url("TFE_Gold")
)

ND = "ND"
JUNK_COLUMNS = ["seniority", "job_category", "education_lvl"]
COMPANY_COLUMNS = ["company_name", "company_location", "company_size", "domain"]
ID_CHUNK_SIZE = 500


# =========================================================================
# FONCTIONS UTILITAIRES (identiques au chargement complet)
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


def load_table(df, table_name, connection):
    df.to_sql(table_name, con=connection, if_exists="append", index=False, chunksize=2000)
    print("  ", table_name, ": lignes ajoutées :", len(df))


def id_chunks(id_list):
    """Découpe une liste d'identifiants entiers en morceaux de 500."""
    for start in range(0, len(id_list), ID_CHUNK_SIZE):
        yield ",".join(str(int(x)) for x in id_list[start:start + ID_CHUNK_SIZE])


def add_junk_keys(df):
    """Clés de comparaison en minuscules (SQL Server ignore la casse dans UNIQUE)."""
    for column in JUNK_COLUMNS:
        df[column + "_key"] = df[column].str.lower()
    return df


JUNK_KEYS = [column + "_key" for column in JUNK_COLUMNS]

run_start = pd.Timestamp.now().floor("s")

# =========================================================================
# 1. REPÈRE DU DERNIER CHARGEMENT
# =========================================================================
print("--- 1. Repère du dernier chargement ---")
df_log = pd.read_sql("SELECT run_id, watermark FROM etl_load_log", con=db_gold)
if len(df_log) == 0:
    raise RuntimeError("etl_load_log vide : lance d'abord 04_gold_chargement.py (chargement complet)")
df_log = df_log.sort_values("run_id")
watermark = df_log["watermark"].iloc[-1]
if pd.isna(watermark):
    raise RuntimeError("Repère (watermark) absent du dernier chargement")
print("Dernier repère silver_loaded_at :", watermark)

# =========================================================================
# 2. LECTURE DU DELTA DANS LE SILVER
# =========================================================================
print("\n--- 2. Lecture du delta dans le Silver ---")
delta_query = text(
    "SELECT * FROM jobs_freehire WHERE silver_loaded_at > :watermark"
).bindparams(bindparam("watermark", type_=DateTime))
df_job = pd.read_sql(
    delta_query,
    con=db_silver,
    params={"watermark": pd.Timestamp(watermark).to_pydatetime()},
)
print("Lignes (offre x pays) à traiter :", len(df_job), "|", df_job["ID"].nunique(), "offres")

new_watermark = df_job["silver_loaded_at"].max() if len(df_job) > 0 else watermark

if len(df_job) == 0:
    print("Rien à charger : le Gold est déjà à jour.")
    df_empty_log = pd.DataFrame(
        {
            "run_start": [run_start],
            "run_end": [pd.Timestamp.now().floor("s")],
            "load_mode": ["incremental"],
            "offers_new": [0],
            "offers_updated": [0],
            "fact_rows_loaded": [0],
            "bridge_rows_loaded": [0],
            "watermark": [watermark],
        }
    )
    with db_gold.begin() as connection:
        load_table(df_empty_log, "etl_load_log", connection)
    raise SystemExit(0)

if df_job["year_month"].isna().sum() > 0:
    raise RuntimeError("year_month contient des valeurs manquantes")
if df_job.duplicated(subset=["ID", "country_code"]).any():
    raise RuntimeError("Le delta Silver contient des doublons (offre, pays)")
if df_job["Id_job_country"].duplicated().any():
    raise RuntimeError("Le delta Silver contient des Id_job_country en double")

delta_ids = sorted(int(x) for x in df_job["ID"].unique())
df_skills_silver = pd.read_sql("SELECT * FROM skills", con=db_silver)
bridge_parts = []
for id_list in id_chunks(delta_ids):
    bridge_parts.append(
        pd.read_sql("SELECT * FROM bridge WHERE job_id IN (" + id_list + ")", con=db_silver)
    )
df_bridge_silver = pd.concat(bridge_parts, ignore_index=True)

# =========================================================================
# 3. NOUVELLES OFFRES OU OFFRES MODIFIÉES ? + CONTRÔLE ANTI-DOUBLON
# =========================================================================
print("\n--- 3. Nouvelles offres ou offres modifiées ---")
df_gold_offers = pd.read_sql("SELECT Id_dim_job FROM dim_job", con=db_gold)
df_gold_keys = pd.read_sql(
    "SELECT DISTINCT external_id, Id_dim_job FROM Fact_job", con=db_gold
)
df_gold_keys.columns = ["external_id", "Id_dim_job"]
# Comparaison sans tenir compte de la casse (comme SQL Server)
df_gold_keys["external_key"] = df_gold_keys["external_id"].astype("string").str.strip().str.lower()
gold_offer_ids = set(int(x) for x in df_gold_offers["Id_dim_job"])
updated_ids = [x for x in delta_ids if x in gold_offer_ids]
new_ids = [x for x in delta_ids if x not in gold_offer_ids]
print("  offres nouvelles :", len(new_ids))
print("  offres modifiées (mise à jour) :", len(updated_ids))

# Un external_id déjà dans le Gold ne doit pas revenir sous un autre identifiant
df_delta_keys = df_job[["ID", "external_id"]].drop_duplicates().copy()
df_delta_keys["external_key"] = df_delta_keys["external_id"].astype("string").str.strip().str.lower()
df_check = pd.merge(
    df_delta_keys[["ID", "external_key"]],
    df_gold_keys[["Id_dim_job", "external_key", "external_id"]],
    on="external_key",
    how="inner",
)
conflicts = df_check[df_check["ID"] != df_check["Id_dim_job"]]
if len(conflicts) > 0:
    raise RuntimeError(
        "external_id déjà présent dans le Gold sous un autre identifiant : "
        + str(conflicts["external_id"].head(5).tolist())
    )
print("  contrôle anti-doublon external_id : OK")

# =========================================================================
# 4. TRANSFORMATIONS (mêmes règles que le chargement complet)
# =========================================================================
print("\n--- 4. Transformations ---")
freehire_seniority = clean_text(df_job["seniority_final"], 20)
freehire_seniority = freehire_seniority.where(
    freehire_seniority == ND, freehire_seniority.str.lower()
)
df_job["seniority"] = freehire_seniority
df_job["job_category"] = clean_text(df_job["enrichment.category"], 50)
df_job["education_lvl"] = clean_text(df_job["education_final"], 50)
df_job["company_name"] = clean_text(df_job["company"], 300)
df_job["company_location"] = clean_text(df_job["location"], 300)
df_job["company_size"] = clean_text(df_job["enrichment.company_size"], 50)
df_job["domain"] = clean_text(df_job["enrichment.domains"], 500)

# =========================================================================
# 5. ÉCRITURE DANS LE GOLD (UNE SEULE TRANSACTION)
# =========================================================================
print("\n--- 5. Écriture dans le Gold (transaction) ---")
with db_gold.begin() as connection:

    # 5.1 Offres modifiées : on retire l'ancienne version (bridge, faits, offre)
    for id_list in id_chunks(updated_ids):
        connection.execute(text(
            "DELETE FROM bridge WHERE job_id IN "
            "(SELECT job_id FROM Fact_job WHERE Id_dim_job IN (" + id_list + "))"
        ))
        connection.execute(text("DELETE FROM Fact_job WHERE Id_dim_job IN (" + id_list + ")"))
        connection.execute(text("DELETE FROM dim_job WHERE Id_dim_job IN (" + id_list + ")"))
    print("  anciennes versions retirées :", len(updated_ids), "offres")

    # 5.2 Dim_date : seulement les mois qui n'existent pas encore
    existing_dates = set(
        int(x) for x in pd.read_sql(text("SELECT date_id FROM Dim_date"), connection)["date_id"]
    )
    delta_dates = sorted(set(int(x) for x in df_job["year_month"]) - existing_dates)
    df_dim_date = pd.DataFrame({"date_id": delta_dates})
    df_dim_date["year"] = df_dim_date["date_id"] // 100
    df_dim_date["month"] = df_dim_date["date_id"] % 100
    load_table(df_dim_date, "Dim_date", connection)

    # 5.3 Dim_country : lecture seule (codes connus)
    df_country_db = pd.read_sql(text("SELECT country_code FROM Dim_country"), connection)
    known_codes = set(df_country_db["country_code"].astype(str))

    # 5.4 Junk_dim : seulement les combinaisons qui n'existent pas encore
    df_junk_known = pd.read_sql(
        text("SELECT Id_Junk_dim, seniority, job_category, education_lvl FROM Junk_dim"),
        connection,
    )
    df_junk_known.columns = ["Id_Junk_dim"] + JUNK_COLUMNS
    df_junk_known = add_junk_keys(df_junk_known)
    df_junk_delta = df_job[JUNK_COLUMNS].drop_duplicates().copy()
    df_junk_delta = add_junk_keys(df_junk_delta)
    df_junk_delta = df_junk_delta.drop_duplicates(subset=JUNK_KEYS)
    df_junk_merge = pd.merge(
        df_junk_delta, df_junk_known[JUNK_KEYS], on=JUNK_KEYS, how="left", indicator=True
    )
    df_junk_new = df_junk_merge[df_junk_merge["_merge"] == "left_only"][JUNK_COLUMNS]
    load_table(df_junk_new, "Junk_dim", connection)
    df_junk_db = pd.read_sql(
        text("SELECT Id_Junk_dim, seniority, job_category, education_lvl FROM Junk_dim"),
        connection,
    )
    df_junk_db.columns = ["Id_Junk_dim"] + JUNK_COLUMNS
    df_junk_db = add_junk_keys(df_junk_db)
    df_junk_db = df_junk_db.sort_values("Id_Junk_dim").drop_duplicates(subset=JUNK_KEYS)
    df_junk_db = df_junk_db[["Id_Junk_dim"] + JUNK_KEYS]

    # 5.5 Dim_company : seulement les entreprises qui n'existent pas encore
    df_company_known = pd.read_sql(
        text("SELECT company_id, company_name, company_location, company_size, domain FROM Dim_company"),
        connection,
    )
    df_company_known.columns = ["company_id"] + COMPANY_COLUMNS
    df_company_delta = df_job[COMPANY_COLUMNS].drop_duplicates()
    df_company_merge = pd.merge(
        df_company_delta,
        df_company_known[COMPANY_COLUMNS].drop_duplicates(),
        on=COMPANY_COLUMNS,
        how="left",
        indicator=True,
    )
    df_company_new = df_company_merge[df_company_merge["_merge"] == "left_only"][COMPANY_COLUMNS]
    load_table(df_company_new, "Dim_company", connection)
    df_company_db = pd.read_sql(
        text("SELECT company_id, company_name, company_location, company_size, domain FROM Dim_company"),
        connection,
    )
    df_company_db.columns = ["company_id"] + COMPANY_COLUMNS
    df_company_db = df_company_db.sort_values("company_id").drop_duplicates(subset=COMPANY_COLUMNS)

    # 5.6 skills : seulement les compétences dont l'identifiant n'existe pas encore
    existing_skill_ids = set(
        int(x) for x in pd.read_sql(text("SELECT Id_skills FROM skills"), connection)["Id_skills"]
    )
    df_skills_new = df_skills_silver[~df_skills_silver["Id_skills"].isin(existing_skill_ids)]
    df_skills_gold = pd.DataFrame(
        {
            "Id_skills": df_skills_new["Id_skills"],
            "skills_name": truncate_text(df_skills_new["skill_name"], 200),
            "skill_type": df_skills_new["skill_type"],
        }
    )
    load_table(df_skills_gold, "skills", connection)

    # 5.7 dim_job : nouvelles offres + offres modifiées (nouvelle version)
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
    load_table(df_dim_job, "dim_job", connection)

    # 5.8 Fact_job
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
    df_fact_job = add_junk_keys(df_fact_job)
    df_fact_job = pd.merge(df_fact_job, df_company_db, on=COMPANY_COLUMNS, how="left")
    df_fact_job = pd.merge(df_fact_job, df_junk_db, on=JUNK_KEYS, how="left")
    if df_fact_job["company_id"].isna().any() or df_fact_job["Id_Junk_dim"].isna().any():
        raise RuntimeError("Jointure Fact_job : clé company_id ou Id_Junk_dim manquante")
    df_fact_job = df_fact_job.drop_duplicates(subset=["Id_dim_job", "country_code"])
    FACT_JOB_COLUMNS = [
        "job_id", "external_id", "public_slug", "Id_dim_job", "date_id",
        "company_id", "country_code", "Id_Junk_dim", "salary_min_annual",
        "salary_max_annual", "salary_ref_annual", "salary_in_usd",
        "salary_currency", "load_date",
    ]
    df_fact_job = df_fact_job[FACT_JOB_COLUMNS]
    load_table(df_fact_job, "Fact_job", connection)

    # 5.9 bridge : une ligne par (ligne de Fact_job, compétence)
    df_bridge_gold = df_bridge_silver.rename(columns={"job_id": "Id_dim_job"})
    df_bridge_gold = pd.merge(
        df_bridge_gold,
        df_fact_job[["job_id", "Id_dim_job"]],
        on="Id_dim_job",
        how="inner",
    )
    df_bridge_gold = df_bridge_gold[["Id_skills", "job_id"]].drop_duplicates()
    df_bridge_gold = df_bridge_gold.sort_values(by=["job_id", "Id_skills"]).reset_index(drop=True)
    max_bridge_id = pd.read_sql(text("SELECT MAX(Id_bridge) AS max_id FROM bridge"), connection)["max_id"].iloc[0]
    bridge_start = 0 if pd.isna(max_bridge_id) else int(max_bridge_id)
    df_bridge_gold.insert(
        0, "Id_bridge", range(bridge_start + 1, bridge_start + 1 + len(df_bridge_gold))
    )
    load_table(df_bridge_gold, "bridge", connection)

    # 5.10 Contrôles anti-doublon avant validation de la transaction
    duplicate_pairs = connection.execute(text(
        "SELECT COUNT(*) FROM (SELECT Id_dim_job, country_code FROM Fact_job "
        "GROUP BY Id_dim_job, country_code HAVING COUNT(*) > 1) t"
    )).scalar()
    # Contrôle limité aux offres de ce lot (SQL Server ignore la casse)
    duplicate_external = 0
    for id_list in id_chunks(delta_ids):
        duplicate_external += connection.execute(text(
            "SELECT COUNT(*) FROM (SELECT f.external_id FROM Fact_job f "
            "WHERE f.external_id IN (SELECT external_id FROM Fact_job "
            "WHERE Id_dim_job IN (" + id_list + ")) "
            "GROUP BY f.external_id HAVING COUNT(DISTINCT f.Id_dim_job) > 1) t"
        )).scalar()
    if duplicate_pairs > 0 or duplicate_external > 0:
        df_offenders = pd.read_sql(text(
            "SELECT TOP 10 external_id, COUNT(DISTINCT Id_dim_job) AS nb_offres "
            "FROM Fact_job GROUP BY external_id HAVING COUNT(DISTINCT Id_dim_job) > 1"
        ), connection)
        print("Identifiants en cause (exemples) :")
        print(df_offenders)
        raise RuntimeError("Doublons détectés dans le Gold : chargement annulé")
    print("  contrôle doublons (offre x pays) :", duplicate_pairs, "| external_id partagés :", duplicate_external)

    # 5.11 Journal
    df_run_log = pd.DataFrame(
        {
            "run_start": [run_start],
            "run_end": [pd.Timestamp.now().floor("s")],
            "load_mode": ["incremental"],
            "offers_new": [len(new_ids)],
            "offers_updated": [len(updated_ids)],
            "fact_rows_loaded": [len(df_fact_job)],
            "bridge_rows_loaded": [len(df_bridge_gold)],
            "watermark": [new_watermark],
        }
    )
    load_table(df_run_log, "etl_load_log", connection)

# =========================================================================
# 6. CONTRÔLES FINAUX
# =========================================================================
print("\n--- 6. Contrôles finaux ---")
with db_gold.connect() as connection:
    for table_name in ["Dim_date", "Junk_dim", "Dim_company", "dim_job", "skills", "Fact_job", "bridge"]:
        nb = connection.execute(text("SELECT COUNT(*) FROM " + table_name)).scalar()
        print(" ", table_name, ":", nb, "lignes")
    offers_count = connection.execute(text("SELECT COUNT(DISTINCT Id_dim_job) FROM Fact_job")).scalar()
    dim_job_count = connection.execute(text("SELECT COUNT(*) FROM dim_job")).scalar()
print("  offres distinctes dans Fact_job = lignes de dim_job :", offers_count, "=", dim_job_count)
print("\nChargement incrémental terminé : nouvelles =", len(new_ids), "| modifiées =", len(updated_ids))
