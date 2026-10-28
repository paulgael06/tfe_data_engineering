"""
SILVER freehire - deux modes de chargement :
  python 02b_silver_jobs_freehire.py full         -> reconstruit tout le Silver depuis le Bronze
  python 02b_silver_jobs_freehire.py incremental  -> ne traite que les offres nouvelles ou modifiées
Sans argument : mode full.

Mode incremental :
  - une offre est identifiée par external_id (clé métier stable) ;
  - nouvelle offre   : absente du Silver -> nouvel ID ;
  - offre modifiée   : last_seen_at du Bronze plus récent que celui du Silver
                       -> même ID, ses lignes Silver sont remplacées ;
  - offre inchangée  : ignorée (jamais retraitée, jamais dupliquée) ;
  - les identifiants (ID, Id_job_country, Id_skills) ne changent jamais.
"""
import ast
import sys
import numpy as np
import pandas as pd
import pyodbc
import re
from sqlalchemy import create_engine, text
from db_config import build_connection_url

db_bronze = create_engine(
    build_connection_url("TFE_Bronze")
)
db__silver = create_engine(
    build_connection_url("TFE_Silver")
)


LOAD_MODE = sys.argv[1] if len(sys.argv) > 1 else "full"
if LOAD_MODE not in ("full", "incremental"):
    raise SystemExit("Mode inconnu : utilise 'full' ou 'incremental'")
print("Mode de chargement Silver :", LOAD_MODE)

query = "SELECT * FROM jobs_freehire"

df_job = pd.read_sql(query, con=db_bronze)
df_job = df_job.sort_values(by="last_seen_at", ascending=False)

# Clé de comparaison de l'offre : external_id sans espaces ni majuscules.
# L'API écrit parfois le même identifiant de deux façons (AccorCorp:744... et
# accorcorp:744...) : c'est la même offre, on garde la ligne la plus récente.
df_job["external_key"] = df_job["external_id"].astype("string").str.strip().str.lower()
duplicates_count = int(df_job.duplicated(subset=["external_key"]).sum())
df_job = df_job.drop_duplicates(["external_key"], keep="first")
print("Doublons d'offres supprimés (external_id, sans tenir compte de la casse) :", duplicates_count)

if LOAD_MODE == "full":
    df_job.insert(0, "ID", range(1, len(df_job) + 1))
else:
    # Offres déjà présentes dans le Silver : une ligne par offre
    df_known = pd.read_sql(
        "SELECT ID, external_id, last_seen_at FROM jobs_freehire", con=db__silver
    )
    df_known["external_key"] = (
        df_known["external_id"].astype("string").str.strip().str.lower()
    )
    df_known = df_known.drop(columns=["external_id"])
    df_known = df_known.drop_duplicates(subset=["external_key"], keep="first")
    df_known = df_known.rename(
        columns={"ID": "known_id", "last_seen_at": "known_last_seen_at"}
    )
    df_job = pd.merge(df_job, df_known, on="external_key", how="left")

    bronze_seen = pd.to_datetime(df_job["last_seen_at"], errors="coerce", utc=True)
    silver_seen = pd.to_datetime(
        df_job["known_last_seen_at"], errors="coerce", utc=True
    )
    is_new = df_job["known_id"].isna()
    is_modified = (~is_new) & (bronze_seen > silver_seen)
    print("Offres dans le Bronze          :", len(df_job))
    print("  nouvelles                    :", int(is_new.sum()))
    print("  modifiées (last_seen_at plus récent) :", int(is_modified.sum()))
    print("  inchangées (ignorées)        :", int((~is_new & ~is_modified).sum()))

    df_job = df_job[is_new | is_modified].copy()
    if len(df_job) == 0:
        print("Rien à charger : le Silver est déjà à jour.")
        sys.exit(0)

    # IDs stables : une offre connue garde son ID, une nouvelle prend le suivant
    max_known_id = int(
        pd.read_sql("SELECT MAX(ID) AS max_id FROM jobs_freehire", con=db__silver)[
            "max_id"
        ].iloc[0]
    )
    modified_ids = sorted(int(x) for x in df_job["known_id"].dropna().unique())
    new_mask = df_job["known_id"].isna()
    df_job["ID"] = df_job["known_id"]
    df_job.loc[new_mask, "ID"] = range(max_known_id + 1, max_known_id + 1 + int(new_mask.sum()))
    df_job["ID"] = df_job["ID"].astype("int64")
    df_job = df_job.drop(columns=["known_id", "known_last_seen_at"])
    id_column = df_job.pop("ID")
    df_job.insert(0, "ID", id_column)

# La clé de comparaison ne sert qu'ici : elle n'est pas écrite dans le Silver
df_job = df_job.drop(columns=["external_key"])
df_job["countries"] = df_job["countries"].str.upper()


# 1. Isolation du tableau de travail et vérification Bronze "[]" pour skills
df_skills_work = df_job[["ID", "skills"]].copy()


def check_bronze_empty(value):
    if pd.isna(value):
        return True
    text_value = str(value).strip()
    return text_value in ["[]", ""]


def convert_skills(x):
    if check_bronze_empty(x):
        return ["noskill"]
    try:
        skills = ast.literal_eval(x)
        if not isinstance(skills, list) or len(skills) == 0:
            return ["noskill"]
        return skills
    except (ValueError, SyntaxError):
        return ["noskill"]


df_skills_work["skills"] = df_skills_work["skills"].apply(convert_skills)


# 2. Nettoyage de la colonne countries en chaîne propre, sans tri, avec "ND" pour les vides
def clean_countries(x):
    if pd.isna(x):
        return "ND"
    text_value = str(x).strip()
    if text_value in ["[]", "", "nan", "none"]:
        return "ND"
    try:
        countries_list = ast.literal_eval(text_value)
        if not isinstance(countries_list, list) or len(countries_list) == 0:
            return "ND"
        countries_clean = [str(p).strip().upper() for p in countries_list if p]
        if not countries_clean:
            return "ND"
        return ", ".join(countries_clean)
    except (ValueError, SyntaxError):
        return "ND"


df_job["countries"] = df_job["countries"].apply(clean_countries)


# 3. Nettoyage de la colonne regions en chaîne propre, sans tri, avec "ND" pour les vides
def clean_regions(x):
    if pd.isna(x):
        return "ND"
    text_value = str(x).strip()
    if text_value in ["[]", "", "nan", "none"]:
        return "ND"
    try:
        regions_list = ast.literal_eval(text_value)
        if not isinstance(regions_list, list) or len(regions_list) == 0:
            return "ND"
        regions_clean = [str(r).strip().upper() for r in regions_list if r]
        if not regions_clean:
            return "ND"
        return ", ".join(regions_clean)
    except (ValueError, SyntaxError):
        return "ND"


df_job["regions"] = df_job["regions"].apply(clean_regions)


# 4. Nettoyage de la colonne cities en chaîne propre, sans tri, avec "ND" pour les vides
def clean_cities(x):
    if pd.isna(x):
        return "ND"
    text_value = str(x).strip()
    if text_value in ["[]", "", "nan", "none"]:
        return "ND"
    try:
        cities_list = ast.literal_eval(text_value)
        if not isinstance(cities_list, list) or len(cities_list) == 0:
            return "ND"
        cities_clean = [str(c).strip() for c in cities_list if c]
        if not cities_clean:
            return "ND"
        return ", ".join(cities_clean)
    except (ValueError, SyntaxError):
        return "ND"


df_job["cities"] = df_job["cities"].apply(clean_cities)


# 5. Nettoyage de la colonne collections en chaîne propre, sans tri, avec "ND" pour les vides
def clean_collections(x):
    if pd.isna(x):
        return "ND"
    text_value = str(x).strip()
    if text_value in ["[]", "", "nan", "none"]:
        return "ND"
    try:
        collections_list = ast.literal_eval(text_value)
        if not isinstance(collections_list, list) or len(collections_list) == 0:
            return "ND"
        collections_clean = [str(col).strip() for col in collections_list if col]
        if not collections_clean:
            return "ND"
        return ", ".join(collections_clean)
    except (ValueError, SyntaxError):
        return "ND"


df_job["collections"] = df_job["collections"].apply(clean_collections)


# 6. Nettoyage de la colonne enrichment.domains en chaîne propre, sans tri, avec "ND" pour les vides
def clean_enrichment_domains(x):
    if pd.isna(x):
        return "ND"
    text_value = str(x).strip()
    if text_value in ["[]", "", "nan", "none"]:
        return "ND"
    try:
        domains_list = ast.literal_eval(text_value)
        if not isinstance(domains_list, list) or len(domains_list) == 0:
            return "ND"
        domains_clean = [str(d).strip().lower() for d in domains_list if d]
        if not domains_clean:
            return "ND"
        return ", ".join(domains_clean)
    except (ValueError, SyntaxError):
        return "ND"


df_job["enrichment.domains"] = df_job["enrichment.domains"].apply(
    clean_enrichment_domains
)


# =========================================================================
# 6 BIS. ENRICHISSEMENT DE enrichment.seniority
# =========================================================================
seniority_patterns = {
    "c_level": [
        r"\bc[-_\s]?level\b",
        r"\bcto\b",
        r"\bcio\b",
        r"\bciso\b",
        r"\bc[-_\s]?suite\b",
        r"\bvp\b",
        r"\bvice[-_\s]?president\b",
        r"\bdirigeant\b",
    ],
    "lead": [
        r"\btech[-_\s]?lead\b",
        r"\bteam[-_\s]?lead\b",
        r"\blead developer\b",
        r"\blead data\b",
        r"\bchef d'équipe\b",
        r"\blead\b",
        r"\bleader\b",
        r"\bresponsable\b",
    ],
    "principal": [r"\bprincipal\b"],
    "staff": [r"\bstaff\b"],
    "senior": [
        r"\bsenior\b",
        r"\bsr\b",
        r"\bexpérimenté\b",
        r"\bexperimente\b",
    ],
    "middle": [
        r"\bmiddle\b",
        r"\bmid\b",
        r"\bconfirmé\b",
        r"\bconfirme\b",
        r"\bmedior\b",
        r"\bintermédiaire\b",
    ],
    "junior": [
        r"\bjunior\b",
        r"\bjr\b",
        r"\bdébutant\b",
        r"\bdebutant\b",
        r"\bgraduate\b",
        r"\bentry[-_\s]?level\b",
    ],
    "intern": [r"\bintern\b", r"\bstage\b", r"\bstagiaire\b", r"\binternship\b"],
}


def search_seniority_text(text_value):
    if not text_value or pd.isna(text_value):
        return None
    text_lower = str(text_value).lower()
    for level, patterns in seniority_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return level
    return None


def detect_seniority_row(row):
    current_val = row["enrichment.seniority"]
    if (
        pd.notna(current_val)
        and str(current_val).strip().lower() not in ["nan", "none", ""]
    ):
        return current_val, "source_original"

    fields_to_test = [
        ("title", row.get("title", "")),
        ("enrichment.summary", row.get("enrichment.summary", "")),
    ]

    for field_name, field_value in fields_to_test:
        level_found = search_seniority_text(field_value)
        if level_found:
            return level_found, field_name

    return "ND", "ND"


new_vals = []
new_sources = []
for idx, row in df_job.iterrows():
    val, src = detect_seniority_row(row)
    new_vals.append(val)
    new_sources.append(src)

df_job["seniority_final"] = new_vals
df_job["seniority_origin"] = new_sources


# =========================================================================
# 7. TRAITEMENT DE enrichment.requirements (Intégration des 3 colonnes)
# =========================================================================
df_req_work = df_job[["ID", "enrichment.requirements"]].copy()


def parse_requirements_audited(x):
    if pd.isna(x):
        return []
    text_value = str(x).strip()
    if text_value in ["[]", "", "nan", "none"]:
        return []
    try:
        req_list = ast.literal_eval(text_value)
        if not isinstance(req_list, list) or len(req_list) == 0:
            return []

        parsed_items = []
        for item in req_list:
            if isinstance(item, dict):
                req_text = item.get("text")
                if not req_text or str(req_text).strip() == "":
                    continue

                cleaned_text = str(req_text).strip()

                if "priority" not in item or item.get("priority") is None:
                    priority = "required"
                else:
                    priority_val = str(item.get("priority")).strip().lower()
                    if priority_val not in ["required", "preferred"]:
                        priority = "required"
                    else:
                        priority = priority_val

                parsed_items.append({"text": cleaned_text, "priority": priority})

            elif isinstance(item, str):
                cleaned_text = str(item).strip()
                if cleaned_text:
                    parsed_items.append({"text": cleaned_text, "priority": "required"})

        return parsed_items
    except (ValueError, SyntaxError):
        return []


parsed_req_col = []
for val in df_req_work["enrichment.requirements"]:
    parsed_req_col.append(parse_requirements_audited(val))

df_req_work["parsed_req"] = parsed_req_col

df_job = df_job.drop(columns=["enrichment.requirements"], errors="ignore")

df_req_exploded = df_req_work.explode("parsed_req")

requirement_lines = []
for idx, row in df_req_exploded.iterrows():
    offer_id = row["ID"]
    item = row["parsed_req"]
    if pd.isna(item) or not isinstance(item, dict):
        requirement_lines.append({
            "ID": offer_id,
            "requirement_text": "norequirement",
            "priority": "ND",
        })
    else:
        requirement_lines.append({
            "ID": offer_id,
            "requirement_text": item["text"],
            "priority": item["priority"],
        })

df_req_clean = pd.DataFrame(requirement_lines)
df_req_clean = df_req_clean.drop_duplicates(
    subset=["ID", "requirement_text", "priority"]
)

df_real_req = df_req_clean[
    df_req_clean["requirement_text"] != "norequirement"
].copy()
df_real_req = df_real_req.sort_values(
    by=["ID", "priority", "requirement_text"]
)


def assemble_requirements_basket(text_series):
    return " | ".join(text_series)


df_req_baskets = (
    df_real_req.groupby(["ID", "priority"])["requirement_text"]
    .agg(assemble_requirements_basket)
    .reset_index()
)
df_req_baskets.rename(columns={"requirement_text": "basket_text"}, inplace=True)

df_pivot_req = df_req_baskets.pivot(
    index="ID", columns="priority", values="basket_text"
).reset_index()
if "required" not in df_pivot_req.columns:
    df_pivot_req["required"] = pd.NA
if "preferred" not in df_pivot_req.columns:
    df_pivot_req["preferred"] = pd.NA

df_pivot_req = df_pivot_req[["ID", "required", "preferred"]]
df_pivot_req.columns = [
    "ID",
    "requirements_required",
    "requirements_preferred",
]

df_offer_ids = df_job[["ID"]].copy()
df_offer_baskets = pd.merge(df_offer_ids, df_pivot_req, on="ID", how="left")

ids_noreq_source = df_req_clean[
    df_req_clean["requirement_text"] == "norequirement"
]["ID"]
mask_noreq = df_offer_baskets["ID"].isin(ids_noreq_source)

df_offer_baskets.loc[mask_noreq, "requirements_required"] = "ND"
df_offer_baskets.loc[mask_noreq, "requirements_preferred"] = "ND"

df_offer_baskets["requirements_required"] = df_offer_baskets[
    "requirements_required"
].fillna("aucune exigence")
df_offer_baskets["requirements_preferred"] = df_offer_baskets[
    "requirements_preferred"
].fillna("aucune exigence")

all_baskets = [{"requirements_required": "ND", "requirements_preferred": "ND"}]
unique_baskets = df_offer_baskets[
    ["requirements_required", "requirements_preferred"]
].drop_duplicates()

df_dim_requirements_baskets = pd.DataFrame(all_baskets)
df_dim_requirements_baskets = pd.concat(
    [df_dim_requirements_baskets, unique_baskets], ignore_index=True
)
df_dim_requirements_baskets = df_dim_requirements_baskets.drop_duplicates(
    subset=["requirements_required", "requirements_preferred"]
).reset_index(drop=True)

df_dim_requirements_baskets["requirement_basket_id"] = range(
    0, len(df_dim_requirements_baskets)
)
df_dim_requirements_baskets = df_dim_requirements_baskets[
    [
        "requirement_basket_id",
        "requirements_required",
        "requirements_preferred",
    ]
]

df_offer_baskets_joined = pd.merge(
    df_offer_baskets,
    df_dim_requirements_baskets,
    on=["requirements_required", "requirements_preferred"],
    how="left",
)

df_offer_keys = df_offer_baskets_joined[[
    "ID",
    "requirement_basket_id",
    "requirements_required",
    "requirements_preferred",
]].copy()
df_offer_keys["requirement_basket_id"] = df_offer_keys[
    "requirement_basket_id"
].astype(int)

df_job = pd.merge(df_job, df_offer_keys, on="ID", how="left")


# =========================================================================
# 8 BIS. ENRICHISSEMENT DE enrichment.education_level
# =========================================================================
education_patterns = {
    "phd": [
        r"\bphd\b",
        r"\bdoctorat\b",
        r"\bdoctorate\b",
        r"\bph\.d\b",
    ],
    "master": [
        r"\bmaster['s]?\b",
        r"\bmsc\b",
        r"\bm\.sc\b",
        r"\bmaîtrise\b",
        r"\bmaitrise\b",
        r"\bdiplôme d'ingénieur\b",
        r"\bécole d'ingénieur\b",
        r"\becole d'ingenieur\b",
        r"\bbac\+5\b",
    ],
    "bachelor": [
        r"\bbachelor['s]?\b",
        r"\blicence académique\b",
        r"\blicence universitaire\b",
        r"\bb\.sc\b",
        r"\bb\.a\b",
        r"\bbac\+3\b",
        r"\bbac\+4\b",
        r"\bhbo\b",
        r"\bundergraduate\b",
    ],
}


def clean_text_anti_traps(text_value):
    if not text_value or pd.isna(text_value):
        return ""
    t = str(text_value).lower()
    traps = [
        "master data management",
        "master data",
        "scrum master",
        "masterclass",
        "licence software",
        "software licence",
        "driver's license",
        "driving license",
    ]
    for p in traps:
        t = t.replace(p, "")
    return t


def search_education_text(text_value):
    text_clean = clean_text_anti_traps(text_value)
    if not text_clean:
        return None
    levels_detected = []
    for level, patterns in education_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text_clean):
                levels_detected.append(level)
                break
    if not levels_detected:
        return None
    if "phd" in levels_detected:
        return "phd"
    if "master" in levels_detected:
        return "master"
    if "bachelor" in levels_detected:
        return "bachelor"
    return None


def detect_education_row(row):
    current_val = row["enrichment.education_level"]
    if (
        pd.notna(current_val)
        and str(current_val).strip().lower() not in ["nan", "", "null"]
    ):
        return current_val, "source_original", "source_original"
    sources_to_evaluate = [
        ("requirements_required", "required"),
        ("requirements_preferred", "preferred"),
        ("title", "title"),
        ("enrichment.summary", "summary"),
    ]
    for field_name, prio_type in sources_to_evaluate:
        level_found = search_education_text(row.get(field_name, ""))
        if level_found:
            prio_val = prio_type if prio_type in ["required", "preferred"] else "ND"
            return level_found, field_name, prio_val
    return "ND", "ND", "ND"


new_edu, new_orig_edu, new_prio_edu = [], [], []
for idx, row in df_job.iterrows():
    edu, orig, prio = detect_education_row(row)
    new_edu.append(edu)
    new_orig_edu.append(orig)
    new_prio_edu.append(prio)

df_job["education_final"] = new_edu
df_job["education_origin"] = new_orig_edu
df_job["education_priority"] = new_prio_edu


# =========================================================================
# 9 TER. ENRICHISSEMENT DE enrichment.experience_years_min (Optimisé & Testé par champ)
# =========================================================================
fields_priority_order = [
    "requirements_required",
    "description",
    "enrichment.summary",
    "requirements_preferred",
]

number_words = {
    "one": 1,
    "un": 1,
    "une": 1,
    "two": 2,
    "deux": 2,
    "three": 3,
    "trois": 3,
    "four": 4,
    "quatre": 4,
    "five": 5,
    "cinq": 5,
    "six": 6,
    "seven": 7,
    "sept": 7,
    "eight": 8,
    "huit": 8,
    "nine": 9,
    "neuf": 9,
    "ten": 10,
    "dix": 10,
}


def extract_all_experience_years(text_value):
    if not text_value or pd.isna(text_value):
        return []
    t = str(text_value).lower()

    if not any(
        word in t
        for word in [
            "experience",
            "expériences",
            "professional",
            "career",
            "hintergrund",
            "erfahrung",
            "years",
            "ans",
            "jahre",
            "jaar",
            "años",
        ]
    ):
        return []

    values_found = []

    digit_patterns = [
        r"(\d+)\s*(?:-|à|to)\s*\d+\s*(?:years?|ans?|años?|jahre?|jaar)",
        r"(\d+)\s*\+?\s*(?:years?|ans?|años?|jahre?|jaar)\s*(?:of)?\s*(?:professional|experience|expériences?)?",
        r"(?:minimum|at least|mindestens|min\.)\s*(\d+)\s*(?:years?|ans?|años?|jahre?|jaar)",
        r"(\d+)\s*(?:years?|ans?|años?|jahre?|jaar)\s*(?:of)?\s*(?:professional|experience|expériences?)?",
    ]

    for p in digit_patterns:
        for match in re.finditer(p, t):
            try:
                val = int(match.group(1))
                if 0 <= val <= 20:
                    values_found.append(val)
            except ValueError:
                continue

    for word, val in number_words.items():
        letter_pattern = rf"\b{word}\b\s*(?:years?|ans?|años?|jahre?|jaar)\s*(?:of)?\s*(?:professional|experience|expériences?)?"
        if re.search(letter_pattern, t):
            if 0 <= val <= 20:
                values_found.append(val)

    return values_found


def detect_experience_row_optimized(row):
    current_val = row["enrichment.experience_years_min"]
    if (
        pd.notna(current_val)
        and str(current_val).strip().lower() not in ["nan", "", "null"]
    ):
        return current_val, "source_original"

    for field_name in fields_priority_order:
        vals = extract_all_experience_years(row.get(field_name, ""))
        if vals:
            return min(vals), field_name

    return np.nan, "ND"


new_exp, new_orig_exp = [], []
for idx, row in df_job.iterrows():
    exp, orig = detect_experience_row_optimized(row)
    new_exp.append(exp)
    new_orig_exp.append(orig)

df_job["experience_min_final"] = new_exp
df_job["experience_min_origin"] = new_orig_exp


# =========================================================================
# 10. PANIER DE COMPÉTENCES (skills)
# =========================================================================
df_exploded = df_skills_work.explode("skills")
df_exploded["skills"] = df_exploded["skills"].str.strip().str.lower()
# Synonymes regroupés sous un seul nom (azure-ad s'appelle aujourd'hui entra-id)
SKILL_ALIASES = {"azure-ad": "entra-id"}
df_exploded["skills"] = df_exploded["skills"].replace(SKILL_ALIASES)
df_exploded = df_exploded.drop_duplicates(subset=["ID", "skills"])
df_exploded = df_exploded.sort_values(by=["ID", "skills"])


# Les paniers de compétences (dim_skills_paniers) sont abandonnés : les
# compétences passent par les tables skills et bridge (étape 10 BIS).
df_job = df_job.drop(columns=["skills"], errors="ignore")
df_job = df_job.drop(columns=["primary_country"], errors="ignore")


# =========================================================================
# 10 BIS. COMPÉTENCES INDIVIDUELLES ET TABLE PONT (modèle 1)
#   - skills : une ligne par compétence distincte (Id_skills, skill_name)
#   - bridge : une ligne par couple (offre, compétence)
#              (Id_bridge, Id_skills, job_id)  avec job_id = ID de l'offre
# Les compétences appartiennent à l'offre : la table pont pointe sur ID
# (unique dans dim_job), pas sur Id_job_country. "noskill" est conservé pour
# qu'aucune offre ne disparaisse de la table pont.
# =========================================================================
df_skills = pd.DataFrame(
    {"skill_name": sorted(df_exploded["skills"].dropna().unique())}
)
df_skills.insert(0, "Id_skills", range(1, len(df_skills) + 1))

# Type de compétence : outil (technologie nommée : langage, logiciel,
# plateforme, framework, format) ou concept (pratique, méthode, domaine,
# catégorie générique, réglementation, certification, protocole).
# Rien n'est supprimé : le filtre outil / concept se fait à la lecture (Q1).
CONCEPTS = (
    "ab-testing", "acceptance-criteria", "accessibility", "account-executive", "accounts-payable", "accounts-receivable",
    "agentic-ai", "agile", "ai", "ai-enablement", "analytics", "anomaly-detection",
    "api", "api-design", "api-documentation", "api-security", "as9100", "asic",
    "authentication", "automation", "backbone", "bgp", "bioinformatics", "blockchain",
    "bpmn", "business-development", "cad", "can-bus", "candidate-experience", "causal-inference",
    "ccna", "ccpa", "certified-scrummaster", "churn-prevention", "ci-cd", "cism",
    "cissp", "cli", "cloud", "cloud-native", "cloud-security", "cmdb",
    "cmmc", "cmos", "cnc", "cold-outreach", "community-management", "computer-vision",
    "container-security", "containerization", "content-marketing", "contract-lifecycle-management", "contract-negotiation", "conversational-ai",
    "copywriting", "cpq", "crm", "cryptocurrency", "cryptography", "customer-health-score",
    "customer-onboarding", "customer-retention", "cybersecurity", "dast", "data-analytics", "data-engineering",
    "data-governance", "data-ingestion", "data-lake", "data-lineage", "data-mining", "data-modeling",
    "data-pipelines", "data-quality", "data-science", "data-visualization", "data-warehousing", "deep-learning",
    "defi", "demand-generation", "design-patterns", "design-thinking", "developer-experience", "devops",
    "devsecops", "dfars", "dhcp", "dimensional-modeling", "distributed-computing", "distributed-systems",
    "distributed-training", "dlp", "dns", "docs-as-code", "ecommerce", "edi",
    "edr", "elt", "email-marketing", "embedded-software", "embedded-systems", "embeddings",
    "employee-relations", "employer-branding", "erp", "ethernet", "etl", "event-driven-architecture",
    "evm", "fea", "feature-engineering", "fedramp", "fhir", "fiber",
    "financial-modeling", "financial-reporting", "fine-tuning", "finops", "fintech", "firewall",
    "firmware", "fpga", "game-development", "gap-analysis", "gd-t", "gdpr",
    "general-ledger", "generative-ai", "generative-engine-optimization", "gis", "gitops", "go-to-market",
    "help-desk", "hipaa", "hl7", "hmi", "hpc", "i2c",
    "iam", "image-processing", "influencer-marketing", "information-architecture", "infrastructure-as-code", "interaction-design",
    "intrusion-detection", "ipsec", "iso-13485", "iso-26262", "iso-27001", "iso-9001",
    "itar", "itil", "jwt", "kanban", "lakehouse", "ldap",
    "lead-generation", "legal-research", "lgpd", "lidar", "lifecycle-marketing", "llm",
    "llmops", "machine-learning", "mainframe", "marketing-automation", "master-data-management", "mdm",
    "metadata-management", "microcontrollers", "microservices", "mlops", "modbus", "model-deployment",
    "model-evaluation", "mpls", "multithreading", "mvc", "network-security", "networking",
    "neural-networks", "nist", "nlp", "nosql", "oauth", "observability",
    "oop", "openid", "oscilloscope", "osint", "ospf", "owasp",
    "paid-social", "pci-dss", "penetration-testing", "people-analytics", "performance-management", "pipeline-management",
    "pki", "plc", "pmp", "ppc", "pre-sales", "predictive-analytics",
    "predictive-modeling", "process-improvement", "process-modeling", "professional-scrum-master", "program-management", "prompt-engineering",
    "proof-of-concept", "prototyping", "qos", "quantization", "rag", "rbac",
    "rdbms", "recommendation-systems", "regulatory-compliance", "reinforcement-learning", "requirements-elicitation", "requirements-gathering",
    "rest", "revenue-recognition", "reverse-engineering", "rlhf", "rpa", "rtos",
    "saas", "safe-agile", "sales-enablement", "sales-engineering", "saml", "sase",
    "sast", "scada", "scim", "scrum", "sd-wan", "sdlc",
    "sdn", "secrets-management", "secure-coding", "semantic-search", "sensor-fusion", "seo",
    "serverless", "service-desk", "sftp", "siem", "signal-processing", "six-sigma",
    "smart-contracts", "snmp", "soa", "soc-2", "solution-design", "ssh",
    "ssl", "sso", "stakeholder-management", "statistics", "storyboarding", "strategic-planning",
    "succession-planning", "sysml", "talent-management", "talent-sourcing", "tcp-ip", "tdd",
    "technical-discovery", "technical-screening", "technical-seo", "technical-writing", "test-automation", "threat-hunting",
    "ticket-resolution", "time-series", "typography", "uart", "udp", "uml",
    "unit-testing", "usability-testing", "user-flows", "user-research", "user-stories", "uvm",
    "vector-databases", "vector-search", "vendor-management", "version-control", "virtualization", "vlan",
    "voip", "vpc", "vpn", "vulnerability-scanning", "waf", "wan",
    "wcag", "web3", "webhooks", "websockets", "wifi", "wireframing",
    "workflow-orchestration", "xdr", "zero-trust",
)
df_skills["skill_type"] = "tool"
df_skills.loc[df_skills["skill_name"].isin(CONCEPTS), "skill_type"] = "concept"
df_skills.loc[df_skills["skill_name"] == "noskill", "skill_type"] = "none"
print("\nTypes de compétences :")
print(df_skills["skill_type"].value_counts())

# Mode incremental : les compétences déjà connues gardent leur Id_skills,
# seules les compétences nouvelles reçoivent les identifiants suivants.
if LOAD_MODE == "incremental":
    df_skills_known = pd.read_sql("SELECT * FROM skills", con=db__silver)
    known_names = set(df_skills_known["skill_name"])
    df_skills_added = df_skills[~df_skills["skill_name"].isin(known_names)].copy()
    max_skill_id = int(df_skills_known["Id_skills"].max())
    df_skills_added["Id_skills"] = range(
        max_skill_id + 1, max_skill_id + 1 + len(df_skills_added)
    )
    skill_columns = ["Id_skills", "skill_name", "skill_type"]
    df_skills = pd.concat(
        [df_skills_known[skill_columns], df_skills_added[skill_columns]],
        ignore_index=True,
    )
    print("Compétences nouvelles ajoutées :", len(df_skills_added))
else:
    df_skills_added = df_skills

df_bridge = pd.merge(
    df_exploded[["ID", "skills"]],
    df_skills,
    left_on="skills",
    right_on="skill_name",
    how="left",
)
df_bridge = df_bridge.rename(columns={"ID": "job_id"})
df_bridge = df_bridge[["Id_skills", "job_id"]].reset_index(drop=True)
if LOAD_MODE == "incremental":
    # Les lignes pont des offres modifiées sont supprimées avant l'écriture :
    # on ne repart donc que du plus grand Id_bridge des autres offres.
    bridge_start = int(
        pd.read_sql("SELECT MAX(Id_bridge) AS max_id FROM bridge", con=db__silver)[
            "max_id"
        ].iloc[0]
    )
else:
    bridge_start = 0
df_bridge.insert(0, "Id_bridge", range(bridge_start + 1, bridge_start + 1 + len(df_bridge)))

print("\n--- Contrôle des compétences individuelles et de la table pont ---")
print("Compétences distinctes (skills) :", len(df_skills))
print("Lignes de la table pont (bridge) :", len(df_bridge))
print("Offres distinctes dans la bridge :", df_bridge["job_id"].nunique())
print("Offres sans compétence (noskill) :",
      (df_exploded["skills"] == "noskill").sum())
print("Id_skills manquants dans la bridge :", df_bridge["Id_skills"].isna().sum())
print("\nTop 10 des compétences (hors noskill) :")
df_top = pd.merge(df_bridge, df_skills, on="Id_skills", how="left")
df_top = df_top[df_top["skill_name"] != "noskill"]
print(df_top["skill_name"].value_counts().head(10))


# =========================================================================
# 11. TRAITEMENT DES SALAIRES (enrichment.salary_min & enrichment.salary_max)
# =========================================================================
def process_salaries(row):
    min_val = row.get("enrichment.salary_min")
    max_val = row.get("enrichment.salary_max")

    def is_empty(v):
        if pd.isna(v):
            return True
        if str(v).strip().lower() in ["nan", "none", "", "null", "nd"]:
            return True
        return False

    min_empty = is_empty(min_val)
    max_empty = is_empty(max_val)

    if min_empty and max_empty:
        return None, None

    try:
        min_int = int(float(min_val)) if not min_empty else None
    except (ValueError, TypeError):
        min_int = None

    try:
        max_int = int(float(max_val)) if not max_empty else None
    except (ValueError, TypeError):
        max_int = None

    if min_int is not None and max_int is None:
        return min_int, min_int
    elif min_int is None and max_int is not None:
        return max_int, max_int
    else:
        return min_int, max_int


final_min_salaries = []
final_max_salaries = []
for idx, row in df_job.iterrows():
    s_min, s_max = process_salaries(row)
    final_min_salaries.append(s_min)
    final_max_salaries.append(s_max)

df_job["enrichment.salary_min"] = final_min_salaries
df_job["enrichment.salary_max"] = final_max_salaries


# =========================================================================
# 12. PÉRIODE DE SALAIRE : période de la source, seuils seulement si elle est vide
# =========================================================================
VALID_PERIODS = ["hour", "day", "month", "year"]

amount = df_job["enrichment.salary_min"].fillna(df_job["enrichment.salary_max"])

source_period = (
    df_job["enrichment.salary_period"].astype("string").str.strip().str.lower()
)
source_period = source_period.where(source_period.isin(VALID_PERIODS))

period_thresholds = pd.cut(
    amount,
    bins=[-np.inf, 500, 1600, 13000, np.inf],
    right=False,
    labels=VALID_PERIODS,
).astype(object)

# Backtest : les seuils retrouvent-ils la vraie période là où elle est connue ?
known = amount.notna() & source_period.notna()
print(pd.crosstab(source_period[known], period_thresholds[known]))
print("Précision des seuils :", (source_period[known] == period_thresholds[known]).mean())

df_job["enrichment.salary_period"] = (
    source_period.astype(object)
    .fillna(period_thresholds)
    .where(amount.notna(), "ND")
)


# =========================================================================
# 12 BIS. SALAIRE ANNUEL (mesure de Fact_job)
# =========================================================================
ANNUAL_FACTORS = {"hour": 2080, "day": 260, "month": 12, "year": 1}

factor = df_job["enrichment.salary_period"].map(ANNUAL_FACTORS)

df_job["salary_min_annual"] = (
    (df_job["enrichment.salary_min"] * factor).round().astype("Int64")
)
df_job["salary_max_annual"] = (
    (df_job["enrichment.salary_max"] * factor).round().astype("Int64")
)


# =========================================================================
# 12 TER. COLONNES DE RÉFÉRENCE DEMANDÉES PAR LE FORMATEUR
#   - salary_both_present : booléen, True si min ET max sont renseignés
#   - salary_ref_annual   : salaire annuel de référence
#       * min et max présents  -> moyenne des deux
#       * un seul présent      -> ce montant
#       * aucun                -> valeur manquante
# Calculé sur les montants annualisés pour que toutes les offres soient
# comparables (une moyenne entre un taux horaire et un taux annuel n'aurait
# pas de sens). Les montants restent dans la devise locale.
# =========================================================================
df_job["salary_both_present"] = (
    df_job["enrichment.salary_min"].notna()
    & df_job["enrichment.salary_max"].notna()
)

# mean(axis=1) ignore les valeurs manquantes : moyenne si 2 montants,
# le montant seul si un seul, NaN si aucun.
df_job["salary_ref_annual"] = (
    df_job[["salary_min_annual", "salary_max_annual"]]
    .astype("float64")
    .mean(axis=1)
    .round()
    .astype("Int64")
)

print("\n--- Contrôle des colonnes de référence ---")
print("salary_both_present :")
print(df_job["salary_both_present"].value_counts())
print("salary_ref_annual renseigné :", df_job["salary_ref_annual"].notna().sum())
print(
    "Cohérence (ref entre min et max sur offres complètes) :",
    (
        df_job.loc[df_job["salary_both_present"], "salary_ref_annual"]
        .between(
            df_job.loc[df_job["salary_both_present"], "salary_min_annual"],
            df_job.loc[df_job["salary_both_present"], "salary_max_annual"],
        )
        .mean()
    ),
)


# =========================================================================
# 13. TRAITEMENT DE enrichment.salary_currency BASÉ SUR LA COLONNE countries
# =========================================================================
country_currency_mapping = {
    "FR": "EUR", "BE": "EUR", "DE": "EUR", "NL": "EUR", "ES": "EUR", 
    "IT": "EUR", "AT": "EUR", "PT": "EUR", "IE": "EUR", "FI": "EUR", 
    "GR": "EUR", "LU": "EUR", "EE": "EUR", "LV": "EUR", "LT": "EUR", 
    "SK": "EUR", "SI": "EUR", "CY": "EUR", "MT": "EUR", "HR": "EUR",
    "US": "USD",
    "GB": "GBP",
    "CA": "CAD",
    "AU": "AUD",
    "CH": "CHF",
    "PL": "PLN",
    "CZ": "CZK",
    "DK": "DKK",
    "SE": "SEK",
    "NO": "NOK",
    "NZ": "NZD",
    "SG": "SGD",
    "HK": "HKD",
    "AE": "AED",
    "BH": "BHD",
    "BR": "BRL",
    "IN": "INR",
    "MX": "MXN",
    "PH": "PHP",
    "SA": "SAR"
}

def determiner_salary_currency(row):
    # Règle 1 : Devise déjà fournie par la source
    curr_source = row.get("enrichment.salary_currency")
    if pd.notna(curr_source) and str(curr_source).strip().lower() not in ["nan", "none", "", "null", "nd"]:
        return str(curr_source).strip().upper()

    # Règle 2 : Pas de montant de salaire (salary_min et salary_max vides)
    s_min = row.get("enrichment.salary_min")
    s_max = row.get("enrichment.salary_max")
    if pd.isna(s_min) and pd.isna(s_max):
        return "ND"

    # Règle 3 : Pays à ND, ou code absent
    countries_str = row.get("countries")
    if not countries_str or str(countries_str).strip().upper() in ["ND", "NAN", "NONE", ""]:
        return "ND"

    # Extraction des codes pays (séparés par des virgules)
    countries_list = [p.strip().upper() for p in str(countries_str).split(",") if p.strip()]
    if not countries_list:
        return "ND"

    # Règle 6 prioritaire : Plusieurs pays qui contiennent les US -> USD
    if "US" in countries_list:
        return "USD"

    currencies_found = set()
    for p in countries_list:
        if p in country_currency_mapping:
            currencies_found.add(country_currency_mapping[p])
        else:
            return "ND"  # Code absent des références -> ND (Règle 3)

    if not currencies_found:
        return "ND"

    # Règle 4 : Un seul pays
    if len(countries_list) == 1:
        return list(currencies_found)[0]

    # Règle 5 : Plusieurs pays qui ont tous la même devise (ex: France, Belgique, Allemagne -> EUR)
    if len(currencies_found) == 1:
        return list(currencies_found)[0]

    # Règle 7 : Tous les autres cas mixtes -> ND
    return "ND"

salary_currencies = []
for idx, row in df_job.iterrows():
    salary_currencies.append(determiner_salary_currency(row))

df_job["enrichment.salary_currency"] = salary_currencies


# =========================================================================
# 13 BIS. SALAIRE DE RÉFÉRENCE EN DOLLARS (salary_in_usd)
# Les taux viennent de la table Silver taux_change (script 02a).
# Taux exprimé ainsi : 1 USD = rate_per_usd unités de la devise.
# Donc : montant en USD = montant en devise / taux  (une DIVISION).
# Un seul taux (celui de la mise à jour) est appliqué à toutes les offres.
# Devise "ND" ou absente de la table des taux -> salary_in_usd reste vide.
# Seul le montant annuel de référence (salary_ref_annual) est converti.
# =========================================================================
df_rates = pd.read_sql(
    "SELECT currency_code, rate_per_usd, rate_date FROM taux_change", con=db__silver
)
rates_dict = dict(zip(df_rates["currency_code"], df_rates["rate_per_usd"]))
print("\nDate des taux appliqués :", df_rates["rate_date"].max())
print("\n--- Conversion en dollars ---")
print("Taux utilisés pour la conversion :", len(rates_dict))

offer_rate = df_job["enrichment.salary_currency"].map(rates_dict)
df_job["salary_in_usd"] = (
    (df_job["salary_ref_annual"].astype("float64") / offer_rate)
    .round()
    .astype("Int64")
)

with_amount = df_job["salary_ref_annual"].notna()
print("Offres avec salary_ref_annual :", with_amount.sum())
print("  converties en USD :", df_job["salary_in_usd"].notna().sum())
without_rate = df_job.loc[with_amount & offer_rate.isna(), "enrichment.salary_currency"]
print("  sans taux (devise ND ou inconnue) :", len(without_rate))
print(without_rate.value_counts().head(10))


# =========================================================================
# 14. UN PAYS PAR LIGNE : DÉDOUBLEMENT DES OFFRES MULTI-PAYS (formateur)
# Une offre avec "FR, US" devient 2 lignes : une pour FR, une pour US.
#   - ID                : identifiant de l'offre (identique sur les lignes dupliquées)
#   - Id_job_country    : nouvel identifiant unique de la ligne (offre x pays)
#   - country_code      : un seul code pays par ligne ("ND" si inconnu)
# Placé APRÈS les compétences, les paniers et la devise : ces étapes
# travaillent sur des offres uniques et ne sont donc pas faussées.
# =========================================================================
offers_before_count = df_job["ID"].nunique()
rows_before_count = len(df_job)

df_job["country_code"] = df_job["countries"].astype("string").str.split(",")
df_job = df_job.explode("country_code", ignore_index=True)
df_job["country_code"] = (
    df_job["country_code"].astype("string").str.strip().str.upper()
    .replace("", pd.NA).fillna("ND")
)

# Corrections de codes absents de la table country (rest_countries) :
#   UK -> GB : code ISO officiel du Royaume-Uni
#   EU -> ND : l'Union européenne n'est pas un pays (1 ligne concernée)
df_job["country_code"] = df_job["country_code"].replace({"UK": "GB", "EU": "ND"})

# Dédoublonnage (ex. une offre listant UK et GB) puis suppression d'un "ND"
# parasite quand l'offre a aussi un vrai pays
df_job = df_job.drop_duplicates(subset=["ID", "country_code"])
df_job["is_real_country"] = df_job["country_code"] != "ND"
df_job["has_real_country"] = df_job.groupby("ID")["is_real_country"].transform("max")
df_job = df_job[df_job["is_real_country"] | ~df_job["has_real_country"]]
df_job = df_job.drop(columns=["is_real_country", "has_real_country"]).reset_index(drop=True)
if LOAD_MODE == "full":
    df_job.insert(1, "Id_job_country", range(1, len(df_job) + 1))
else:
    # Une paire (offre, pays) déjà connue garde son Id_job_country ;
    # une paire nouvelle prend le numéro suivant.
    df_keys = pd.read_sql(
        "SELECT ID, country_code, Id_job_country FROM jobs_freehire", con=db__silver
    )
    max_job_country_id = int(df_keys["Id_job_country"].max())
    df_job = pd.merge(df_job, df_keys, on=["ID", "country_code"], how="left")
    missing_key = df_job["Id_job_country"].isna()
    df_job.loc[missing_key, "Id_job_country"] = range(
        max_job_country_id + 1, max_job_country_id + 1 + int(missing_key.sum())
    )
    df_job["Id_job_country"] = df_job["Id_job_country"].astype("int64")
    key_column = df_job.pop("Id_job_country")
    df_job.insert(1, "Id_job_country", key_column)
df_job = df_job.drop(columns=["countries"])

print("\n--- Contrôle du dédoublement par pays ---")
print("Offres uniques (ID) avant / après :", offers_before_count, "/", df_job["ID"].nunique())
print("Lignes avant / après :", rows_before_count, "/", len(df_job))
print("Lignes ajoutées par le dédoublement :", len(df_job) - rows_before_count)
print("Id_job_country unique :", df_job["Id_job_country"].is_unique)
print("Lignes avec country_code = ND :", (df_job["country_code"] == "ND").sum())
print("Nombre de pays distincts :", df_job["country_code"].nunique())


# =========================================================================
# 15. DATES : CONVERSION EN VRAIES DATES + ANNÉE ET MOIS (Dim_date)
# Les dates arrivent en texte ("2026-08-13T21:34:59Z"). On les convertit en
# date-heure UTC sans fuseau (naïve) : SQL Server les stocke en datetime2.
# Date de référence de l'offre : posted_at (date de publication).
# Aucun filtre ici : les offres anciennes (2012-2024) sont conservées,
# la fenêtre d'analyse se choisit dans le Gold / Power BI.
# =========================================================================
DATE_COLUMNS = ["posted_at", "created_at", "updated_at", "last_seen_at"]

print("\n--- Conversion des dates ---")
for column in DATE_COLUMNS:
    before = df_job[column].notna().sum()
    df_job[column] = pd.to_datetime(df_job[column], errors="coerce", utc=True)
    df_job[column] = df_job[column].dt.tz_convert(None)
    after = df_job[column].notna().sum()
    print(column, ": renseignées avant / après conversion :", before, "/", after)

# Colonne unique year_month : entier à 6 chiffres AAAAMM (ex. 202608 = août 2026).
# Année = year_month // 100 ; mois = year_month % 100 (utile pour Dim_date).
publication_year = df_job["posted_at"].dt.year.astype("Int64")
publication_month = df_job["posted_at"].dt.month.astype("Int64")
df_job["year_month"] = publication_year * 100 + publication_month

print("posted_at min / max :", df_job["posted_at"].min(), "/", df_job["posted_at"].max())
print("year_month : type", df_job["year_month"].dtype,
      "| manquants :", df_job["year_month"].isna().sum(),
      "| min / max :", df_job["year_month"].min(), "/", df_job["year_month"].max())
print("\nOffres uniques par année de publication :")
print(df_job.groupby(publication_year)["ID"].nunique())
print("\nOffres uniques par year_month (2026) :")
print(df_job[publication_year == 2026].groupby("year_month")["ID"].nunique())


# =========================================================================
# NETTOYAGE FINAL SPÉCIFIQUE À LA BASE SILVER (Suppression des colonnes inutiles)
# =========================================================================
silver_columns_to_drop = [
    "company_slug",
    "collections",
    'manually_added',
    "is_tech",
    "closed_at",
    'enrichment.education_level',
    'enrichment.seniority',
    'enrichment.experience_years_min',
    "enriched_at",
    "enrichment_version",
    "view_count",
    "applied_count",
    "upvote_count",
    "downvote_count",
    "my_vote",
    "reality.class",
    "reality.age_days",
    "reality.repost_count",
    "reality.mass_posting_count",
    "reality.fake_freshness",
    "work_mode",
    "enrichment.relocation",
    "enrichment.employment_type",
    "enrichment.english_level",
    "enrichment.visa_sponsorship",
    "enrichment.timezone_note",
    "requires_clearance",
    "experience_min_origin",
    "education_priority",
    "education_origin",
    "seniority_origin",
    "skills_basket",
    "skills_basket_id",
    "requirement_basket_id",
    "requirements_required",
    "requirements_preferred",
]

df_job = df_job.drop(columns=silver_columns_to_drop, errors="ignore")

# Tous les montants en nombres entiers (Int64 : le vide reste possible).
# Les taux de change ne sont pas des montants : ils restent décimaux (Bronze).
AMOUNT_COLUMNS = [
    "enrichment.salary_min",
    "enrichment.salary_max",
    "salary_min_annual",
    "salary_max_annual",
    "salary_ref_annual",
    "salary_in_usd",
]
for col in AMOUNT_COLUMNS:
    df_job[col] = pd.to_numeric(df_job[col], errors="coerce").round().astype("Int64")
print("\nMontants en entiers :")
print(df_job[AMOUNT_COLUMNS].dtypes)


print("\n--- ÉCRITURE DANS LA BASE TFE_Silver ---")
# =========================================================================
# ÉCRITURE DANS LA BASE TFE_Silver
# =========================================================================
# Date de chargement dans le Silver : le Gold s'en sert pour ne reprendre
# que les lignes arrivées depuis son dernier chargement.
df_job["silver_loaded_at"] = pd.Timestamp.now().floor("s")

if LOAD_MODE == "full":
    df_job.to_sql("jobs_freehire", con=db__silver, if_exists="replace", index=False)
    df_skills.to_sql("skills", con=db__silver, if_exists="replace", index=False)
    df_bridge.to_sql("bridge", con=db__silver, if_exists="replace", index=False)
else:
    # Tout dans une transaction : si une étape échoue, rien n'est écrit.
    with db__silver.begin() as connection:
        for start in range(0, len(modified_ids), 500):
            id_list = ",".join(str(x) for x in modified_ids[start:start + 500])
            connection.execute(text("DELETE FROM bridge WHERE job_id IN (" + id_list + ")"))
            connection.execute(text("DELETE FROM jobs_freehire WHERE ID IN (" + id_list + ")"))
        df_job.to_sql("jobs_freehire", con=connection, if_exists="append", index=False)
        df_skills_added[["Id_skills", "skill_name", "skill_type"]].to_sql(
            "skills", con=connection, if_exists="append", index=False
        )
        df_bridge.to_sql("bridge", con=connection, if_exists="append", index=False)
    print("Offres modifiées remplacées :", len(modified_ids))
    print("Lignes (offre x pays) ajoutées :", len(df_job))
    print("Compétences nouvelles :", len(df_skills_added))
    print("Lignes pont ajoutées :", len(df_bridge))
# dim_requirements_paniers n'est plus écrite (abandonnée avec le modèle 1).

print("Écriture dans silver réussie avec succès !")