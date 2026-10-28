"""
ORCHESTRATION DU PIPELINE : une seule commande pour enchaîner les étapes.

  python 07_run_pipeline.py incremental
      1. 01a_bronze_freehire_api.py            (nouvelles offres seulement -> TFE_Bronze)
      2. 02b_silver_jobs_freehire.py incremental (offres nouvelles ou modifiées -> TFE_Silver)
      3. 04b_gold_chargement_incremental.py    (insertion / mise à jour -> TFE_Gold)

  python 07_run_pipeline.py full
      1. 02c_silver_country.py
      2. 02d_silver_salary.py
      3. 02a_silver_taux_change.py
      4. 02b_silver_jobs_freehire.py full
      (puis, à la main dans SSMS : 03_gold_creation_tables.sql)
      5. 04_gold_chargement.py
      Prérequis du Bronze : 01a, 01b, 01c et 01d (salaries.csv) déjà exécutés.

Le chargement complet sert à reconstruire (premier chargement, reprise après
incident). L'incrémental sert au quotidien. Les taux de change et les pays ne
sont pas rafraîchis en incrémental (01b et 01c se lancent à part si besoin).
Chaque étape est chronométrée : ces durées alimenteront le monitoring.
"""
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

PIPELINES = {
    "incremental": [
        ("Bronze freehire", "01a_bronze_freehire_api.py", []),
        ("Silver freehire", "02b_silver_jobs_freehire.py", ["incremental"]),
        ("Gold incrémental", "04b_gold_chargement_incremental.py", []),
    ],
    "full": [
        ("Silver pays", "02c_silver_country.py", []),
        ("Silver salaires Kaggle", "02d_silver_salary.py", []),
        ("Silver taux de change", "02a_silver_taux_change.py", []),
        ("Silver freehire", "02b_silver_jobs_freehire.py", ["full"]),
        ("Gold complet", "04_gold_chargement.py", []),
    ],
}


def run_step(label, script_name, arguments):
    """Lance un script Python, mesure sa durée, s'arrête si une erreur survient."""
    print("\n" + "=" * 70)
    print("ÉTAPE :", label, "->", script_name, " ".join(arguments))
    print("=" * 70)
    start = time.time()
    subprocess.run(
        [sys.executable, str(SCRIPT_DIR / script_name)] + arguments,
        check=True,
    )
    return round(time.time() - start, 1)


mode = sys.argv[1] if len(sys.argv) > 1 else ""
if mode not in PIPELINES:
    raise SystemExit("Usage : python 07_run_pipeline.py incremental | full")

if mode == "full":
    print("RAPPEL : avant le Gold complet, 03_gold_creation_tables.sql doit avoir été exécuté dans SSMS.")

durations = []
for label, script_name, arguments in PIPELINES[mode]:
    seconds = run_step(label, script_name, arguments)
    durations.append((label, seconds))

print("\n" + "=" * 70)
print("RÉSUMÉ DES DURÉES (mode " + mode + ")")
print("=" * 70)
for label, seconds in durations:
    print(" ", label, ":", seconds, "s")
print("  Total :", round(sum(seconds for label, seconds in durations), 1), "s")
