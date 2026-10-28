"""
ORCHESTRATION PREFECT : planification et suivi du pipeline Bronze -> Silver -> Gold.

Ce fichier ne contient aucune logique de données : il lance les scripts du
pipeline dans le bon ordre, relance une étape en cas d'échec, et Prefect
garde l'historique (durée, logs, état) de chaque exécution.

Deux flows :
  - tfe_incremental_pipeline  (tous les jours)  : 01a -> 02b incremental -> 04b
  - tfe_reference_refresh     (chaque dimanche) : 01b (pays), 02c (pays Silver), 01c (taux), 02a (taux Silver)

Utilisation (dans l'environnement prefect_env) :
  python 08_prefect_flow.py run          -> exécute une fois le pipeline incrémental maintenant
  python 08_prefect_flow.py run_reference -> exécute une fois le rafraîchissement des référentiels
  python 08_prefect_flow.py serve        -> planifie les deux flows (à laisser ouvert)

Les scripts du pipeline sont lancés avec le Python du dossier venv du projet
(celui où ils ont été validés) ; à défaut, avec le Python courant.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

from prefect import flow, get_run_logger, task

SCRIPTS_DIR = Path(__file__).resolve().parent
VENV_PYTHON = SCRIPTS_DIR.parent / "venv" / "Scripts" / "python.exe"
PIPELINE_PYTHON = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

DAILY_CRON = "0 6 * * *"        # tous les jours à 06:00
WEEKLY_CRON = "0 5 * * 0"       # chaque dimanche à 05:00
TIMEZONE = "Europe/Brussels"


@task(retries=2, retry_delay_seconds=30)
def run_script(label, script_name, arguments):
    """Lance un script du pipeline. Une erreur du script fait échouer la tâche
    (puis Prefect la relance, jusqu'à 2 fois)."""
    logger = get_run_logger()
    command = [PIPELINE_PYTHON, str(SCRIPTS_DIR / script_name)] + arguments
    logger.info("Début de l'étape : %s (%s)", label, " ".join(command))

    environment = dict(os.environ)
    environment["PYTHONIOENCODING"] = "utf-8"

    start = time.time()
    result = subprocess.run(
        command,
        cwd=str(SCRIPTS_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=environment,
    )
    seconds = round(time.time() - start, 1)

    # Dernières lignes du script dans les logs Prefect
    output_lines = result.stdout.strip().splitlines()
    for line in output_lines[-15:]:
        logger.info(line)

    if result.returncode != 0:
        error_lines = result.stderr.strip().splitlines()
        for line in error_lines[-15:]:
            logger.error(line)
        raise RuntimeError(label + " a échoué (code " + str(result.returncode) + ")")

    logger.info("Étape terminée : %s en %s s", label, seconds)
    return seconds


@flow(name="tfe_incremental_pipeline", log_prints=True)
def tfe_incremental_pipeline():
    """Pipeline quotidien : nouvelles offres du Bronze jusqu'au Gold."""
    bronze_seconds = run_script("Bronze freehire", "01a_bronze_freehire_api.py", [])
    silver_seconds = run_script("Silver freehire (incrémental)", "02b_silver_jobs_freehire.py", ["incremental"])
    gold_seconds = run_script("Gold (incrémental)", "04b_gold_chargement_incremental.py", [])
    total_seconds = round(bronze_seconds + silver_seconds + gold_seconds, 1)
    print("Durées : Bronze", bronze_seconds, "s | Silver", silver_seconds,
          "s | Gold", gold_seconds, "s | Total", total_seconds, "s")


@flow(name="tfe_reference_refresh", log_prints=True)
def tfe_reference_refresh():
    """Rafraîchissement hebdomadaire des référentiels : pays et taux de change."""
    run_script("Bronze pays", "01b_bronze_countries_api.py", [])
    run_script("Silver pays", "02c_silver_country.py", [])
    run_script("Bronze taux de change", "01c_bronze_taux_change_api.py", [])
    run_script("Silver taux de change", "02a_silver_taux_change.py", [])
    print("Référentiels rafraîchis (pays et taux de change).")


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else ""
    if action == "run":
        tfe_incremental_pipeline()
    elif action == "run_reference":
        tfe_reference_refresh()
    elif action == "serve":
        from prefect import serve
        from prefect.client.schemas.schedules import CronSchedule

        # Prefect 3 : le fuseau horaire se donne dans l'objet CronSchedule.
        incremental_deployment = tfe_incremental_pipeline.to_deployment(
            name="quotidien",
            schedule=CronSchedule(cron=DAILY_CRON, timezone=TIMEZONE),
        )
        reference_deployment = tfe_reference_refresh.to_deployment(
            name="hebdomadaire",
            schedule=CronSchedule(cron=WEEKLY_CRON, timezone=TIMEZONE),
        )
        serve(incremental_deployment, reference_deployment)
    else:
        raise SystemExit("Usage : python 08_prefect_flow.py run | run_reference | serve")
