"""
CONFIGURATION DE LA CONNEXION SQL SERVER (commune à tous les scripts).

Le nom du serveur n'est jamais écrit dans le code : il est lu dans la variable
d'environnement SQL_SERVER (même principe que la clé RESTCOUNTRIES_API_KEY).

Exemple sous Windows (PowerShell) :
    $env:SQL_SERVER = "NOM-DE-TON-PC"          # pour la session en cours
    setx SQL_SERVER "NOM-DE-TON-PC"            # de façon permanente

Connexion par authentification Windows (trusted_connection) : aucun mot de passe.
"""
import os

ODBC_DRIVER = "ODBC+Driver+17+for+SQL+Server"


def build_connection_url(database):
    """Construit l'URL SQLAlchemy vers une base (TFE_Bronze, TFE_Silver ou TFE_Gold)."""
    server = os.environ.get("SQL_SERVER")
    if not server:
        raise SystemExit(
            "Variable d'environnement SQL_SERVER absente : "
            "indique le nom de ton serveur SQL Server (voir db_config.py)."
        )
    return (
        "mssql+pyodbc://@" + server + "/" + database
        + "?trusted_connection=yes&driver=" + ODBC_DRIVER
    )
