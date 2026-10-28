import pandas as pd
import requests
from sqlalchemy import create_engine
from db_config import build_connection_url

# Bronze : taux de change (ExchangeRate-API, endpoint gratuit open.er-api.com).
# Base USD : 1 USD = Taux unités de la devise. Le Silver convertit donc un
# montant en dollars par une DIVISION : montant_usd = montant / Taux.
URL = "https://open.er-api.com/v6/latest/USD"

conn_string = build_connection_url("TFE_Bronze")
source_engine = create_engine(conn_string)


def fetch_and_save_rates():
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erreur de connexion à l'API : {e}")
        return None

    if data.get("result") != "success":
        print("Erreur : L'API n'a pas renvoyé un statut de succès.")
        return None

    df = pd.DataFrame(list(data["rates"].items()), columns=["Devise", "Taux"])
    df["Base"] = data["base_code"]
    df["Mise_a_jour_UTC"] = data["time_last_update_utc"]
    df = df.drop_duplicates(subset=["Devise"]).reset_index(drop=True)
    df = df[["Base", "Devise", "Taux", "Mise_a_jour_UTC"]]

    # Copie de sauvegarde en fichier plat
    df.to_csv("taux_change_usd.csv", index=False, encoding="utf-8")

    # Chargement en Bronze : photo des taux du jour, remplacée à chaque appel
    df.to_sql("taux_change", con=source_engine, if_exists="replace", index=False)

    print(f"{len(df)} taux enregistrés (mise à jour : {data['time_last_update_utc']})")
    return df


if __name__ == "__main__":
    fetch_and_save_rates()
