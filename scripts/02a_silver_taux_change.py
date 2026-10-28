import pandas as pd
from sqlalchemy import create_engine
from db_config import build_connection_url

# Silver : taux de change.
# Bronze (taux_change) : taux bruts de l'API ExchangeRate-API, jamais modifiés.
# Silver (taux_change) : table propre :
#   currency_code : code devise en majuscules, sans espaces, sans doublon
#   rate_per_usd  : taux en nombre décimal (1 USD = rate_per_usd unités)
#   base_currency : devise de base (USD)
#   rate_date     : date-heure de mise à jour en vraie date (UTC, sans fuseau)
# À lancer AVANT le Silver des offres (02b), qui lit cette table pour convertir
# les salaires en dollars.

db_bronze = create_engine(
    build_connection_url("TFE_Bronze")
)
db__silver = create_engine(
    build_connection_url("TFE_Silver")
)

df_rates_bronze = pd.read_sql(
    "SELECT Base, Devise, Taux, Mise_a_jour_UTC FROM taux_change", con=db_bronze
)

df_rates = pd.DataFrame(
    {
        "currency_code": df_rates_bronze["Devise"].astype("string").str.strip().str.upper(),
        "rate_per_usd": pd.to_numeric(df_rates_bronze["Taux"], errors="coerce"),
        "base_currency": df_rates_bronze["Base"].astype("string").str.strip().str.upper(),
        "rate_date": pd.to_datetime(
            df_rates_bronze["Mise_a_jour_UTC"], utc=True, errors="coerce"
        ).dt.tz_localize(None),
    }
)

rates_before = len(df_rates)
df_rates = df_rates.dropna(subset=["currency_code", "rate_per_usd"])
df_rates = df_rates[df_rates["rate_per_usd"] > 0]
df_rates = df_rates.drop_duplicates(subset=["currency_code"])
df_rates = df_rates.sort_values(by="currency_code").reset_index(drop=True)

print("--- Silver des taux de change ---")
print("Taux Bronze :", rates_before, "| taux valides Silver :", len(df_rates))
print("Date de mise à jour :", df_rates["rate_date"].min(), "->", df_rates["rate_date"].max())
print("Dates illisibles :", df_rates["rate_date"].isna().sum())
print("Taux du dollar (doit valoir 1) :",
      df_rates.loc[df_rates["currency_code"] == "USD", "rate_per_usd"].tolist())

df_rates.to_sql("taux_change", con=db__silver, if_exists="replace", index=False)
print("Table Silver taux_change écrite :", len(df_rates), "lignes")
