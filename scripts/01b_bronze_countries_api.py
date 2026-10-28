import pandas as pd
from sqlalchemy import create_engine, text
from db_config import build_connection_url
import os
import requests



# La clé API ne doit JAMAIS être écrite dans le code : variable d'environnement RESTCOUNTRIES_API_KEY
api = os.environ["RESTCOUNTRIES_API_KEY"]
url = "https://api.restcountries.com/countries/v5"
headers = {'Authorization': f"Bearer {api}"} 

all_countries = []
offset = 0

while True:
    params = {'limit': 100, 'offset': offset, "response_fields": ["names.common", "codes.alpha_2","region", "subregion"]}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            objets = data['data']['objects']
            all_countries.extend(objets)
            
            if data['data']['meta']['more'] == False or not objets:
                break
            else:
                offset += 100
        else:
            print(f"Erreur HTTP {response.status_code}: {response.text}")
            break
            
    except requests.exceptions.RequestException as e:
        print(f"Erreur réseau survenue à la page {offset} : {e}")
        break

df = pd.json_normalize(all_countries)



df = df.dropna(subset=['codes.alpha_2'])
df = df.drop_duplicates(subset=['codes.alpha_2'])
df['iso_alpha2_clean'] = df['codes.alpha_2'].str.lower()
df = df[['region', 'subregion', 'names.common', 'iso_alpha2_clean']]


conn_string = build_connection_url("TFE_Bronze")
source_engine = create_engine(conn_string)
df.to_sql('dim_country', con=source_engine, if_exists='replace', index=False)