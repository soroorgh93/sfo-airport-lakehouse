# Installing this package to fetch data from an API or web services
# Loading two csv files into panda dataframe 
import pandas as pd
import requests
df_passenger = pd.read_csv('Air_Traffic_Passenger_Statistics.csv')
df_landings  = pd.read_csv('Air_Traffic_Landings_Statistics.csv')

print("Passenger shape:", df_passenger.shape)
print("Landings shape :", df_landings.shape)
df_passenger.head()

# creating small sample 
df_passenger_sample = df_passenger.head(500).copy()
df_landings_sample  = df_landings.head(500).copy()

# setting CouchDB connection URL and checking 
COUCH_URL = "http://admin:admin@localhost:5984"
DB_PASSENGER = "sfo_passenger_docs"
DB_LANDINGS  = "sfo_landings_docs"

# Quick connectivity test
r = requests.get(COUCH_URL)
print(r.status_code, r.json())
# creating couchdb database 
def create_db_if_not_exists(db_name):
    r = requests.put(f"{COUCH_URL}/{db_name}")
    if r.status_code in (201, 202):
        print("Created DB:", db_name)
    elif r.status_code == 412:
        print("DB already exists:", db_name)
    else:
        print("Error creating DB:", db_name, r.status_code, r.text)

create_db_if_not_exists(DB_PASSENGER)
create_db_if_not_exists(DB_LANDINGS)
# inserting data to couchDB 
import numpy as np
import pandas as pd

def df_to_couch_batched(df, db_name, id_col=None, batch_size=5000):
    n_rows = len(df)
    print(f"Total rows to send: {n_rows}")

    for start in range(0, n_rows, batch_size):
        end = min(start + batch_size, n_rows)
        df_batch = df.iloc[start:end].copy()

        df_batch = df_batch.replace([np.inf, -np.inf], np.nan)      # inf → NaN
        df_batch = df_batch.where(pd.notnull(df_batch), None)      # NaN → None (JSON null)

        records = df_batch.to_dict(orient="records")

        bulk_docs = {"docs": []}
        for rec in records:
            doc = dict(rec)
            if id_col and id_col in doc:
                doc["_id"] = str(doc[id_col])
            bulk_docs["docs"].append(doc)

        url = f"{COUCH_URL}/{db_name}/_bulk_docs"
        r = requests.post(url, json=bulk_docs)
        print("Status:", r.status_code)

        if r.status_code >= 400:
            print(f"Error in batch {start}:{end}: {r.status_code}", r.text)
            break
        else:
            print(f"Inserted rows {start}:{end} into {db_name}")
			
df_to_couch_batched(df_passenger_sample, DB_PASSENGER)
df_to_couch_batched(df_landings_sample, DB_LANDINGS)
df_passenger["GEO Region"].value_counts().head(10)

COUCH_URL = "http://admin:admin@localhost:5984"
DB_PASSENGER = "sfo_passenger_docs"

#DB info 
info = requests.get(f"{COUCH_URL}/{DB_PASSENGER}").json()
print("doc_count:", info.get("doc_count"))

#first few sample
rows = requests.get(f"{COUCH_URL}/{DB_PASSENGER}/_all_docs?include_docs=true&limit=5").json()
for row in rows["rows"]:
    print("----")
    print(row["doc"].get("GEO Region"), row["doc"].get("Operating Airline"))
	
# for sending data from notebook to BigQuery 	
!pip install google-cloud-bigquery pandas pyarrow db-dtypes
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/Key/sfo-lakehouse-226-46bdf89c9bb1.json"

#connection with bigquery
from google.cloud import bigquery
import pandas as pd
import requests

PROJECT_ID = "sfo-lakehouse-226"

client = bigquery.Client(project=PROJECT_ID)
print("Connected to:", client.project)

#connection to couchDB
import requests
import pandas as pd

COUCHDB_BASE = "http://admin:admin@localhost:5984"
DB_NAME = "sfo_passenger_docs"

url = f"{COUCHDB_BASE}/{DB_NAME}/_all_docs?include_docs=true"

resp = requests.get(url)
resp.raise_for_status()

rows = resp.json()["rows"]
print("Total docs fetched from CouchDB:", len(rows))

#cleaning
docs = []

for r in rows:
    doc_id = r.get("id", "")
    doc = r.get("doc", {})

    #skipping design doc
    if doc_id.startswith("_design/"):
        continue

    #removing metafield
    doc.pop("_id", None)
    doc.pop("_rev", None)
    
    doc.pop("views", None)
    doc.pop("language", None)

    docs.append(doc)

df_couch = pd.DataFrame(docs)
print(df_couch.shape)
df_couch.head()

# upload cleaned CouchDB data into BigQuery 
table_id = f"{PROJECT_ID}.sfo_raw.passenger_from_couch"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"
)

load_job = client.load_table_from_dataframe(
    df_couch,
    table_id,
    job_config=job_config,
)

load_job.result()


print("Loaded rows:", df_couch.shape[0], "into", table_id)
