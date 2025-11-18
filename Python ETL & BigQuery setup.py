#python ETL and bigquery connection
!pip install google-cloud-bigquery pandas pyarrow db-dtypes
from google.colab import auth
auth.authenticate_user()
print("Authenticated.")
from google.cloud import bigquery
import pandas as pd

# PROJECT_ID = "sfo-lakehouse-226"
PROJECT_ID = "sfo-lakehouse-226-478502"
client = bigquery.Client(project=PROJECT_ID)
print("Connected to:", client.project)

datasets = ["sfo_raw", "sfo_core", "sfo_marts"]

for ds in datasets:
    dataset_id = f"{PROJECT_ID}.{ds}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    try:
        client.create_dataset(dataset)
        print("Created:", dataset_id)
    except Exception:
        print("Already exists:", dataset_id)
		
		
import io

df_passenger = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/Air_Traffic_Passenger_Statistics.csv")
df_landings  = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/Air_Traffic_Landings_Statistics.csv")

print("Passenger shape:", df_passenger.shape)
print("Landings shape :", df_landings.shape)
df_passenger.head()


df_passenger["Activity Period"] = df_passenger["Activity Period"].astype("int64")
df_landings["Activity Period"]  = df_landings["Activity Period"].astype("int64")
print("Converted Activity Period to int64")

from google.cloud import bigquery

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

table_pass = f"{PROJECT_ID}.sfo_raw.passenger_raw"
table_land = f"{PROJECT_ID}.sfo_raw.landings_raw"

job = client.load_table_from_dataframe(df_passenger, table_pass, job_config=job_config)
job.result()
print("Loaded:", table_pass)

job = client.load_table_from_dataframe(df_landings, table_land, job_config=job_config)
job.result()
print("Loaded:", table_land)

for t in ["passenger_raw", "landings_raw"]:
    q = f"SELECT COUNT(*) AS row_count FROM `{PROJECT_ID}.sfo_raw.{t}`"
    print(t, client.query(q).to_dataframe().iloc[0]["row_count"])


#**Analysis**
from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt

PROJECT_ID = "sfo-lakehouse-226-478502"
client = bigquery.Client(project = PROJECT_ID)
query = """
SELECT
  date_id,
  year,
  month,
  operating_airline,
  passengers_per_landing
FROM `sfo-lakehouse-226-478502.sfo_marts.mart_passengers_per_landing`
WHERE operating_airline = 'UNITED AIRLINES'
ORDER BY date_id
"""

df = client.query(query).to_dataframe()

df["year_month"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str) + "-01")
plt.figure(figsize=(12, 8))
plt.plot(df["year_month"], df["passengers_per_landing"])
plt.title("Passengers per Landing - UNITED AIRLINES")
plt.xlabel("Month")
plt.ylabel("Passengers per Landing")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('Passengers per Landing - UNITED AIRLINES.png')
plt.show()





