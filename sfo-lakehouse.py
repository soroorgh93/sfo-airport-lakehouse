#**for sending data from notebook to BigQuery**
pip install google-cloud-bigquery pandas pyarrow db-dtypes
#**Login from Colab to GCP**
from google.colab import auth
auth.authenticate_user()
print("Authenticated.")
#**Connect to BigQuery project**
from google.cloud import bigquery
import pandas as pd

PROJECT_ID = "sfo-lakehouse-226"
client = bigquery.Client(project=PROJECT_ID)
print("Connected to:", client.project)
#**Create the schemas**
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

#**Load both CSV in pandas and check if data is correct**
import io

df_passenger = pd.read_csv("/Air_Traffic_Passenger_Statistics.csv")
df_landings  = pd.read_csv("/Air_Traffic_Landings_Statistics.csv")

print("Passenger shape:", df_passenger.shape)
print("Landings shape :", df_landings.shape)
df_passenger.head()

#fix datatype of activity period

df_passenger["Activity Period"] = df_passenger["Activity Period"].astype("int64")
df_landings["Activity Period"]  = df_landings["Activity Period"].astype("int64")
print("Converted Activity Period to int64")

#**Load data from pandas to BigQuery**
from google.cloud import bigquery

#create load job configuration
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

table_pass = f"{PROJECT_ID}.sfo_raw.passenger_raw"
table_land = f"{PROJECT_ID}.sfo_raw.landings_raw"

job = client.load_table_from_dataframe(df_passenger, table_pass, job_config=job_config)
job.result()
print("Loaded:", table_pass)

job = client.load_table_from_dataframe(df_landings, table_land, job_config=job_config)
job.result()
print("Loaded:", table_land)

#check row count in both table
for t in ["passenger_raw", "landings_raw"]:
  q = f"SELECT COUNT(*) AS `rows` FROM `{PROJECT_ID}.sfo_raw.{t}`"
  print(t, client.query(q).to_dataframe().iloc[0]["rows"])
  
#**Insights**
from google.colab import auth
auth.authenticate_user()

from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns  # Colab me already hota hai; nahi ho to: !pip install seaborn

sns.set(style="whitegrid")
PROJECT_ID = "sfo-lakehouse-226"
client = bigquery.Client(project=PROJECT_ID)

#**Airline Mix – Top 10 Airlines**
query_top10_airlines = """
SELECT
  REGEXP_REPLACE(operating_airline, r' - PRE .*', '') AS operating_airline,
  SUM(passengers_excl_transit) AS total_pax_excl_transit
FROM `sfo-lakehouse-226.sfo_marts.mart_airline_mix_monthly`
WHERE year = 2023
GROUP BY operating_airline
ORDER BY total_pax_excl_transit DESC
LIMIT 10
"""

df_air_top10 = client.query(query_top10_airlines).to_dataframe()
df_air_top10

df_plot = df_air_top10.copy()
df_plot["pax_millions"] = df_plot["total_pax_excl_transit"] / 1_000_000
df_plot = df_plot.sort_values("pax_millions", ascending=True).reset_index(drop=True)

plt.figure(figsize=(9, 5))
plt.barh(df_plot["operating_airline"], df_plot["pax_millions"])

plt.title("Top 10 Airlines by Passenger Volume at SFO (2023)", fontsize=14)
plt.xlabel("Passengers (millions, excl. thru/transit)", fontsize=12)
plt.ylabel("Operating Airline", fontsize=12)

for i, v in enumerate(df_plot["pax_millions"]):
    plt.text(v + 0.2, i, f"{v:.1f}M", va="center", fontsize=9)

plt.tight_layout()
plt.show()


#**Area that are busiest**
query_terminal_top10 = """
SELECT
  year,
  terminal,
  boarding_area,
  SUM(pax_excl_transit) AS total_pax_excl_transit
FROM `sfo-lakehouse-226.sfo_marts.mart_terminal_load_monthly`
WHERE year = 2023
  AND terminal IS NOT NULL
GROUP BY year, terminal, boarding_area
ORDER BY total_pax_excl_transit DESC
LIMIT 10
"""

df_term = client.query(query_terminal_top10).to_dataframe()
df_term

df_term_plot = df_term.copy()
df_term_plot["pax_millions"] = df_term_plot["total_pax_excl_transit"] / 1_000_000
df_term_plot["label"] = df_term_plot["terminal"] + " - " + df_term_plot["boarding_area"]

# sort small→big for neat bars
df_term_plot = df_term_plot.sort_values("pax_millions", ascending=True).reset_index(drop=True)

plt.figure(figsize=(9, 5))

colors = plt.cm.tab10(np.linspace(0, 1, len(df_term_plot)))
plt.barh(df_term_plot["label"], df_term_plot["pax_millions"], color=colors)

plt.title("Top 10 Terminal/Boarding Areas by Passenger Volume at SFO (2023)", fontsize=14)
plt.xlabel("Passengers (millions, excl. thru/transit)", fontsize=12)
plt.ylabel("Terminal - Boarding Area", fontsize=12)

xmax = df_term_plot["pax_millions"].max()
for i, v in enumerate(df_term_plot["pax_millions"]):
    plt.text(v + xmax * 0.02, i, f"{v:.1f}M", va="center", fontsize=9)

plt.tight_layout()
plt.show()

#**Main KPI – Passengers per Landing (Overall SFO)**
query_ppl_overall = """
SELECT
  year,
  month,
  date_id,
  SUM(passengers_excl_transit) AS total_pax_excl_transit,
  SUM(landings) AS total_landings,
  SAFE_DIVIDE(SUM(passengers_excl_transit), SUM(landings)) AS passengers_per_landing
FROM `sfo-lakehouse-226.sfo_marts.mart_passengers_per_landing`
GROUP BY year, month, date_id
ORDER BY date_id
"""

df_ppl_overall = client.query(query_ppl_overall).to_dataframe()
df_ppl_overall.head(), df_ppl_overall.shape
df_ppl_overall["year_month"] = pd.to_datetime(
    df_ppl_overall["year"].astype(str) + "-" +
    df_ppl_overall["month"].astype(str) + "-01"
)

plt.figure(figsize=(10, 5))
plt.plot(df_ppl_overall["year_month"],
         df_ppl_overall["passengers_per_landing"],
         marker="o")

plt.title("Passengers per Landing – Overall SFO", fontsize=14)
plt.xlabel("Month", fontsize=12)
plt.ylabel("Passengers per Landing (excl. thru/transit)", fontsize=12)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

#**Fleet Mix (Wide vs Narrow Body)**
query_fleet = """
SELECT
  year,
  month,
  body_bucket,
  total_landings
FROM `sfo-lakehouse-226.sfo_marts.mart_fleet_mix_monthly`
WHERE year = 2023
ORDER BY year, month, body_bucket
"""
df_fleet = client.query(query_fleet).to_dataframe()

print(df_fleet["body_bucket"].value_counts())
df_fleet.head()

df_trend = (
    df_fleet
    .groupby(["year", "month", "body_bucket"], as_index=False)["total_landings"]
    .sum()
    .pivot_table(
        index=["year", "month"],
        columns="body_bucket",
        values="total_landings",
        fill_value=0
    )
    .reset_index()
)

df_trend = df_trend.sort_values(["year", "month"])
df_trend["year_month"] = pd.to_datetime(
    df_trend["year"].astype(str) + "-" +
    df_trend["month"].astype(str) + "-01"
)

plt.figure(figsize=(10, 5))

for col in [c for c in ["WIDE-BODY", "NARROW-BODY", "OTHER"] if c in df_trend.columns]:
    plt.plot(df_trend["year_month"], df_trend[col], marker="o", label=col)

plt.title("Monthly Landings by Body Type at SFO (2023)")
plt.xlabel("Month")
plt.ylabel("Landings")
plt.xticks(df_trend["year_month"],
           df_trend["year_month"].dt.strftime("%Y-%m"),
           rotation=45)
plt.legend(title="Body Type")
plt.tight_layout()
plt.show()


#**To check couchdb is connected to BigQuery**
query_count = """
SELECT COUNT(*) AS row_count
FROM `sfo-lakehouse-226.sfo_raw.passenger_from_couch`
"""

df_count = client.query(query_count).to_dataframe()
df_count

#**Region wise Passenger**

query_couch_geo = """
SELECT
  `GEO Region` AS geo_region,
  SUM(CAST(`Passenger Count` AS INT64)) AS total_passengers
FROM `sfo-lakehouse-226.sfo_raw.passenger_from_couch`
GROUP BY
  geo_region
ORDER BY
  total_passengers DESC
"""

df_couch_geo = client.query(query_couch_geo).to_dataframe()
df_couch_geo


plt.figure(figsize=(8, 4))
plt.bar(df_couch_geo["geo_region"], df_couch_geo["total_passengers"] / 1_000_000, color=colors)
plt.xticks(rotation=45, ha="right")
plt.ylabel("Passengers (millions)")
plt.title("Passengers by GEO region (CouchDB → BigQuery table)")
plt.tight_layout()
plt.show()


#**YoY growth by region**
query_yoy_region = """
WITH yearly_region AS (
  SELECT
    year,
    geo_region,
    SUM(passengers_excl_transit) AS passengers_excl_transit
  FROM `sfo-lakehouse-226.sfo_marts.mart_airline_mix_monthly`
  GROUP BY
    year,
    geo_region
),
with_prev AS (
  SELECT
    year,
    geo_region,
    passengers_excl_transit,
    LAG(passengers_excl_transit) OVER (
      PARTITION BY geo_region
      ORDER BY year
    ) AS prev_year_pax
  FROM yearly_region
)
SELECT
  year,
  geo_region,
  passengers_excl_transit AS total_passengers,
  prev_year_pax,
  SAFE_DIVIDE(
    passengers_excl_transit - prev_year_pax,
    prev_year_pax
  ) AS yoy_growth_ratio
FROM with_prev
WHERE prev_year_pax IS NOT NULL
ORDER BY
  geo_region,
  year;
"""

df_yoy_region = client.query(query_yoy_region).to_dataframe()
df_yoy_region


#**Domestic vs International share**
query_dom_intl = """
WITH classified AS (
  SELECT
    year,
    CASE
      WHEN UPPER(geo_region) = 'NORTH AMERICA' THEN 'DOMESTIC'
      ELSE 'INTERNATIONAL'
    END AS market_type,
    passengers_excl_transit
  FROM `sfo-lakehouse-226.sfo_marts.mart_airline_mix_monthly`
)
, yearly_sums AS (
  SELECT
    year,
    market_type,
    SUM(passengers_excl_transit) AS total_passengers
  FROM classified
  GROUP BY
    year,
    market_type
),
with_share AS (
  SELECT
    year,
    market_type,
    total_passengers,
    SAFE_DIVIDE(
      total_passengers,
      SUM(total_passengers) OVER (PARTITION BY year)
    ) AS share_of_passengers
  FROM yearly_sums
)
SELECT
  year,
  market_type,
  total_passengers,
  share_of_passengers
FROM with_share
ORDER BY
  year,
  market_type;
"""

df_dom_intl = client.query(query_dom_intl).to_dataframe()
df_dom_intl


#**Bonus: Month-of-year seasonality by region**
query_seasonality = """
SELECT
  month,
  geo_region,
  AVG(monthly_pax) AS avg_monthly_passengers
FROM (
  SELECT
    year,
    month,
    geo_region,
    SUM(passengers_excl_transit) AS monthly_pax
  FROM `sfo-lakehouse-226.sfo_marts.mart_airline_mix_monthly`
  GROUP BY
    year,
    month,
    geo_region
)
GROUP BY
  month,
  geo_region
ORDER BY
  month,
  avg_monthly_passengers DESC;
"""

df_seasonality = client.query(query_seasonality).to_dataframe()
df_seasonality
