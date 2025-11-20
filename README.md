# sfo-airport-lakehouse
SFO passenger & landings lakehouse
# Airport Operations Lakehouse for SFO 


**Title:** Airport Operations Lakehouse for SFO: Passenger & Landings Analytics (1999–2025)  

## Project idea

We combine two public SFO datasets; Air Traffic Passenger Statistics and Air Traffic Landings
Statistics into a reusable data warehouse and a small “lakehouse” in Google BigQuery.

The warehouse has:

- A shared star schema with conformed dimensions for date, airline, geography, terminal,
  activity type, price category, and aircraft.
- Two monthly fact tables for passengers and landings.
- Four gold marts:
  - `mart_airline_mix_monthly`
  - `mart_terminal_load_monthly`
  - `mart_passengers_per_landing` (main KPI)
  - `mart_fleet_mix_monthly`

On top of BigQuery we use Python (Colab/Jupyter) for ETL and analytics, and **Apache CouchDB
as a NoSQL document store** for a slice of the passenger data.


## Contents

- `notebooks/` – Colab notebooks for:
  - CSV → BigQuery ETL 
  - Analytics and plots from the marts 
- `sql/` – BigQuery SQL scripts for:
  - Dimensions 
  - Facts
  - Marts 
  - SCD Type 1/2/3 demo tables 
- `diagrams/` – Mermaid / PNG diagrams for the pipeline and star schema.
- `couchdb/` – Example **NoSQL** map/reduce view definitions and a small ETL script for
  CouchDB ↔ BigQuery round-trip.

## How to run (high level)

1. Create a BigQuery project and datasets: `sfo_raw`, `sfo_core`, `sfo_marts`.
2. Run the ETL notebook to load the two CSV files into `sfo_raw.passenger_raw` and
   `sfo_raw.landings_raw`.
3. Execute the SQL scripts in `sql/` to build dimensions, facts, and marts.
4. Optionally, run the CouchDB script to write/read a subset of passenger data as JSON.
5. Use the analytics notebook to reproduce the charts in the report
   (airline mix, terminal load, passengers per landing, fleet mix).



   <img width="2542" height="1088" alt="image" src="https://github.com/user-attachments/assets/8e3c9472-ef26-4ca2-b7cb-d2cebf08000f" />

