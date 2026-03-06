import requests
import pandas as pd
import schedule
import time
from sqlalchemy import create_engine
import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    database="countriesdb",
    user="postgres",
    password="Ayush.zen123@",
    port="5432"
)

print("Connected Successfully")
conn.close()

class CountriesETL:

    def __init__(self):

        self.api_url = "https://restcountries.com/v3.1/all?fields=name,capital,region,population,area"
        self.csv_file = "C:/Users/Admin/pandas_project/Rest API/countries_data.csv"
        self.engine = create_engine(
            "postgresql+psycopg2://postgres:Ayush.zen123%40@127.0.0.1:5432/countriesdb"
        )

    def extract(self):
        try:
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()

            self.data = response.json()

            print("Data Extracted Successfully")

        except Exception as e:
            print("Extraction Error:", e)
            self.data = []


    def transform(self):
        try:
            records = []
            for country in self.data:

                record = {
                    "Country": country.get("name", {}).get("common", "Unknown"),
                    "Capital": country.get("capital", ["Unknown"])[0] if country.get("capital") else "Unknown",
                    "Region": country.get("region", "Unknown"),
                    "Population": country.get("population", 0),
                    "Area": country.get("area", 0)
                }

                records.append(record)

            self.df = pd.DataFrame(records)

            self.df.fillna(0, inplace=True)

            self.df["Population_Density"] = self.df.apply(
                lambda row: row["Population"] / row["Area"] if row["Area"] > 0 else 0,
                axis=1
            )

            self.df = self.df[self.df["Region"].isin(["Asia", "Europe"])]

            print("Data Transformed Successfully")

        except Exception as e:
            print("Transformation Error:", e)

    def save_csv(self):

        try:

            self.df.to_csv(self.csv_file, index=False)

            print("Data Saved to CSV")

        except Exception as e:

            print("CSV Saving Error:", e)

    def load_to_database(self):
        try:
            self.df.to_sql(
                "countries_stats",
                self.engine,
                if_exists="replace",
                index=False
            )

            print("Data Loaded to PostgreSQL Successfully")

        except Exception as e:

            print("Database Loading Error:", e)

    def run(self):
        self.extract()
        self.transform()
        self.save_csv()
        self.load_to_database()

        print("ETL Pipeline Finished")

if __name__ == "__main__":

    pipeline = CountriesETL()

    schedule.every(10).seconds.do(pipeline.run)

    print("Scheduler Started...")

    for i in range(12):
        schedule.run_pending()
        time.sleep(1)

    print("Scheduler Stopped")
