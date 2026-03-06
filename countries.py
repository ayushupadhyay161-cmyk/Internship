import requests
import pandas as pd
import sqlite3
import schedule
import time

class CountriesETL:

    def __init__(self):
        self.url = "https://restcountries.com/v3.1/all?fields=name,capital,region,population,area"
        self.csv_path = "C:/Users/Admin/pandas_project/Rest API/countries_data.csv"
        self.db_path = "C:/Users/Admin/pandas_project/Rest API/countries_data.db"

    def extract(self):
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            print("Extraction successful.")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Extraction failed: {e}")
            return []
        print("Data Extracted Successfully")

    def transform(self, data):
        if not data:
            print("No data available for transformation.")
            return None

        records = []

        for country in data:
            record = {
                "country": country.get("name", {}).get("common", "Unknown"),
                "capital": country.get("capital", ["Unknown"])[0] if country.get("capital") else "Unknown",
                "region": country.get("region", "Unknown"),
                "population": country.get("population", 0),
                "area": country.get("area", 0)
            }
            records.append(record)

        df = pd.DataFrame(records)

        df.fillna(0, inplace=True)

        df["population_density"] = df.apply(
            lambda row: row["population"] / row["area"] if row["area"] > 0 else 0,
            axis=1
        )

        df = df[df["region"].isin(["Asia", "Europe"])]

        print("Transformation completed.")
        return df

    def load(self, df):
        if df is None or df.empty:
            print("No data to load.")
            return

        df.to_csv(self.csv_path, index=False)
        print("Data saved to CSV.")

        conn = sqlite3.connect(self.db_path)
        df.to_sql("countries_stats", conn, if_exists="replace", index=False)
        conn.close()
        print("Data saved to SQLite database.")

        print("Loading completed.")

    def run(self):
        print(f"ETL Process Started!")

        data = self.extract()
        df = self.transform(data)
        self.load(df)

        print(f"ETL Process Completed!")

if __name__ == "__main__":

    pipeline = CountriesETL()

    schedule.every(10).seconds.do(pipeline.run)

    print("Scheduler started...")

    for i in range(12):
        schedule.run_pending()
        time.sleep(1)

    print("Scheduler stopped.")
