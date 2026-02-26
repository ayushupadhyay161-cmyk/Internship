# CSV-TO-CLEAN-CSV-BASIC-ETL-


import os 
import pandas as pd 
from datetime import date 

class EmployeeETL:
    def __init__(self, filename):
        self.filename = filename
        self.df = None 

    def extract(self):
        if not os.path.exists(self.filename):
            print("File not found!")

        self.df = pd.read_csv(self.filename)
        print("Data Extracted Successfully")


    def clean(self):
        self.df = self.df.drop_duplicates(subset=["Employee_ID"])
        self.df = self.df.dropna(subset=["Name", "DOB"])
        self.df["Salary"] = pd.to_numeric(self.df["Salary"], errors="coerce")
        self.df["Salary"] = self.df["Salary"].fillna(0)
        self.df["DOB"] = pd.to_datetime(
            self.df["DOB"],
            errors = "coerce",
            dayfirst = True
        )

        self.df["DOB"] = self.df["DOB"].dt.strftime("%Y-%m-%d")
        print("Data Cleaned Successfully")

    def transform(self):
        self.df["DOB"] = pd.to_datetime(self.df["DOB"])
        self.df["Age"] = date.today().year - self.df["DOB"].dt.year
        print("Age Column Added Successfully")

    def load(self):
        output_file = "employees_cleaned.csv"
        self.df.to_csv(output_file, index=False)
        print("Cleaned file saved as: ", output_file)

    def run(self):
        self.extract()
        self.clean()
        self.transform()
        self.load()

if __name__ == "__main__":
    file_path = r"C:\Users\Admin\pandas_project\project1\employees.csv"
    etl = EmployeeETL(file_path)
    etl.run()
