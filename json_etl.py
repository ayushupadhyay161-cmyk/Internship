# JSON-EXTRACTION-ETL

import os 
import pandas as pd 
import json

class JSON_ETL:
    def __init__(self, filename):
        self.filename = filename 
        self.df = None

    def extract(self):
        if not os.path.exists(self.filename):
            print("File Not Found") 
            return 
        with open(self.filename, "r") as file:
            data = json.load(file)

        self.df = pd.json_normalize(data)      
        print("Extracted Data Successfully From JSON File")

    def transform(self):
        if self.df is None:
            print("No Data To Transform!")
            return
        
        self.df.rename(columns={
            "id": "Employee_ID",
            "name": "Employee_Name",
            "age": "Employee_Age",
            "department": "Department",
            "contact.email": "Email",
            "contact.phone": "Phone",
            "address.city": "City",
            "address.state": "State"
        }, inplace=True)

        self.df = self.df[
            ["Employee_ID", "Employee_Name", "Employee_Age", "Department", "Email", "Phone", "City", "State"]
        ]
        print("Data Transformed Successfully")


    def load(self, output_file):
        if self.df is None:
            print("No data to load!")
            return 
        self.df.to_csv(output_file, index=False)
        print(f"Data Loaded Successfully into {output_file}")

    def run(self):
        self.extract()
        self.transform()

        output_path = os.path.join(
            os.path.dirname(self.filename),
            "employees-cleaned.csv"
       )
        self.load(output_path)


if __name__ == "__main__":
    file_path = "C:/Users/Admin/pandas_project/Rest API/employees.json"
    etl = JSON_ETL(file_path)
    etl.run()
