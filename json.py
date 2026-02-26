import os 
import pandas as pd 
import json 
import threading

class JSON_ETL:
    def __init__(self, filename):
        self.filename = filename
        self.df = None

    def extract(self):
        try:
            if not os.path.exists(self.filename):
                print("File Not Found")
                return
            with open(self.filename, "r") as file:
                data = json.load(file)

            self.df = pd.json_normalize(data)
            print("Data Extracted Successfully")

        except Exception as err:
            print("Error in Extract:", err)

    def transform(self):
        try:
            if self.df is None:
                print("No Data To Transform")
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

            print("Data Transformed Successfully")

        except Exception as err:
            print("Error In Transform", err)

    def load(self, output_file):
        try:
            if self.df is None:
                print("No Data To Load")
                return
            self.df.to_csv(output_file, index=False)
            print("Data Loaded Successfully")

        except Exception as err:
            print("Error In Load:", err)

    def run(self):
        t1 = threading.Thread(target=self.extract)
        t2 = threading.Thread(target=self.transform)
        t3 = threading.Thread(target=self.load, args=("employees-cleaned.csv",))

        t1.start()
        t1.join()

        t2.start()
        t2.join()

        t3.start()
        t3.join()

if __name__ == "__main__":
    file_path = "C:/Users/Admin/pandas_project/Rest API/employees.json"
    etl = JSON_ETL(file_path)
    etl.run()
