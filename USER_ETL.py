# Duplicate-Detection-ETL
import os 
import pandas as pd 
import threading

class USERS_ETL:
    def __init__(self,filename):
        self.filename = filename
        self.df  = None

    def extract(self):
        try:
            if not os.path.exists(self.filename):
                print("File Not Found")
                return
            self.df = pd.read_csv(self.filename)
            print("File Extracted Successfully")

        except Exception as e:
            print("Error in Extract", e)

    def transform(self):
        try:
            if self.df is None:
                print("No Data To Transform")
                return
            self.df["created_at"] = pd.to_datetime(
                self.df["created_at"],
                dayfirst=True,
                errors="coerce"
            )

            self.df = self.df.sort_values(
                by = "created_at",
                ascending=False
            )

            self.df = self.df.drop_duplicates(
                subset="email",
                keep="first"
            )

            print("Data Transformed Successfully")

        except Exception as e:
            print("Error In Transform", e)


    def load(self, output_path):
        try:
            if self.df is None:
                print("No Data To Load")
                return
            self.df.to_csv(output_path, index=False)
            print("Data Loaded Successfully")
        except Exception as e:
            print("Error In Load", e)

    def run(self):
        output_path = os.path.join(
            os.path.dirname(self.filename),
            "users_cleaned.csv"
        )

        t1 = threading.Thread(target=self.extract)
        t2 = threading.Thread(target=self.transform)
        t3 = threading.Thread(target=self.load, args=(output_path,))

        t1.start()
        t1.join()

        t2.start()
        t2.join()

        t3.start()
        t3.join()

if __name__ == "__main__":
    file_path = "C:/Users/Admin/pandas_project/Rest API/users.csv"
    etl = USERS_ETL(file_path)
    etl.run()
