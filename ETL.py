import pandas as pd
import requests
import numpy as np
import io
from sys import argv

class ETL:
    def __init__(self, url):
        self.url = url

    def getData(self) -> pd.DataFrame:
        """
        This method is used to get data from the url
        args: None
        return: pd.DataFrame
        """
        response = requests.get(self.url)
        try:
            if response.status_code == requests.codes.ok:
                data = io.StringIO(response.text)
                print("Data gathered successfully")
                data = pd.read_csv(data)
                return data
        except Exception as e:
            raise print(e)
        
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        - This method is used to transform the data
        - clean the data
        - calculate the interquartile range for each column and eliminate outliers
        - create age groups
        args: pd.DataFrame
        return: pd.DataFrame
        """
        print("Data transformation started")
            ## Verify that there are no null values
        null_values = data.isnull().sum()
        if null_values.sum() > 0:
            data.dropna(inplace=True)
            print("The null values has been deleted")

        # Verify that there are no repeated values
        repeated_values = data.duplicated().sum()
        if repeated_values > 0:
            data.drop_duplicates(inplace=True)
            print("The repeated values has been deleted")

        # Calculathe interquartile range for each column
        # ELiminate outliers
        for column in data.columns:
            Q1 = data[column].quantile(0.25)
            Q3 = data[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            data = data.query(f"{column} >= {lower_bound} and {column} <= {upper_bound}")
        print("Outliers eliminated")

        Child = data.query('age <= 12')
        teen = data.query('age > 12 and age <= 19')
        adult_young = data.query('age > 19 and age <= 39')
        adult = data.query('age > 39 and age < 60')
        old = data.query('age >= 60')

        # Create a new column
        data["age_group"] = np.nan
        data.loc[Child.index, "age_group"] = "Child"
        data.loc[teen.index, "age_group"] = "Teen"
        data.loc[adult_young.index, "age_group"] = "Adult_Young"
        data.loc[adult.index, "age_group"] = "Adult"
        data.loc[old.index, "age_group"] = "Old"
        print("Age groups created")
        print("Data transformation completed")


        return data
    
    def load_data(self, data: pd.DataFrame) -> None:
        """
        This method is used to load the data into a csv file
        args: pd.DataFrame
        return: None
        """
        data.to_csv("heart_failure_clean.csv", index=False)
        print("Data loaded successfully, file saved as heart_failure_clean.csv")
        print("ETL process completed")
        
url = argv[1]       
etl = ETL(url=url)
data = etl.getData()
data_clean = etl.transform_data(data)
etl.load_data(data_clean)