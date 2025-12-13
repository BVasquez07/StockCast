#The code here will pull in the connections to the API and leverage the ETL modules in the src directory.
import pandas as pd
from src.main import compile_ETL_data
from config import api_keys, db_credentials
from src.db.connection import psql_connect_and_setup


#this file will be the entry point for the entire application!
#here we need to define the tickers we will use so that we can just pass them into the pipeline.


def main() -> None:
    etl_data = compile_ETL_data(api_keys["finnhub"], db_credentials, tickers=['KOP', 'ENVX'], time_period='ytd')
    if type(etl_data) is pd.DataFrame:
        print("ETL Data Compiled:", etl_data.head())
    print("ETL Data Compiled:", etl_data)

if __name__ == "__main__":
    main()