#The code here will pull in the connections to the API and leverage the ETL modules in the src directory.
from src.main import compile_ETL_data
from config import api_keys, db_credentials
from src.db.connection import psql_connect_and_setup


#this file will be the entry point for the entire application!
#here we need to define the tickers we will use so that we can just pass them into the pipeline.


def main() -> None:
    etl_data = compile_ETL_data(api_keys["finnhub"], db_credentials, tickers=['META', 'TSLA'], time_period='ytd')
    print("ETL Data Compiled:", etl_data.head())



if __name__ == "__main__":
    main()