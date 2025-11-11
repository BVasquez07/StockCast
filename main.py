#The code here will pull in the connections to the API and leverage the ETL modules in the src directory.
from src.main import compile_ETL_data
from config import api_keys, db_credentials
from src.db.connection import psql_connect_and_setup


#this file will be the entry point for the entire application!


def main() -> None:
    print(api_keys['finnhub'])
    etl_data = compile_ETL_data(api_keys["finnhub"], api_keys["yahoo_fin"])
    psql_connect_and_setup(
        db_host_addr=db_credentials['host'], 
        db_port=db_credentials['port'], 
        db_name=db_credentials['database'], 
        db_user=db_credentials['user'], 
        db_password=db_credentials['password'], 
        db_timeout=db_credentials['timeout'])

    print("ETL Data Compiled:", etl_data)



if __name__ == "__main__":
    main()