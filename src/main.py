from src.Extract.main import compile_extracted_data
from src.Transform.main import transform_extracted_data
from src.db.insertion import insert_data
from src.db.connection import psql_connect_and_setup
import pandas as pd
import psycopg
from typing import Dict, Union

"""
This file will serve as the main ETL orchestrator, 
calling the necessary functions from the Extract, Transform, and Load modules.
The data will then be passed up to the root main.py 
because that root main will be the entry point for the entire application!


TODO:
- error handling
    -> complete Monte_carlo submission of data to the ETL pipeline which should happen in the Transform stage
        -> insert the simulation data into the simulation table
    -> timeout handling
    -> ticker error handling
"""



#this file will need to recieve the API keys and the db credentials from the config file which will be passed down from the root main.py file
def compile_ETL_data(api_1: str='api_1', db_credentials: dict[str]=None, source: str = 'yfinance', tickers: list[str]=['AAPL', 'MSFT', 'GOOGL'], time_period: str='ytd') -> Dict[str, pd.DataFrame]:
    """
    Main ETL orchestrator function.
    
    Flow:
    1. Extract: Get raw data from APIs
    2. Transform: Clean and standardize data to match data model
    3. Load: (To be implemented) Insert into database
    
    Args:
        api_1: First API key (typically Finnhub)
        source: Data source identifier ('yfinance' or 'finnhub')
        tickers: List of stock ticker symbols to fetch data for
        time_period: Time period for which to fetch data (e.g., '5d', '1mo', 'ytd') default is 'ytd'
        
    Returns:
        Dictionary with 'extracted' and 'transformed' DataFrames
    """
    # Step 1: Extract - Get raw data from APIs
    extracted_data = compile_extracted_data(api_1, tickers, time_period)
    
    # Step 2: Transform - Clean and standardize data
    transformed_data = None
    
    #more info on the isinstance built-in method can be found at https://docs.python.org/3/library/functions.html#isinstance
    if isinstance(extracted_data, dict):#this checks if extracted_data is a dictionary
        # Check if we have actual data to transform
        for key, value in extracted_data.items():
            if isinstance(value, pd.DataFrame):
                transformed_data = transform_extracted_data(value, source=source)
                break
        # If no DataFrame found, return empty transformed structure
        if transformed_data is None:
            transformed_data = pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])
    elif isinstance(extracted_data, pd.DataFrame): #checks if instead the extracted_data is already a dataframe
        transformed_data = transform_extracted_data(extracted_data, source=source)
    else: #otherwise create the DF with the appropriate structure
        transformed_data = pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])
    
    #assume that at this point the data was extracted and transformed successfully!
    try:
        psql_connect_and_setup(
            db_host_addr=db_credentials['host'], 
            db_port=db_credentials['port'], 
            db_name=db_credentials['database'], 
            db_user=db_credentials['user'], 
            db_password=db_credentials['password'], 
            db_timeout=db_credentials['timeout'])
        insert_data(
            db_host_addr=db_credentials['host'], 
            db_port=db_credentials['port'], 
            db_name=db_credentials['database'], 
            db_user=db_credentials['user'], 
            db_password=db_credentials['password'], 
            db_timeout=db_credentials['timeout'],
            data=list(transformed_data.itertuples(index=False, name=None)) #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.itertuples.html, https://stackoverflow.com/questions/9758450/pandas-convert-dataframe-to-array-of-tuples 
        )
    except psycopg.IntegrityError as ie:
        print("Data insertion failed due to integrity error (there is probably duplicate data being entered):", ie)
    except psycopg.DatabaseError as de:
        print("Database setup failed:", de)
    else:
        #the data is converted to a list of tuples for each row for insertion with psycopg3
        print('Database setup and data insertion completed successfully!')
    
    return {
        'extracted': extracted_data,
        'transformed': transformed_data
    }