from src.Extract.main import compile_extracted_data
from src.Transform.main import transform_extracted_data
import pandas as pd
from typing import Dict, Union

"""
This file will serve as the main ETL orchestrator, 
calling the necessary functions from the Extract, Transform, and Load modules.
The data will then be passed up to the root main.py 
because that root main will be the entry point for the entire application!

"""

#this file will need to recieve the API keys and the db credentials from the config file which will be passed down from the root main.py file



def compile_ETL_data(api_1='api_1', api_2='api_2', source: str = 'yfinance') -> Dict[str, pd.DataFrame]:
    """
    Main ETL orchestrator function.
    
    Flow:
    1. Extract: Get raw data from APIs
    2. Transform: Clean and standardize data to match data model
    3. Load: (To be implemented) Insert into database
    
    Args:
        api_1: First API key (typically Finnhub)
        api_2: Second API key (typically Yahoo Finance)
        source: Data source identifier ('yfinance' or 'finnhub')
        
    Returns:
        Dictionary with 'extracted' and 'transformed' DataFrames
    """
    # Step 1: Extract - Get raw data from APIs
    extracted_data = compile_extracted_data(api_1_key=api_1, api_2_key=api_2)
    
    # Step 2: Transform - Clean and standardize data
    # Note: Currently extract returns placeholder dict, so transform will handle it gracefully
    transformed_data = None
    
    # If extracted_data contains actual DataFrames, transform them
    if isinstance(extracted_data, dict):
        # Check if we have actual data to transform
        for key, value in extracted_data.items():
            if isinstance(value, pd.DataFrame):
                transformed_data = transform_extracted_data(value, source=source)
                break
        
        # If no DataFrame found, return empty transformed structure
        if transformed_data is None:
            transformed_data = pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])
    elif isinstance(extracted_data, pd.DataFrame):
        transformed_data = transform_extracted_data(extracted_data, source=source)
    else:
        # Fallback: create empty DataFrame with correct structure
        transformed_data = pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])
    
    # Step 3: Load - (To be implemented in Load module)
    # transformed_data will be ready for database insertion
    
    return {
        'extracted': extracted_data,
        'transformed': transformed_data
    }