"""
Transform Module - Process and Clean Stock Data

This module handles:
1. Standardizing data from different sources (Yahoo Finance, Finnhub)
2. Cleaning and validating data
3. Ensuring all required columns match the data model
4. Preparing data for database insertion
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union


def transform_yfinance_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Yahoo Finance data to match our data model.
    
    Yahoo Finance returns MultiIndex DataFrame with columns:
    - Open, High, Low, Close, Volume, Adj Close (when auto_adjust=True)
    - Index: Date (DatetimeIndex)
    - Columns: MultiIndex with tickers as level 0
    
    Args:
        data: MultiIndex DataFrame from yfinance.download()
        
    Returns:
        DataFrame with columns: ticker, date, open, high, low, close, adj_close, volume
    """
    if data.empty:
        return pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])
    
    # Stack the MultiIndex to get ticker and date in rows
    # This converts from wide format (tickers as columns) to long format
    transformed_rows = []
    
    # Get all tickers from the columns
    if isinstance(data.columns, pd.MultiIndex):
        tickers = data.columns.get_level_values(0).unique()
        price_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        # Check if Adj Close exists
        has_adj_close = 'Adj Close' in data.columns.get_level_values(1).unique()
        
        for ticker in tickers:
            ticker_data = data[ticker].copy()
            
            # Reset index to get date as a column
            ticker_data = ticker_data.reset_index()
            
            # Standardize column names to lowercase
            ticker_data.columns = ticker_data.columns.str.lower()
            
            # Rename 'date' if it exists, otherwise use index
            if 'date' not in ticker_data.columns:
                ticker_data = ticker_data.reset_index()
                if 'date' in ticker_data.columns:
                    pass  # Already have date
                else:
                    ticker_data.rename(columns={ticker_data.columns[0]: 'date'}, inplace=True)
            
            # Ensure we have all required columns
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in required_cols:
                if col not in ticker_data.columns:
                    raise ValueError(f"Missing required column '{col}' for ticker {ticker}")
            
            # Handle adj_close
            if has_adj_close and 'adj close' in ticker_data.columns:
                ticker_data['adj_close'] = ticker_data['adj close']
            elif 'adj_close' in ticker_data.columns:
                pass  # Already have adj_close
            else:
                # If no adj_close, use close as fallback (common for ETFs or when auto_adjust=True)
                ticker_data['adj_close'] = ticker_data['close']
            
            # Add ticker column
            ticker_data['ticker'] = ticker
            
            # Select and reorder columns to match data model
            ticker_data = ticker_data[['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
            
            transformed_rows.append(ticker_data)
    else:
        # Single ticker case - columns are already flat
        data_reset = data.reset_index()
        data_reset.columns = data_reset.columns.str.lower()
        
        # Handle date column
        if 'date' not in data_reset.columns:
            data_reset = data_reset.reset_index()
            if data_reset.columns[0] != 'date':
                data_reset.rename(columns={data_reset.columns[0]: 'date'}, inplace=True)
        
        # Handle adj_close
        if 'adj close' in data_reset.columns:
            data_reset['adj_close'] = data_reset['adj close']
        elif 'adj_close' not in data_reset.columns:
            data_reset['adj_close'] = data_reset['close']
        
        # Add ticker (would need to be passed as parameter for single ticker)
        # For now, we'll assume it's in the data or needs to be added
        if 'ticker' not in data_reset.columns:
            raise ValueError("Ticker column missing for single ticker data")
        
        transformed_rows.append(data_reset[['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']])
    
    # Combine all tickers
    result_df = pd.concat(transformed_rows, ignore_index=True)
    
    # Clean and validate data
    result_df = clean_stock_data(result_df)
    
    return result_df


def transform_finnhub_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Finnhub API data to match our data model.
    
    Finnhub returns DataFrame with columns:
    - symbol, datetime, open, high, low, close, volume
    
    Args:
        data: DataFrame from Finnhub API
        
    Returns:
        DataFrame with columns: ticker, date, open, high, low, close, adj_close, volume
    """
    if data.empty:
        return pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])
    
    df = data.copy()
    
    # Rename columns to match our standard
    column_mapping = {
        'symbol': 'ticker',
        'datetime': 'date'
    }
    df.rename(columns=column_mapping, inplace=True)
    
    # Ensure date is datetime type
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    elif 'datetime' in df.columns:
        df['date'] = pd.to_datetime(df['datetime'])
        df.drop(columns=['datetime'], inplace=True)
    
    # Finnhub doesn't provide adj_close, so we'll use close as fallback
    # In production, you might want to calculate it based on splits/dividends
    if 'adj_close' not in df.columns:
        df['adj_close'] = df['close']
    
    # Ensure all required columns exist
    required_cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Select and reorder columns
    df = df[required_cols]
    
    # Clean and validate data
    df = clean_stock_data(df)
    
    return df


def clean_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate stock data.
    
    Operations:
    - Remove rows with missing critical data
    - Ensure data types are correct
    - Validate price ranges (no negative prices)
    - Remove duplicates
    - Sort by ticker and date
    
    Args:
        df: DataFrame with stock data
        
    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Convert date to datetime if it's not already
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        # Extract just the date part (remove time if present)
        df['date'] = df['date'].dt.date
    
    # Ensure ticker is string
    if 'ticker' in df.columns:
        df['ticker'] = df['ticker'].astype(str).str.upper()
    
    # Remove rows where critical price data is missing
    price_cols = ['open', 'high', 'low', 'close', 'adj_close']
    df = df.dropna(subset=price_cols)
    
    # Validate prices are positive
    for col in price_cols:
        df = df[df[col] > 0]
    
    # Validate high >= low, high >= open, high >= close, low <= open, low <= close
    df = df[
        (df['high'] >= df['low']) &
        (df['high'] >= df['open']) &
        (df['high'] >= df['close']) &
        (df['low'] <= df['open']) &
        (df['low'] <= df['close'])
    ]
    
    # Ensure volume is non-negative integer
    if 'volume' in df.columns:
        df['volume'] = df['volume'].fillna(0)
        df['volume'] = df['volume'].astype(int)
        df = df[df['volume'] >= 0]
    
    # Remove duplicates (same ticker and date)
    df = df.drop_duplicates(subset=['ticker', 'date'], keep='first')
    
    # Sort by ticker and date
    df = df.sort_values(['ticker', 'date']).reset_index(drop=True)
    
    return df


def transform_extracted_data(extracted_data: Union[Dict, pd.DataFrame], source: str = 'yfinance') -> pd.DataFrame:
    """
    Main transformation function that routes to appropriate transformer based on source.
    
    Args:
        extracted_data: Raw data from Extract module (dict or DataFrame)
        source: Data source identifier ('yfinance', 'finnhub', etc.)
        
    Returns:
        Transformed and cleaned DataFrame ready for database insertion
    """
    if isinstance(extracted_data, dict):
        # If it's a dict, try to extract the actual data
        # This handles the current placeholder structure
        if 'api_1_data' in extracted_data or 'api_2_data' in extracted_data:
            # Placeholder data - return empty DataFrame with correct structure
            return pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])
        else:
            # Try to find DataFrame in dict
            for key, value in extracted_data.items():
                if isinstance(value, pd.DataFrame):
                    extracted_data = value
                    break
            else:
                raise ValueError("Could not find DataFrame in extracted_data dict")
    
    if not isinstance(extracted_data, pd.DataFrame):
        raise TypeError(f"extracted_data must be DataFrame or dict containing DataFrame, got {type(extracted_data)}")
    
    # Route to appropriate transformer
    if source.lower() == 'yfinance':
        return transform_yfinance_data(extracted_data)
    elif source.lower() == 'finnhub':
        return transform_finnhub_data(extracted_data)
    else:
        raise ValueError(f"Unknown data source: {source}. Supported sources: 'yfinance', 'finnhub'")


if __name__ == "__main__":
    # Test with sample data
    print("Transform module loaded successfully")
    print("Use transform_extracted_data() to process extracted data")

