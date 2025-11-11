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
    
    transformed_rows = []
    
    # Handle MultiIndex columns (multiple tickers or single ticker with MultiIndex)
    if isinstance(data.columns, pd.MultiIndex):
        # Get all values from both levels
        level0_values = data.columns.get_level_values(0).unique()
        level1_values = data.columns.get_level_values(1).unique()
        
        # Price column names that should NOT be treated as tickers
        price_keywords = ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close', 'AdjClose', 'Adj']
        
        # Determine which level contains tickers and which contains price types
        # Typically: level 0 = tickers, level 1 = price types
        # But sometimes it can be reversed or inconsistent
        
        # Check if level 0 looks like tickers (not all are price keywords)
        level0_is_tickers = not all(str(v) in price_keywords for v in level0_values)
        level1_is_tickers = not all(str(v) in price_keywords for v in level1_values)
        
        if level0_is_tickers and not level1_is_tickers:
            # Standard case: level 0 = tickers, level 1 = price types
            tickers = [t for t in level0_values if str(t) not in price_keywords]
        elif level1_is_tickers and not level0_is_tickers:
            # Reversed case: level 1 = tickers, level 0 = price types
            tickers = [t for t in level1_values if str(t) not in price_keywords]
        else:
            # Ambiguous - try to infer
            # Usually tickers are shorter strings and price types are longer
            potential_tickers = []
            for val in level0_values:
                val_str = str(val)
                if val_str not in price_keywords and len(val_str) <= 10:  # Tickers are usually short
                    potential_tickers.append(val)
            tickers = potential_tickers if potential_tickers else level0_values.tolist()
        
        # If still no valid tickers, extract from first column tuple
        if len(tickers) == 0 or all(str(t) in price_keywords for t in tickers):
            first_col = data.columns[0]
            if isinstance(first_col, tuple) and len(first_col) == 2:
                # Try both positions
                if str(first_col[0]) not in price_keywords:
                    tickers = [first_col[0]]
                elif str(first_col[1]) not in price_keywords:
                    tickers = [first_col[1]]
                else:
                    tickers = [first_col[0]]  # Fallback to first element
            else:
                tickers = ['UNKNOWN']
        
        # Check if Adj Close exists
        all_level1 = data.columns.get_level_values(1).unique()
        has_adj_close = any('Adj' in str(val) or 'adj' in str(val).lower() for val in all_level1)
        
        for ticker in tickers:
            try:
                # Access ticker data - try both level 0 and level 1
                ticker_data = None
                if ticker in data.columns.get_level_values(0):
                    ticker_data = data[ticker].copy()
                elif ticker in data.columns.get_level_values(1):
                    # Ticker might be in level 1, need to transpose or filter differently
                    # Filter columns where level 1 matches ticker
                    mask = data.columns.get_level_values(1) == ticker
                    ticker_data = data.loc[:, mask].copy()
                    # The columns will still have MultiIndex, need to flatten
                    if isinstance(ticker_data.columns, pd.MultiIndex):
                        # Flatten by taking level 0 (price types)
                        ticker_data.columns = ticker_data.columns.get_level_values(0)
                else:
                    # Ticker not found in either level - might be single ticker case
                    # Use all columns and extract ticker from structure
                    ticker_data = data.copy()
                    if isinstance(data.columns[0], tuple):
                        # Extract ticker from first column tuple
                        first_col = data.columns[0]
                        if len(first_col) >= 1:
                            # Try to find which element is the ticker
                            for elem in first_col:
                                if str(elem) not in price_keywords:
                                    ticker = elem
                                    break
                
                if ticker_data is None:
                    raise ValueError(f"Could not extract data for ticker {ticker}")
                
                # Reset index to get date as a column
                ticker_data = ticker_data.reset_index()
                
                # Standardize column names to lowercase
                ticker_data.columns = ticker_data.columns.str.lower()
                
                # Handle date column - yfinance uses 'Date' as index name
                if 'date' not in ticker_data.columns:
                    # Check if index was reset and first column is date
                    if len(ticker_data.columns) > 0:
                        first_col = ticker_data.columns[0]
                        if pd.api.types.is_datetime64_any_dtype(ticker_data[first_col]):
                            ticker_data.rename(columns={first_col: 'date'}, inplace=True)
                        else:
                            # Date might be in index name
                            ticker_data.insert(0, 'date', ticker_data.index if hasattr(ticker_data, 'index') else pd.date_range('2024-01-01', periods=len(ticker_data)))
                
                # Ensure date column exists
                if 'date' not in ticker_data.columns:
                    ticker_data.insert(0, 'date', ticker_data.index)
                
                # Map common column name variations
                column_mapping = {
                    'adj close': 'adj_close',
                    'adjclose': 'adj_close',
                }
                for old_name, new_name in column_mapping.items():
                    if old_name in ticker_data.columns and new_name not in ticker_data.columns:
                        ticker_data[new_name] = ticker_data[old_name]
                
                # Ensure we have all required columns
                required_cols = ['open', 'high', 'low', 'close', 'volume']
                missing_cols = [col for col in required_cols if col not in ticker_data.columns]
                if missing_cols:
                    raise ValueError(f"Missing required columns {missing_cols} for ticker {ticker}. Available columns: {list(ticker_data.columns)}")
                
                # Handle adj_close
                if 'adj_close' not in ticker_data.columns:
                    if 'adj close' in ticker_data.columns:
                        ticker_data['adj_close'] = ticker_data['adj close']
                    else:
                        # Use close as fallback (common when auto_adjust=True)
                        ticker_data['adj_close'] = ticker_data['close']
                
                # Add ticker column
                ticker_data['ticker'] = ticker
                
                # Select and reorder columns to match data model
                ticker_data = ticker_data[['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
                
                transformed_rows.append(ticker_data)
            except Exception as e:
                # Skip this ticker if there's an error, but log it
                print(f"Warning: Error processing ticker {ticker}: {e}")
                continue
    else:
        # Flat columns case - single ticker with non-MultiIndex columns
        data_reset = data.reset_index()
        data_reset.columns = data_reset.columns.str.lower()
        
        # Handle date column
        if 'date' not in data_reset.columns:
            first_col = data_reset.columns[0]
            if pd.api.types.is_datetime64_any_dtype(data_reset[first_col]):
                data_reset.rename(columns={first_col: 'date'}, inplace=True)
            else:
                data_reset.insert(0, 'date', data_reset.index)
        
        # Handle adj_close
        if 'adj close' in data_reset.columns:
            data_reset['adj_close'] = data_reset['adj close']
        elif 'adj_close' not in data_reset.columns:
            data_reset['adj_close'] = data_reset['close']
        
        # For single ticker, we need to infer or use a default
        # Try to get ticker from data if available, otherwise use placeholder
        if 'ticker' not in data_reset.columns:
            # Try to infer from column names or use 'UNKNOWN'
            data_reset['ticker'] = 'UNKNOWN'  # Will need to be set by caller
        
        transformed_rows.append(data_reset[['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']])
    
    if not transformed_rows:
        raise ValueError("No valid data could be transformed. Check input DataFrame structure.")
    
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

