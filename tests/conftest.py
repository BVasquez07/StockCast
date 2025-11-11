"""
Pytest configuration and shared fixtures
"""
import pytest
import pandas as pd
import numpy as np
from config import api_keys


@pytest.fixture
def finnhub_api_key():
    """Fixture for Finnhub API key"""
    key = api_keys.get('finnhub', '')
    if not key or key == 'No Key Found':
        pytest.skip("Finnhub API key not set")
    return key


@pytest.fixture
def sample_stock_data():
    """Fixture providing sample stock data for testing"""
    return pd.DataFrame({
        'ticker': ['AAPL', 'AAPL', 'NVDA', 'NVDA'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
        'open': [150.0, 151.0, 200.0, 201.0],
        'high': [152.0, 153.0, 201.0, 202.0],
        'low': [149.0, 150.0, 199.0, 200.0],
        'close': [151.0, 152.0, 200.0, 201.0],
        'adj_close': [151.0, 152.0, 200.0, 201.0],
        'volume': [1000000, 1100000, 2000000, 2100000]
    })


@pytest.fixture
def dirty_stock_data():
    """Fixture providing stock data with validation issues"""
    return pd.DataFrame({
        'ticker': ['AAPL', 'AAPL', 'NVDA', 'NVDA'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
        'open': [150.0, 151.0, 200.0, -50.0],  # Negative price
        'high': [152.0, 153.0, 201.0, 201.0],
        'low': [149.0, 150.0, 199.0, 199.0],
        'close': [151.0, 152.0, 200.0, 200.0],
        'adj_close': [151.0, 152.0, 200.0, 200.0],
        'volume': [1000000, 1100000, 2000000, 2100000]
    })


@pytest.fixture
def sample_finnhub_data():
    """Fixture providing sample Finnhub API response data"""
    return pd.DataFrame({
        'symbol': ['AAPL', 'AAPL', 'AAPL'],
        'datetime': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'open': [150.0, 151.0, 152.0],
        'high': [152.0, 153.0, 154.0],
        'low': [149.0, 150.0, 151.0],
        'close': [151.0, 152.0, 153.0],
        'volume': [1000000, 1100000, 1200000]
    })


@pytest.fixture
def sample_yfinance_data():
    """Fixture providing sample yfinance MultiIndex DataFrame structure"""
    dates = pd.date_range('2024-01-01', periods=5, freq='D')
    data = {
        ('AAPL', 'Open'): [150.0, 151.0, 152.0, 153.0, 154.0],
        ('AAPL', 'High'): [152.0, 153.0, 154.0, 155.0, 156.0],
        ('AAPL', 'Low'): [149.0, 150.0, 151.0, 152.0, 153.0],
        ('AAPL', 'Close'): [151.0, 152.0, 153.0, 154.0, 155.0],
        ('AAPL', 'Adj Close'): [151.0, 152.0, 153.0, 154.0, 155.0],
        ('AAPL', 'Volume'): [1000000, 1100000, 1200000, 1300000, 1400000],
    }
    df = pd.DataFrame(data, index=dates)
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    return df

