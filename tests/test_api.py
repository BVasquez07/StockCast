"""
Tests for API extraction
"""
import pytest
import pandas as pd
import requests
import yfinance as yf
from config import api_keys


class TestYahooFinanceAPI:
    """Test Yahoo Finance (yfinance) API"""
    
    def test_yfinance_import(self):
        """Test that yfinance can be imported"""
        import yfinance as yf
        assert yf is not None
    
    def test_yfinance_download_single_ticker(self):
        """Test downloading data for a single ticker"""
        data = yf.download("AAPL", period="5d", auto_adjust=False)
        
        assert not data.empty, "Should return data"
        assert len(data) > 0, "Should have at least one row"
    
    def test_yfinance_download_multiple_tickers(self):
        """Test downloading data for multiple tickers"""
        tickers = ["AAPL", "NVDA"]
        data = yf.download(tickers, period="5d", auto_adjust=False)
        
        assert not data.empty
        # Should have MultiIndex columns for multiple tickers
        if len(tickers) > 1:
            assert isinstance(data.columns, pd.MultiIndex), "Multiple tickers should have MultiIndex columns"
    
    def test_yfinance_has_required_columns(self):
        """Test that yfinance returns required columns"""
        data = yf.download("AAPL", period="5d", auto_adjust=False)
        
        if isinstance(data.columns, pd.MultiIndex):
            # MultiIndex case
            level1 = data.columns.get_level_values(1).unique()
            required = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in required:
                assert col in level1 or any(col.lower() in str(c).lower() for c in level1), f"Missing {col}"
        else:
            # Single level columns
            required = ['Open', 'High', 'Low', 'Close', 'Volume']
            cols_lower = [str(c).lower() for c in data.columns]
            for col in required:
                assert any(col.lower() in c for c in cols_lower), f"Missing {col}"


class TestFinnhubAPI:
    """Test Finnhub API"""
    
    @pytest.fixture
    def finnhub_key(self):
        """Get Finnhub API key or skip test"""
        key = api_keys.get('finnhub', '')
        if not key or key == 'No Key Found':
            pytest.skip("Finnhub API key not set")
        return key
    
    def test_finnhub_quote_endpoint(self, finnhub_key):
        """Test Finnhub quote endpoint"""
        url = f"https://finnhub.io/api/v1/quote?symbol=AAPL&token={finnhub_key}"
        response = requests.get(url, timeout=10)
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert 'error' not in data, f"API returned error: {data.get('error')}"
        assert 'c' in data, "Should have current price"
    
    def test_finnhub_historical_data(self, finnhub_key):
        """Test Finnhub historical data endpoint (may require paid plan)"""
        import time
        import datetime
        
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=5)
        from_ts = int(start_date.timestamp())
        to_ts = int(end_date.timestamp())
        
        url = f"https://finnhub.io/api/v1/stock/candle?symbol=AAPL&resolution=D&from={from_ts}&to={to_ts}&token={finnhub_key}"
        response = requests.get(url, timeout=10)
        
        assert response.status_code in [200, 403], "Should return 200 (success) or 403 (requires paid plan)"
        
        if response.status_code == 200:
            data = response.json()
            if data.get('s') == 'ok':
                assert 'c' in data, "Should have close prices"
                assert 't' in data, "Should have timestamps"

