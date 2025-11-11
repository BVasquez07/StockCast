"""
Tests for Transform module
"""
import pytest
import pandas as pd
from src.Transform.main import (
    transform_yfinance_data,
    transform_finnhub_data,
    clean_stock_data,
    transform_extracted_data
)


class TestTransformYFinance:
    """Test yfinance data transformation"""
    
    def test_transform_yfinance_single_ticker(self, sample_yfinance_data):
        """Test transforming single ticker yfinance data"""
        result = transform_yfinance_data(sample_yfinance_data)
        
        assert not result.empty, "Result should not be empty"
        assert 'ticker' in result.columns, "Should have ticker column"
        assert 'date' in result.columns, "Should have date column"
        assert 'adj_close' in result.columns, "Should have adj_close column"
        assert len(result.columns) == 8, "Should have 8 columns"
        assert all(result['ticker'] == 'AAPL'), "All rows should be AAPL"
    
    def test_transform_yfinance_multiple_tickers(self):
        """Test transforming multiple tickers from yfinance"""
        import yfinance as yf
        
        # Download multiple tickers
        tickers = ["AAPL", "NVDA"]
        data = yf.download(tickers, period="5d", auto_adjust=False)
        
        if not data.empty:
            result = transform_yfinance_data(data)
            
            assert not result.empty, "Result should not be empty"
            assert 'ticker' in result.columns
            assert len(result['ticker'].unique()) >= 1, "Should have at least one ticker"
    
    def test_transform_yfinance_auto_adjust(self):
        """Test transforming yfinance data with auto_adjust=True"""
        import yfinance as yf
        
        data = yf.download(["AAPL"], period="5d", auto_adjust=True)
        
        if not data.empty:
            result = transform_yfinance_data(data)
            
            assert not result.empty
            assert 'adj_close' in result.columns
            # When auto_adjust=True, adj_close should equal close
            assert all(result['adj_close'] == result['close'])
    
    def test_transform_yfinance_empty_data(self):
        """Test transforming empty DataFrame"""
        empty_df = pd.DataFrame()
        result = transform_yfinance_data(empty_df)
        
        assert result.empty
        assert list(result.columns) == ['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']


class TestTransformFinnhub:
    """Test Finnhub data transformation"""
    
    def test_transform_finnhub_data(self, sample_finnhub_data):
        """Test transforming Finnhub API data"""
        result = transform_finnhub_data(sample_finnhub_data)
        
        assert not result.empty
        assert 'ticker' in result.columns
        assert 'date' in result.columns
        assert 'adj_close' in result.columns
        assert all(result['ticker'] == 'AAPL')
        # Finnhub doesn't provide adj_close, so it should use close
        assert all(result['adj_close'] == result['close'])
    
    def test_transform_finnhub_empty_data(self):
        """Test transforming empty Finnhub DataFrame"""
        empty_df = pd.DataFrame()
        result = transform_finnhub_data(empty_df)
        
        assert result.empty
        assert list(result.columns) == ['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']


class TestCleanStockData:
    """Test data cleaning functions"""
    
    def test_clean_stock_data_removes_negative_prices(self, dirty_stock_data):
        """Test that negative prices are removed"""
        cleaned = clean_stock_data(dirty_stock_data)
        
        assert len(cleaned) < len(dirty_stock_data), "Should remove invalid rows"
        assert all(cleaned['open'] > 0), "All prices should be positive"
    
    def test_clean_stock_data_removes_duplicates(self, sample_stock_data):
        """Test that duplicate rows are removed"""
        # Add duplicate
        duplicated = pd.concat([sample_stock_data, sample_stock_data.iloc[[0]]], ignore_index=True)
        cleaned = clean_stock_data(duplicated)
        
        assert len(cleaned) == len(sample_stock_data), "Duplicates should be removed"
    
    def test_clean_stock_data_validates_price_ranges(self):
        """Test that invalid price ranges are removed"""
        invalid_data = pd.DataFrame({
            'ticker': ['AAPL'],
            'date': ['2024-01-01'],
            'open': [150.0],
            'high': [140.0],  # High < Low (invalid)
            'low': [145.0],
            'close': [148.0],
            'adj_close': [148.0],
            'volume': [1000000]
        })
        
        cleaned = clean_stock_data(invalid_data)
        assert len(cleaned) == 0, "Invalid price ranges should be removed"
    
    def test_clean_stock_data_handles_missing_values(self):
        """Test that rows with missing critical data are removed"""
        data_with_nulls = pd.DataFrame({
            'ticker': ['AAPL', 'NVDA'],
            'date': ['2024-01-01', '2024-01-02'],
            'open': [150.0, None],  # Missing value
            'high': [152.0, 201.0],
            'low': [149.0, 199.0],
            'close': [151.0, 200.0],
            'adj_close': [151.0, 200.0],
            'volume': [1000000, 2000000]
        })
        
        cleaned = clean_stock_data(data_with_nulls)
        assert len(cleaned) == 1, "Row with missing data should be removed"
        assert cleaned.iloc[0]['ticker'] == 'AAPL'


class TestTransformExtractedData:
    """Test main transform function"""
    
    def test_transform_extracted_data_yfinance(self, sample_yfinance_data):
        """Test transform_extracted_data with yfinance source"""
        result = transform_extracted_data(sample_yfinance_data, source='yfinance')
        
        assert not result.empty
        assert 'ticker' in result.columns
    
    def test_transform_extracted_data_finnhub(self, sample_finnhub_data):
        """Test transform_extracted_data with finnhub source"""
        result = transform_extracted_data(sample_finnhub_data, source='finnhub')
        
        assert not result.empty
        assert 'ticker' in result.columns
    
    def test_transform_extracted_data_invalid_source(self, sample_stock_data):
        """Test transform_extracted_data with invalid source"""
        with pytest.raises(ValueError, match="Unknown data source"):
            transform_extracted_data(sample_stock_data, source='invalid_source')

