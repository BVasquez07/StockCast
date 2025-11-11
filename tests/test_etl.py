"""
Tests for ETL pipeline integration
"""
import pytest
import pandas as pd
from src.main import compile_ETL_data
from config import api_keys


class TestETLPipeline:
    """Test ETL pipeline integration"""
    
    def test_compile_ETL_data_returns_dict(self):
        """Test that compile_ETL_data returns expected structure"""
        result = compile_ETL_data(
            api_keys.get("finnhub", ""),
            api_keys.get("yahoo_fin", ""),
            source='yfinance'
        )
        
        assert isinstance(result, dict), "Should return a dictionary"
        assert 'extracted' in result, "Should have 'extracted' key"
        assert 'transformed' in result, "Should have 'transformed' key"
    
    def test_compile_ETL_data_transformed_structure(self):
        """Test that transformed data has correct structure"""
        result = compile_ETL_data(
            api_keys.get("finnhub", ""),
            api_keys.get("yahoo_fin", ""),
            source='yfinance'
        )
        
        transformed = result['transformed']
        assert isinstance(transformed, pd.DataFrame), "Transformed should be DataFrame"
        
        # Check columns match data model
        expected_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
        assert list(transformed.columns) == expected_columns, "Columns should match data model"
    
    def test_compile_ETL_data_with_different_sources(self):
        """Test ETL pipeline with different source parameters"""
        # Test with yfinance
        result_yf = compile_ETL_data(
            api_keys.get("finnhub", ""),
            api_keys.get("yahoo_fin", ""),
            source='yfinance'
        )
        assert 'transformed' in result_yf
        
        # Test with finnhub (if we had actual data)
        result_fh = compile_ETL_data(
            api_keys.get("finnhub", ""),
            api_keys.get("yahoo_fin", ""),
            source='finnhub'
        )
        assert 'transformed' in result_fh

