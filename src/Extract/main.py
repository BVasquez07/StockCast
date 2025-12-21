#here we will do the extraction
from src.Extract.yfinance_fetch_data import fetch_yfinance_data


def compile_extracted_data(api_key: str, tickers: list[str], time_period: str) -> dict:
    """
    Extract stock data from Yahoo Finance.
    
    Args:
        api_key: Unused, kept for compatibility
        tickers: List of stock ticker symbols
        time_period: Time period for data (e.g., '5d', '1mo', 'ytd')
    
    Returns:
        Dictionary containing yfinance DataFrame
    """
    data = {
        "yfinance_data": fetch_yfinance_data(tickers_list=tickers, time_period=time_period)
    }
    return data


if __name__ == "__main__":
    print("Extract module loaded successfully")

 