import yfinance as yf


def fetch_yfinance_data(tickers_list: list[str], time_period: str) -> dict:
    """
    Fetch historical stock data from Yahoo Finance for the given tickers.
    
    Args:
        tickers: List of stock ticker symbols.
        time_period: Time period for which to fetch data (e.g., '5d', '1mo', 'ytd') default is 'ytd'.
    """
    data = yf.download(tickers_list, period=time_period, auto_adjust=False)

    return data