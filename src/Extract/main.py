#here we will do the extraction
from config import api_keys
from src.Extract.yfinance_fetch_data import fetch_yfinance_data



def testing(text: str) -> str:
    return f"This is a test function from src.Extract.main {text}"

def compile_extracted_data(api_1_key: str, tickers: list[str], time_period: str) -> dict:
    #each of these will make a call to the respective function in the file and then cache it in this dictionary
    #which could potentially just be turned into a dataframe for easy manipulation when handed to the 
    data = {
        "api_1_data": f"Data extracted using {api_1_key}",
        "yfinance_data": fetch_yfinance_data(tickers_list=tickers, time_period=time_period)
    }
    return data

if __name__ == "__main__":
    print(testing())

 