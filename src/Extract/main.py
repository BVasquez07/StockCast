#here we will do the extraction
from src.Extract.finnhub_api_extraction import fetch_finnhub_data
from config import api_keys



def testing(text: str) -> str:
    return f"This is a test function from src.Extract.main {text}"

def compile_extracted_data(api_1_key='api_1', api_2_key='api_2') -> dict:
    #each of these will make a call to the respective function in the file and then cache it in this dictionary
    #which could potentially just be turned into a dataframe for easy manipulation when handed to the 
    data = {
        "api_1_data": f"Data extracted using {api_1_key}",
        "api_2_data": f"Data extracted using {api_2_key}",
    }
    return data

if __name__ == "__main__":
    print(testing())

 