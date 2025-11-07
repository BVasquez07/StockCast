from dotenv import load_dotenv
import os

#loading the API_keys from the envorionment variables
load_dotenv()

api_keys = {
    "alpha_vantage": os.getenv(key="ALPHAVANTAGE_API_KEY", default="No Key Found"),
    "finnhub": os.getenv(key="FINNHUB_API_KEY", default="No Key Found"),
    "polygon": os.getenv(key="POLYGON_API_KEY", default="No Key Found")
}