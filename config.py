from dotenv import load_dotenv
import os

try:
    with open('.env', 'r') as f:
        pass # we don't really care to do anything if the file exists and the file will close automatically due to the with statement
except OSError:
    with open('.env', 'w') as f:
        f.write("PSQL_USERNAME='postgres'\n")
        f.write("PSQL_PASSWORD='0000'\n")
        f.write("PSQL_HOST_ADDR='127.0.0.1'\n")
        f.write("PSQL_PORT='5430'\n")
        f.write("DB_NAME='monte_sim_stock_data'\n")
        f.write("CONNECTION_TIMEOUT=10\n")
finally:
    print('.env file checked for existence!')

# Load environment variables
load_dotenv()

# ETFs (Index Funds)
etf_list = ['SPY', 'QQQ', 'AGG']

# Individual Stocks (Top 10)
stock_list = ['NVDA', 'MSFT', 'AAPL', 'AMZN', 'GOOGL', 'META', 'LLY', 'AVGO', 'TSLA', 'BRK-B']

# Combined for analysis
ticker_list = etf_list + stock_list
db_credentials = {
    "user": os.getenv(key="PSQL_USERNAME", default="No Key Found"),
    "password": os.getenv(key="PSQL_PASSWORD", default="No Key Found"),
    "host": os.getenv(key="PSQL_HOST_ADDR", default="No Key Found"),
    "port": os.getenv(key="PSQL_PORT", default="No Key Found"),
    "database": os.getenv(key="DB_NAME", default="No Key Found"),
    "timeout": os.getenv(key="CONNECTION_TIMEOUT", default="No Key Found"),
}