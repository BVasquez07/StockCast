import requests
import datetime
import time
import pandas as pd

#API KEY
API_KEY = ""

#stocks for testing
symbols = ["AAPL", "NVDA"]
#present day rows
rowsP =[]
#historical rows
rowsH = []

#present day calls
for sym in symbols:
  url = f"https://finnhub.io/api/v1/quote?symbol={sym}&token={API_KEY}"
  pData = requests.get(url).json()
  pData['symbol'] = sym
  rowsP.append(pData)

#init dataframe 1
df1 = pd.DataFrame(rowsP)

#changing t from timestamp into datetime for ease of use/understanding
df1['datetime'] = pd.to_datetime(df1['t'], unit='s')
df1 = df1.drop(columns=['t'])

#c = current price, d = change from prev close, dp = percent change from prev close
#h = high, l = low, o = open price, pc = previous close, symbol = stock symbol
print(df1.columns, "\n")

print(df1, "\n")

#historical data call dates
start_date = "2025-09-01" 
end_date = "2025-10-01"

# converts dates to unix (original t)
from_ts = int(time.mktime(datetime.datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
to_ts = int(time.mktime(datetime.datetime.strptime(end_date, "%Y-%m-%d").timetuple()))

for sym in symbols:
  hurl = f"https://finnhub.io/api/v1/stock/candle?symbol={sym}&resolution=D&from={from_ts}&to={to_ts}&token={API_KEY}"
  hData = requests.get(hurl).json()
  hData['symbol'] = sym
  rowsH.append(hData)

#creating historical dataframe
#ERROR: DO NOT HAVE ACCESS
"""
Warning: No historical data for AAPL or API error
Returned data: {'error': "You don't have access to this resource.", 'symbol': 'AAPL'}
Warning: No historical data for NVDA or API error
Returned data: {'error': "You don't have access to this resource.", 'symbol': 'NVDA'}
"""

hist_dfs =[]
for hData in rowsH:
    if hData.get('s') == 'ok':
        df_temp = pd.DataFrame({
            'symbol': hData['symbol'],
            'datetime': pd.to_datetime(hData['t'], unit='s'),
            'open': hData['o'],
            'high': hData['h'],
            'low': hData['l'],
            'close': hData['c'],
            'volume': hData['v']
        })
        hist_dfs.append(df_temp)
    else:
      print(f"Warning: No historical data for {hData.get('symbol', 'Unknown')} or API error")
      print("Returned data:", hData)

df2 = pd.concat(hist_dfs, ignore_index=True)

print("Historical Data:")
print(df2.head())
