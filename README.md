# Monte Carlo Simulation of $250K in Stocks & ETFs

## Data Model

```sql
Table stock_data {
  id integer [primary key]
  ticker varchar
  date date
  open float
  high float
  low float
  close float
  adj_close float
  volume integer
}

Table simulation {
  id integer [primary key]
  ticker varchar [not null, ref: > stock_data.ticker]
  simulation_id integer
  year integer
  starting_value float
  ending_value float
  annual_return float
  cumulative_return float
  volatility float
  probability float
}

-- one-to-many: each stock record generates many simulation results
Ref stock_data.id < simulation.id
```

**Refinements:**
- Added `adj_close` column: Yahoo Finance provides it; Finnhub doesn't (uses `close` as fallback). Critical for accurate analysis accounting for splits/dividends.
- Changed `date` to `year` in simulation table: Simulations are aggregated yearly, integer is more efficient for this use case.

## Architecture

ETL Pipeline: **Extract → Transform → Load**

- **Extract** (`src/Extract/`): Fetches data from Yahoo Finance & Finnhub APIs
- **Transform** (`src/Transform/`): Cleans, standardizes, and validates data to match data model
- **Load** (`src/Load/`): Inserts transformed data into PostgreSQL database
- **Monte Carlo** (`src/Monte_Carlo/`): Runs simulations using Geometric Brownian Motion
- **Database** (`src/db/`): PostgreSQL connection and schema management

## Completed

✅ **Transform Module**: Created complete transformation pipeline
- Handles both Yahoo Finance (MultiIndex DataFrame) and Finnhub (JSON) formats
- Standardizes column names and data types
- Validates data quality (price ranges, removes duplicates, handles missing values)
- Maps to data model columns (ticker, date, open, high, low, close, adj_close, volume)

✅ **Database Schema**: Updated to match data model
- Added `adj_close` column to `stock_data` table
- Changed `date` to `year` in `simulation` table
- Migration logic for existing databases

✅ **ETL Integration**: Transform step integrated into pipeline
- `src/main.py` now orchestrates Extract → Transform → Load flow
- Returns both extracted and transformed data

## Next Steps

🚧 **Extract Module**: Implement actual API extraction functions
- Complete `finnhub_api_extraction.py` implementation
- Update `compile_extracted_data()` to return actual DataFrames instead of placeholders

🚧 **Load Module**: Implement database insertion
- Create functions to insert transformed stock_data into PostgreSQL
- Handle bulk inserts efficiently
- Add error handling

🚧 **Monte Carlo Integration**: Connect simulation to database
- Update simulation to output data matching `simulation` table schema
- Add year extraction from simulation results
- Insert simulation results into database

🚧 **Error Handling**: Add comprehensive error handling throughout pipeline

## Testing

### 1. Test Transform Module
```python
# Test with sample Yahoo Finance data
import pandas as pd
from src.Transform.main import transform_yfinance_data

# Create sample MultiIndex DataFrame (simulating yfinance output)
dates = pd.date_range('2024-01-01', periods=5)
tickers = ['AAPL', 'NVDA']
data = {
    ('AAPL', 'Open'): [150, 151, 152, 153, 154],
    ('AAPL', 'High'): [151, 152, 153, 154, 155],
    ('AAPL', 'Low'): [149, 150, 151, 152, 153],
    ('AAPL', 'Close'): [150.5, 151.5, 152.5, 153.5, 154.5],
    ('AAPL', 'Adj Close'): [150.5, 151.5, 152.5, 153.5, 154.5],
    ('AAPL', 'Volume'): [1000000, 1100000, 1200000, 1300000, 1400000],
    # ... similar for NVDA
}
df = pd.DataFrame(data, index=dates)
df.columns = pd.MultiIndex.from_tuples(df.columns)

result = transform_yfinance_data(df)
print(result.head())
# Should output: ticker, date, open, high, low, close, adj_close, volume
```

### 2. Test Database Connection
```python
# Run the connection script
python src/db/connection.py

# Should create/connect to database and show empty tables
# Verify tables exist:
# - stock_data (with adj_close column)
# - simulation (with year column, not date)
```

### 3. Test ETL Pipeline
```python
# Test the full ETL flow
from src.main import compile_ETL_data
from config import api_keys

# This will run Extract → Transform (Load not yet implemented)
result = compile_ETL_data(api_keys["finnhub"], api_keys["yahoo_fin"], source='yfinance')
print("Transformed data shape:", result['transformed'].shape)
print("Transformed columns:", result['transformed'].columns.tolist())
```

### 4. Test Data Validation
```python
# Test cleaning functions
from src.Transform.main import clean_stock_data
import pandas as pd

# Create test data with issues
test_data = pd.DataFrame({
    'ticker': ['AAPL', 'AAPL', 'NVDA'],
    'date': ['2024-01-01', '2024-01-02', '2024-01-01'],
    'open': [150, 151, -100],  # Negative price should be removed
    'high': [152, 153, 200],
    'low': [149, 150, 50],
    'close': [151, 152, 100],
    'adj_close': [151, 152, 100],
    'volume': [1000000, 1100000, 2000000]
})

cleaned = clean_stock_data(test_data)
# Should remove row with negative price
print(f"Original: {len(test_data)} rows, Cleaned: {len(cleaned)} rows")
```
