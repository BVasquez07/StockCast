# Monte Carlo Simulation of $250K in Stocks & ETFs

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Environment Variables (.env file)

Create a `.env` file in the root directory (or copy from `.env.example`):

```bash
# API Keys
FINNHUB_API_KEY=your_finnhub_api_key_here
# Not needed - yfinance is free, no API key required

# PostgreSQL Database
PSQL_USERNAME=postgres
PSQL_PASSWORD=your_postgres_password
PSQL_HOST_ADDR=127.0.0.1
PSQL_PORT=5432
DB_NAME=monte_sim_stock_data
CONNECTION_TIMEOUT=10
```

**Getting API Keys:**
- **Finnhub**: Get free API key at https://finnhub.io/register (free tier: 60 calls/minute)
- **Yahoo Finance**: No API key needed! The `yfinance` library is free and doesn't require authentication

### 3. Quick Test
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_transform.py

# Run specific test
pytest tests/test_transform.py::TestTransformYFinance::test_transform_yfinance_single_ticker

# Run with coverage
pytest --cov=src tests/

# Run only fast tests (skip slow/API/DB tests)
pytest -m "not slow and not api and not db"
```

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

✅ **Test Suite**: Comprehensive pytest-based test suite
- Individual test functions for each component
- Fixtures for common setup
- Easy to add new tests
- Can run specific tests or all tests

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

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_transform.py
pytest tests/test_api.py
pytest tests/test_etl.py
pytest tests/test_database.py


```





1. Create a new test file: `tests/test_your_module.py`
2. Import pytest and your module
3. Write test functions starting with `test_`
4. Use fixtures from `conftest.py` or create new ones
5. Run: `pytest tests/test_your_module.py`


```
