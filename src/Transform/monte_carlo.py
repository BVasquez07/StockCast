import pandas as pd
import numpy as np

# Monte Carlo simulation with annual aggregation using previously cleaned DataFrame
# Can pass any list of tickers, portfolio value, and years
def run_monte_carlo(
    df: pd.DataFrame,
    tickers: list[str],
    portfolio_value: float = 250000,
    years: int = 10,
    num_simulations: int = 10000,
    seed: int = None
) -> pd.DataFrame:
    """
    Monte Carlo simulation using pre-cleaned stock data from Transform module.
    Columns: id, ticker, simulation_num, year, starting_value, ending_value,
             annual_return, cumulative_return, volatility, probability
    """

    if seed is not None:
        np.random.seed(seed)

    # Ensure dataframe has required columns
    required_cols = ['ticker', 'date', 'adj_close']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"DataFrame must contain '{col}' column")

    results = []
    trading_days_per_year = 252
    n_tickers = len(tickers)

    for sim in range(num_simulations):
        for ticker in tickers:
            # Select ticker's historical prices
            ticker_data = df[df['ticker'] == ticker].sort_values('date')
            prices = ticker_data['adj_close'].values
            if len(prices) < 2:
                continue  # skip tickers with insufficient data

            # Compute daily log returns from cleaned prices
            daily_returns = np.log(prices[1:] / prices[:-1])

            # Mean and covariance for univariate simulation
            mean_return = daily_returns.mean()
            std_return = daily_returns.std()

            # Simulate daily returns for 'years'
            total_days = years * trading_days_per_year
            simulated_returns = np.random.normal(
                loc=mean_return,
                scale=std_return,
                size=total_days
            )
            growth_factors = np.exp(simulated_returns)

            starting_val = portfolio_value / n_tickers
            initial_val = starting_val

            # Track values per year
            for year in range(1, years + 1):
                start_idx = (year - 1) * trading_days_per_year
                end_idx = year * trading_days_per_year
                yearly_growth = growth_factors[start_idx:end_idx].prod()
                ending_val = starting_val * yearly_growth

                annual_return = yearly_growth - 1
                cumulative_return = (ending_val / initial_val) - 1
                volatility = np.std(
                    simulated_returns[start_idx:end_idx]
                ) * np.sqrt(trading_days_per_year)
                probability = 1.0 if ending_val > initial_val else 0.0

                results.append({
                    "simulation_num": sim,
                    "ticker": ticker,
                    "year": year,
                    "starting_value": starting_val,
                    "ending_value": ending_val,
                    "annual_return": annual_return,
                    "cumulative_return": cumulative_return,
                    "volatility": volatility,
                    "probability": probability
                })

                starting_val = ending_val

    return pd.DataFrame(results)


# Needed to create a different transform function due to different columns from live data
def transform_monte_carlo_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and validate Monte Carlo simulation results.
    Ensures correct types, fills missing values, and sorts by ticker and simulation.
    """
    if df.empty:
        return pd.DataFrame(columns=[
            'simulation_num', 'ticker', 'year',
            'starting_value', 'ending_value', 'annual_return',
            'cumulative_return', 'volatility', 'probability'
        ])

    df = df.copy()

    # Ensuring correct data types
    df['ticker'] = df['ticker'].astype(str).str.upper()
    df['simulation_num'] = df['simulation_num'].astype(int)
    df['year'] = df['year'].astype(int)
    numeric_cols = [
        'starting_value', 'ending_value',
        'annual_return', 'cumulative_return',
        'volatility', 'probability'
    ]
    df[numeric_cols] = df[numeric_cols].apply(
        pd.to_numeric, errors='coerce'
    )

    # Fill any missing values with 0
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # Sort by ticker, simulation_num, and year
    df = df.sort_values(
        ['ticker', 'simulation_num', 'year']
    ).reset_index(drop=True)

    return df
