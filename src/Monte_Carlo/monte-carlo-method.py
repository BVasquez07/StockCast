import yfinance as yf
import pandas as pd
import numpy as np

#probability column might need some work

#can pass any list of strings as tickers.

def run_monte_carlo(
    tickers: list[str],
    portfolio_value: float = 250000,
    years: int = 10,
    num_simulations: int = 1000,
    start_date: str = "2014-01-01",
    end_date: str = "2024-12-31"
):
    """
    Monte Carlo simulation returning a DataFrame with integer IDs.
    Columns: id, ticker, simulation_id, year, starting_value, ending_value,
             annual_return, cumulative_return, volatility, and probability
    """

    # Downloading historical adjusted close prices using start and end dates (10 year range)
    price_data = yf.download(tickers, start=start_date, end=end_date)["Adj Close"].dropna()

    # Compute daily log returns
    daily_returns = np.log(price_data / price_data.shift(1)).dropna()

    # Mean and covariance (allows us to preserve historical correlation) of daily returns
    mean_returns = daily_returns.mean()
    cov_matrix = daily_returns.cov()
    num_days = years * 252  # simulated trading days

    results = []
    row_id = 0  # sequential integer ID

    for sim in range(num_simulations):
        # Sample daily returns for all tickers
        simulated = np.random.multivariate_normal(
            mean_returns, cov_matrix, num_days
        )

        for i, ticker in enumerate(tickers):
            starting_val = portfolio_value / len(tickers)
            ticker_returns = simulated[:, i]
            growth_factors = np.exp(ticker_returns)
            ending_val = starting_val * growth_factors.prod()

            annual_return = (ending_val / starting_val) ** (1 / years) - 1
            cumulative_return = (ending_val / starting_val) - 1
            volatility = np.std(ticker_returns) * np.sqrt(252)
            probability = 1 / num_simulations  # placeholder for aggregation

            results.append({
                "id": row_id,
                "ticker": ticker,
                "simulation_id": sim,
                "year": years,
                "starting_value": starting_val,
                "ending_value": ending_val,
                "annual_return": annual_return,
                "cumulative_return": cumulative_return,
                "volatility": volatility,
                "probability": probability
            })

            row_id += 1  # increment ID

    return pd.DataFrame(results)
