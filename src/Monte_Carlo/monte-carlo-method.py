port_mean #mean of portfolio
port_stdev #standard deviation of portfolio
dt = 1/days #time step to account for everyday in the year

for i in range(num_simulations):
    #induced randomness in stock spikes (downwards or upwards), will produce a dice roll like array of random spikes
    spikes = np.random.normal(0, 1, days)

    #GBM formula: Tomorrow's Price = S(t) * exp((μ - 0.5σ^2)dt + σ * ε√dt)
    daily_returns = np.exp((port_mean - 0.5 * port_stdev**2) * dt + port_stdev * spikes * np.sqrt(dt))

    #initial investment would be our starting value, multiplied by the cumulative product of all growth factors we simulated regarding the daily returns
    price_path = initial_investment * np.cumprod(daily_returns)

    #result array
    simulation_results[:, i] = price_path
