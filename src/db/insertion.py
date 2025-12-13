import psycopg #https://www.psycopg.org/psycopg3/docs/basic/usage.html
"""
TODO:
- implement threading or async to speed up the connection and insertion process
 -> threading in psycopg3 would usually be done with connection pool!
 -> might not be
- error handling
    -> timeout handling
    -> ticker error handling
    -> implement simulaton data insertion when simulation is added to the Transform stage!

Processing is as follows:

-> Transformed and simulated data is ready 
    -> check if db & tables ready
        -> if not create them
    -> connect to the db
    -> insert the data into the appropriate tables
"""


def insert_stock_data(db_host_addr: str, db_port: str, db_name: str, db_user: str, db_password: str, db_timeout: int, data: list[dict[str]]) -> None:
    with psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname={db_name} user={db_user} password={db_password} connect_timeout={db_timeout}") as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO stock_data (ticker, date, open, high, low, close, adj_close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, data) #data needs to be a list of tuples [('ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'), (...), ...]
            conn.commit()

def insert_sim_data(db_host_addr: str, db_port: str, db_name: str, db_user: str, db_password: str, db_timeout: int, data: list[dict[str]]) -> None:
    with psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname={db_name} user={db_user} password={db_password} connect_timeout={db_timeout}") as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO simulation (id, simulation_num, ticker, year, starting_value, ending_value, annual_return, cumulative_return, volatility, probability)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, data)
            conn.commit()


            