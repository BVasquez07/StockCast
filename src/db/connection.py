import psycopg #https://www.psycopg.org/psycopg3/docs/basic/usage.html
"""
TODO:
- error handling
    -> timeout handling
"""

def psql_connect_and_setup(db_host_addr: str, db_port: str, db_name: str, db_user: str, db_password: str, db_timeout: int) -> None: #cool thing to look into is how to make use of the *args and **kwargs in python functions
    """
    Connects to PostgreSQL database using credentials from config.
    Creates the database and necessary tables if they do not exist.
    """

    try:
        psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname={db_name} user={db_user} password={db_password} connect_timeout={db_timeout}").close()
    except psycopg.DatabaseError:
        #creates the database if it does not exist by connecting to the default 'postgres' database
        with psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname=postgres user={db_user} password={db_password} connect_timeout={db_timeout}") as conn:
            conn.autocommit = True #this helps as it automatically commits each statement without needing to call conn.commit()!!!
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE {db_name};")
    finally:
        print(f'Connected to database {db_name} successfully!')

    #for refrence on the param kwargs: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    with psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname={db_name} user={db_user} password={db_password} connect_timeout={db_timeout}") as conn:
        with conn.cursor() as cur:
            #create the stock_data table if it does not exist...
            # Data Model: stock_data table with all OHLCV data + adj_close
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    id SERIAL PRIMARY KEY,
                    ticker varchar(10) NOT NULL,
                    date date NOT NULL,
                    open float,
                    high float,
                    low float,
                    close float,
                    adj_close float,
                    volume integer,
                    UNIQUE (ticker, date));
            """)
            
            
            #create the simulation table if it does not exist....
            # Data Model: simulation table with year instead of date
            cur.execute("""
                CREATE TABLE IF NOT EXISTS simulation (
                    id SERIAL PRIMARY KEY,
                    simulation_num integer,
                    ticker varchar(10) NOT NULL,
                    year integer NOT NULL,
                    starting_value float,
                    ending_value float,
                    annual_return float,
                    cumulative_return float,
                    volatility float,
                    probability float);
            """)

            #lets put insertion query here then we can print it with the code below
            conn.commit()
        
