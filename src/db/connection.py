import psycopg #https://www.psycopg.org/psycopg3/docs/basic/usage.html
PSQL_USERNAME = 'postgres'
PSQL_PASSWORD = '0000'
PSQL_HOST_ADDR= '127.0.0.1' #better than using host param since it avoids the host name/DNS lookup
PSQL_PORT= '8080'
DB_NAME = 'monte_sim_stock_data' #this is the default database created on installation so we will connect to it to check if the other we need is in there!
CONNECTION_TIMEOUT = 10


"""
TODO:


- implement threading or async to speed up the connection and insertion process
 -> threading in psycopg3 would usually be done with connection pool!
 -> might not be
- a lot of error handling has to be done
    - need to make sure that the server/host is ok
    -
- I think most if not all of the processing can be done in one file
    - Otherwise pass the connection object around

Processing is as follows:

-> Transformed and simulated data is ready 
    -> check if db & tables ready
        -> if not create them
    -> connect to the db
    -> insert the data into the appropriate tables
"""
try:
    psycopg.connect(f"hostaddr={PSQL_HOST_ADDR} port={PSQL_PORT} dbname={DB_NAME} user={PSQL_USERNAME} password={PSQL_PASSWORD} connect_timeout={CONNECTION_TIMEOUT}").close()
except psycopg.DatabaseError:
    #creates the database if it does not exist by connecting to the default 'postgres' database
    with psycopg.connect(f"hostaddr={PSQL_HOST_ADDR} port={PSQL_PORT} dbname=postgres user={PSQL_USERNAME} password={PSQL_PASSWORD} connect_timeout={CONNECTION_TIMEOUT}") as conn:
        conn.autocommit = True #this helps as it automatically commits each statement without needing to call conn.commit()!!!
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {DB_NAME};")
finally:
    print(f'Connected to database {DB_NAME} successfully!')

#for refrence on the param kwargs: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
with psycopg.connect(f"hostaddr={PSQL_HOST_ADDR} port={PSQL_PORT} dbname={DB_NAME} user={PSQL_USERNAME} password={PSQL_PASSWORD} connect_timeout={CONNECTION_TIMEOUT}") as conn:
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
                volume integer);
        """)
        
        # Add adj_close column if it doesn't exist (for existing databases)
        cur.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='stock_data' AND column_name='adj_close'
                ) THEN
                    ALTER TABLE stock_data ADD COLUMN adj_close float;
                END IF;
            END $$;
        """)
        
        #create the simulation table if it does not exist....
        # Data Model: simulation table with year instead of date
        cur.execute("""
            CREATE TABLE IF NOT EXISTS simulation (
                id integer NOT NULL,
                simulation_id SERIAL PRIMARY KEY,
                ticker varchar(10) NOT NULL,
                year integer NOT NULL,
                starting_value float,
                ending_value float,
                annual_return float,
                cumulative_return float,
                volatility float,
                probability float,
                FOREIGN KEY (id) REFERENCES stock_data(id) ON UPDATE CASCADE ON DELETE CASCADE);
        """)
        
        # Add year column if it doesn't exist (migration from date to year)
        cur.execute("""
            DO $$ 
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='simulation' AND column_name='date'
                ) AND NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='simulation' AND column_name='year'
                ) THEN
                    ALTER TABLE simulation ADD COLUMN year integer;
                    UPDATE simulation SET year = EXTRACT(YEAR FROM date) WHERE year IS NULL;
                    ALTER TABLE simulation ALTER COLUMN year SET NOT NULL;
                    ALTER TABLE simulation DROP COLUMN date;
                END IF;
            END $$;
        """)

        #lets put insertion query here then we can print it with the code below

        cur.execute("SELECT * from stock_data;")
        print(cur.fetchall()) # can use this method or iterate over the cursor...

        cur.execute("SELECT * from simulation;")
        print(cur.fetchall())

        conn.commit()
        
