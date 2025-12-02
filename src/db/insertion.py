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


def insert_data(db_host_addr: str, db_port: str, db_name: str, db_user: str, db_password: str, db_timeout: int, data: list[dict[str]]) -> None:
    with psycopg.connect(f"hostaddr={db_host_addr} port={db_port} dbname={db_name} user={db_user} password={db_password} connect_timeout={db_timeout}") as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO stock_data (ticker, date, open, high, low, close, adj_close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, data) #data needs to be a list of tuples [('ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'), (...), ...]
            #this will print out the data to make sure we actually inserted the proper data AND IS TEMPORARY FOR TESTING PURPOSES
            cur.execute("SELECT * from stock_data;")
            print(cur.fetchall()) # can use this method or iterate over the cursor...
            cur.execute("SELECT * from simulation;")
            print(cur.fetchall())

            conn.commit()


            