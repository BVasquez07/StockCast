import psycopg #https://www.psycopg.org/psycopg3/docs/basic/usage.html
PSQL_USERNAME = 'brandon'
PSQL_PASSWORD = 'password'
PSQL_HOST_ADDR= '127.0.0.1' #better than using host param since it avoids the host name/DNS lookup
PSQL_PORT= '5432'
DB_NAME = 'test_db'
CONNECTION_TIMEOUT = 10


"""
TODO:

- create the database if it does not exist
- create the tables if they do not exist
- create the appropriate tables and PK's
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

#for refrence on the param kwargs: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
with psycopg.connect(f"hostaddr={PSQL_HOST_ADDR} port={PSQL_PORT} dbname={DB_NAME} user={PSQL_USERNAME} connect_timeout={CONNECTION_TIMEOUT}") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * from prices WHERE ticker=(%s)", ('NVDA',))
        print(cur.fetchall()) # can use this method or iterate over the cursor
        
        
        
        conn.commit()
        
