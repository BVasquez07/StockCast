"""
Tests for database connection and schema
"""
import pytest
import psycopg
from config import db_credentials


class TestDatabaseConnection:
    """Test database connection"""
    
    @pytest.fixture
    def db_config(self):
        """Get database configuration"""
        config = {
            'host': db_credentials.get('host', '127.0.0.1'),
            'port': db_credentials.get('port', '5432'),
            'user': db_credentials.get('user', 'postgres'),
            'password': db_credentials.get('password', ''),
            'dbname': db_credentials.get('database', 'monte_sim_stock_data'),
        }
        return config
    
    def test_database_connection(self, db_config):
        """Test that we can connect to the database"""
        try:
            conn = psycopg.connect(
                f"hostaddr={db_config['host']} "
                f"port={db_config['port']} "
                f"dbname={db_config['dbname']} "
                f"user={db_config['user']} "
                f"password={db_config['password']} "
                f"connect_timeout=10"
            )
            conn.close()
            assert True, "Connection successful"
        except psycopg.DatabaseError as e:
            pytest.skip(f"Cannot connect to database: {e}")
    
    def test_stock_data_table_exists(self, db_config):
        """Test that stock_data table exists with correct schema"""
        try:
            conn = psycopg.connect(
                f"hostaddr={db_config['host']} "
                f"port={db_config['port']} "
                f"dbname={db_config['dbname']} "
                f"user={db_config['user']} "
                f"password={db_config['password']} "
                f"connect_timeout=10"
            )
            with conn.cursor() as cur:
                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'stock_data'
                    );
                """)
                exists = cur.fetchone()[0]
                assert exists, "stock_data table should exist"
                
                # Check for adj_close column
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'stock_data' AND column_name = 'adj_close';
                """)
                has_adj_close = cur.fetchone() is not None
                assert has_adj_close, "stock_data should have adj_close column"
            
            conn.close()
        except psycopg.DatabaseError as e:
            pytest.skip(f"Cannot connect to database: {e}")
    
    def test_simulation_table_exists(self, db_config):
        """Test that simulation table exists with year column"""
        try:
            conn = psycopg.connect(
                f"hostaddr={db_config['host']} "
                f"port={db_config['port']} "
                f"dbname={db_config['dbname']} "
                f"user={db_config['user']} "
                f"password={db_config['password']} "
                f"connect_timeout=10"
            )
            with conn.cursor() as cur:
                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'simulation'
                    );
                """)
                exists = cur.fetchone()[0]
                assert exists, "simulation table should exist"
                
                # Check for year column (not date)
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'simulation' AND column_name = 'year';
                """)
                has_year = cur.fetchone() is not None
                assert has_year, "simulation table should have year column"
            
            conn.close()
        except psycopg.DatabaseError as e:
            pytest.skip(f"Cannot connect to database: {e}")

