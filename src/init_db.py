import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from src.config import settings
import time
import sys

from src.models import create_tables

def wait_for_db(max_retries=5, retry_interval=2):
    """Wait for the PostgreSQL server to be ready"""
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(
                user=settings.db_user,
                password=settings.db_password,
                host=settings.db_host,
                port=settings.db_port,
                database='postgres'
            )
            conn.close()
            return True
        except psycopg2.OperationalError:
            retries += 1
            if retries == max_retries:
                return False
            print(f"Database not ready. Retrying in {retry_interval} seconds... ({retries}/{max_retries})")
            time.sleep(retry_interval)
    return False

def init_database():
    """Initialize the database if it doesn't exist"""
    if not wait_for_db():
        print("Could not connect to PostgreSQL server. Please check if it's running.")
        sys.exit(1)

    try:
        # Connect to PostgreSQL server (to create database)
        conn = psycopg2.connect(
            user=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
            database='postgres'  
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        # Create a cursor
        cur = conn.cursor()
        
        try:
            # Check if database exists
            cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{settings.db_name}'")
            exists = cur.fetchone()
            
            if not exists:
                print(f"Creating database {settings.db_name}")
                cur.execute(f'CREATE DATABASE {settings.db_name}')
                print("Database created successfully")
            else:
                print(f"Database {settings.db_name} already exists")
        
        finally:
            cur.close()
            conn.close()

    except psycopg2.Error as e:
        print(f"Error initializing database: {e}")
        sys.exit(1)

def create_schema():
    """Create database schema"""
    try:
        create_tables()
        print("Database schema created successfully")
    except Exception as e:
        print(f"Error creating database schema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("Initializing database...")
    init_database()
    print("Creating tables...")
    create_schema()
    print("Database initialization completed") 