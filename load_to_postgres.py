# load_to_postgres.py
"""
Load cleaned CSV data into PostgreSQL staging tables
"""
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import logging
from pathlib import Path
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'bank_reviews_dw',
    'user': 'dw_user',  # Update with your username
    'password': 'your_secure_password',  # Update with your password
    'port': 5432
}

def create_connection():
    """Create database connection"""
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)
    return engine

def clean_branches_data(df):
    """Clean branches dataframe before loading"""
    logger.info("Cleaning branches data...")
    
    # Remove unnecessary columns
    columns_to_drop = ['original_address', 'phone']
    for col in columns_to_drop:
        if col in df.columns:
            df = df.drop(columns=[col])
    
    # Use enriched_address as the main address
    if 'enriched_address' in df.columns:
        df['address'] = df['enriched_address']
        df = df.drop(columns=['enriched_address'])
    
    # Clean missing values
    df['address'] = df['address'].fillna('')
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df['review_count'] = pd.to_numeric(df['review_count'], errors='coerce').fillna(0).astype(int)
    
    logger.info(f"Branches data cleaned. Shape: {df.shape}")
    return df

def clean_reviews_data(df):
    """Clean reviews dataframe before loading"""
    logger.info("Cleaning reviews data...")
    
    # Remove unnecessary columns
    columns_to_drop = ['response_from_owner', 'helpful_count']
    for col in columns_to_drop:
        if col in df.columns:
            df = df.drop(columns=[col])
    
    # Clean data types
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
    df['review_date_normalized'] = pd.to_datetime(df['review_date_normalized'], errors='coerce')
    
    # Clean text fields
    df['review_text'] = df['review_text'].fillna('')
    df['reviewer_name'] = df['reviewer_name'].fillna('Anonymous')
    df['branch_address'] = df['branch_address'].fillna('')
    
    # Ensure year and month are integers
    df['review_year'] = pd.to_numeric(df['review_year'], errors='coerce').fillna(0).astype(int)
    df['review_month'] = pd.to_numeric(df['review_month'], errors='coerce').fillna(0).astype(int)
    
    logger.info(f"Reviews data cleaned. Shape: {df.shape}")
    return df

def create_staging_tables(engine):
    """Create staging tables in PostgreSQL"""
    
    with engine.connect() as conn:
        # Create schemas if they don't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))
        conn.commit()
        
        # Drop existing tables
        conn.execute(text("DROP TABLE IF EXISTS staging.stg_branches CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS staging.stg_reviews CASCADE;"))
        conn.commit()
        
        # Create branches staging table
        create_branches_table = """
        CREATE TABLE staging.stg_branches (
            id SERIAL PRIMARY KEY,
            bank_name VARCHAR(255) NOT NULL,
            branch_name VARCHAR(255) NOT NULL,
            branch_url TEXT,
            address TEXT,
            rating FLOAT,
            review_count INTEGER,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create reviews staging table
        create_reviews_table = """
        CREATE TABLE staging.stg_reviews (
            id SERIAL PRIMARY KEY,
            bank_name VARCHAR(255) NOT NULL,
            branch_name VARCHAR(255) NOT NULL,
            branch_address TEXT,
            branch_url TEXT,
            reviewer_name VARCHAR(255),
            rating FLOAT,
            review_text TEXT,
            review_date VARCHAR(100),
            scraped_at TIMESTAMP,
            review_date_normalized DATE,
            review_year INTEGER,
            review_month INTEGER,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create indexes for performance
        create_indexes = """
        CREATE INDEX idx_stg_branches_bank ON staging.stg_branches(bank_name);
        CREATE INDEX idx_stg_branches_url ON staging.stg_branches(branch_url);
        CREATE INDEX idx_stg_reviews_bank ON staging.stg_reviews(bank_name);
        CREATE INDEX idx_stg_reviews_branch ON staging.stg_reviews(branch_name);
        CREATE INDEX idx_stg_reviews_date ON staging.stg_reviews(review_date_normalized);
        """
        
        conn.execute(text(create_branches_table))
        conn.execute(text(create_reviews_table))
        conn.execute(text(create_indexes))
        conn.commit()
        
        logger.info("Staging tables created successfully")

def find_data_files():
    """Find the cleaned data files"""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    
    # Look for data/cleaned directory
    cleaned_dir = script_dir / "data" / "cleaned"
    
    if not cleaned_dir.exists():
        logger.error(f"Cleaned data directory not found: {cleaned_dir}")
        return None, None
    
    # Find the files
    branches_file = None
    reviews_file = None
    
    for file in cleaned_dir.glob("*.csv"):
        if "branches" in file.name.lower():
            branches_file = file
        elif "reviews" in file.name.lower():
            reviews_file = file
    
    logger.info(f"Found branches file: {branches_file}")
    logger.info(f"Found reviews file: {reviews_file}")
    
    return branches_file, reviews_file

def load_data():
    """Load cleaned CSV files to PostgreSQL"""
    
    # Create connection
    engine = create_connection()
    
    # Test connection
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return
    
    # Create tables
    create_staging_tables(engine)
    
    # Find data files
    branches_file, reviews_file = find_data_files()
    
    if not branches_file or not reviews_file:
        logger.error("Could not find data files")
        return
    
    # Load branches
    try:
        branches_df = pd.read_csv(branches_file)
        logger.info(f"Loaded branches CSV: {len(branches_df)} rows")
        
        # Clean the data
        branches_df = clean_branches_data(branches_df)
        
        # Load to database
        branches_df.to_sql('stg_branches', engine, schema='staging', 
                          if_exists='append', index=False)
        logger.info(f"Loaded {len(branches_df)} branches to staging")
    except Exception as e:
        logger.error(f"Error loading branches: {e}")
    
    # Load reviews
    try:
        reviews_df = pd.read_csv(reviews_file)
        logger.info(f"Loaded reviews CSV: {len(reviews_df)} rows")
        
        # Clean the data
        reviews_df = clean_reviews_data(reviews_df)
        
        # Load to database
        reviews_df.to_sql('stg_reviews', engine, schema='staging', 
                         if_exists='append', index=False, chunksize=1000)
        logger.info(f"Loaded {len(reviews_df)} reviews to staging")
    except Exception as e:
        logger.error(f"Error loading reviews: {e}")
    
    # Verify data
    try:
        with engine.connect() as conn:
            branch_count = conn.execute(text("SELECT COUNT(*) FROM staging.stg_branches")).scalar()
            review_count = conn.execute(text("SELECT COUNT(*) FROM staging.stg_reviews")).scalar()
            
            logger.info(f"Verification - Branches: {branch_count}, Reviews: {review_count}")
            
            # Show sample data
            logger.info("\nSample branches:")
            sample_branches = conn.execute(text("""
                SELECT bank_name, branch_name, address, rating 
                FROM staging.stg_branches 
                LIMIT 5
            """)).fetchall()
            for row in sample_branches:
                logger.info(f"  {row}")
            
            logger.info("\nSample reviews:")
            sample_reviews = conn.execute(text("""
                SELECT bank_name, rating, review_year, LENGTH(review_text) as text_length 
                FROM staging.stg_reviews 
                LIMIT 5
            """)).fetchall()
            for row in sample_reviews:
                logger.info(f"  {row}")
                
    except Exception as e:
        logger.error(f"Error verifying data: {e}")

if __name__ == "__main__":
    # First, let's check what files we have
    script_dir = Path(__file__).parent
    cleaned_dir = script_dir / "data" / "cleaned"
    
    print(f"Looking for files in: {cleaned_dir}")
    if cleaned_dir.exists():
        print("Files found:")
        for file in cleaned_dir.glob("*"):
            print(f"  - {file.name}")
    else:
        print("Directory not found!")
    
    # Now load the data
    load_data()