import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import datetime
import logging
from datetime import datetime as dt
import numpy as np

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("sinch_grill_etl")

# Database connection parameters
db_params = {
    "host": "localhost",
    "port": 5432,
    "database": "sinch_grill",
    "user": "postgres",
    "password": "mysecretpassword"
}

def get_db_connection():
    """Establish a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def truncate_tables(conn):
    """Truncate all tables in the correct order to respect foreign key constraints"""
    logger.info("Truncating tables...")
    
    cursor = conn.cursor()
    
    try:
        # Disable triggers temporarily to allow truncating tables with foreign keys
        cursor.execute("SET session_replication_role = 'replica';")
        
        # Truncate fact tables first (they reference dimension tables)
        cursor.execute("TRUNCATE TABLE fact_order_status CASCADE;")
        cursor.execute("TRUNCATE TABLE fact_order_items CASCADE;")
        cursor.execute("TRUNCATE TABLE fact_orders CASCADE;")
        
        # Then truncate dimension tables
        cursor.execute("TRUNCATE TABLE dim_member_preferences CASCADE;")
        cursor.execute("TRUNCATE TABLE dim_items CASCADE;")
        cursor.execute("TRUNCATE TABLE dim_campaigns CASCADE;")
        cursor.execute("TRUNCATE TABLE dim_members CASCADE;")
        cursor.execute("TRUNCATE TABLE dim_stores CASCADE;")
        cursor.execute("TRUNCATE TABLE dim_dates CASCADE;")
        
        # Re-enable triggers
        cursor.execute("SET session_replication_role = 'origin';")
        
        conn.commit()
        logger.info("All tables truncated successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error truncating tables: {e}")
        raise
    finally:
        cursor.close()

def parse_date(date_str):
    """Parse date string and return in PostgreSQL-compatible format"""
    if pd.isna(date_str):
        return None
        
    try:
        # Try to parse as day/month/year
        date_obj = dt.strptime(date_str, '%d/%m/%Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        try:
            # Try to parse as year-month-day
            date_obj = dt.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            logger.warning(f"Could not parse date: {date_str}")
            return None

def create_dummy_campaign(conn):
    """Create a dummy campaign for NULL campaign IDs"""
    logger.info("Creating dummy campaign for NULL values...")
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        INSERT INTO dim_campaigns (campaign_id, target_audience, store_id, campaign_start_date, campaign_end_date)
        VALUES ('NONE', 'No Campaign', 'NONE', '2020-01-01', '2099-12-31')
        ON CONFLICT (campaign_id) DO NOTHING
        """)
        conn.commit()
        logger.info("Dummy campaign created successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating dummy campaign: {e}")
        raise
    finally:
        cursor.close()

def create_dummy_store(conn):
    """Create a dummy store for NULL store IDs"""
    logger.info("Creating dummy store for NULL values...")
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        INSERT INTO dim_stores (store_id, store_name)
        VALUES ('NONE', 'No Store')
        ON CONFLICT (store_id) DO NOTHING
        """)
        conn.commit()
        logger.info("Dummy store created successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating dummy store: {e}")
        raise
    finally:
        cursor.close()

def load_dates(conn, start_date='2020-01-01', end_date='2025-12-31'):
    """Create and load the date dimension table"""
    logger.info("Loading date dimension table...")
    
    cursor = conn.cursor()
    
    # Convert string dates to datetime objects
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    
    # Generate date range
    dates = []
    current_date = start
    while current_date <= end:
        date_id = current_date.strftime('%Y-%m-%d')
        day_of_week = current_date.weekday()
        day_name = current_date.strftime('%A')
        month = current_date.month
        month_name = current_date.strftime('%B')
        quarter = (month - 1) // 3 + 1
        year = current_date.year
        is_weekend = day_of_week >= 5  # 5 and 6 are Saturday and Sunday
        
        dates.append((date_id, day_of_week, day_name, month, month_name, quarter, year, is_weekend))
        current_date += datetime.timedelta(days=1)
    
    # Insert dates into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_dates (date_id, day_of_week, day_name, month, month_name, quarter, year, is_weekend)
            VALUES %s
            ON CONFLICT (date_id) DO NOTHING
            """,
            dates
        )
        conn.commit()
        logger.info(f"Loaded {len(dates)} records into dim_dates")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading date dimension: {e}")
        raise
    finally:
        cursor.close()

def load_stores(conn):
    """Create and load the stores from order data"""
    logger.info("Loading store dimension...")
    
    cursor = conn.cursor()
    
    # Extract unique stores from orders
    df = pd.read_csv('order.csv')
    store_ids = df['StoreID'].dropna().unique()
    
    # Prepare store data
    stores = [(store_id, f"Store {store_id}") for store_id in store_ids]
    
    # Insert stores into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_stores (store_id, store_name)
            VALUES %s
            ON CONFLICT (store_id) DO NOTHING
            """,
            stores
        )
        conn.commit()
        logger.info(f"Loaded {len(stores)} records into dim_stores")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading stores: {e}")
        raise
    finally:
        cursor.close()

def load_members(conn):
    """Load members data into the dimension table"""
    logger.info("Loading member dimension...")
    
    cursor = conn.cursor()
    
    # Read members data
    df = pd.read_csv('members.csv')
    
    # Prepare member data with date format conversion
    members = []
    for _, row in df.iterrows():
        join_date = parse_date(row['JoinDate'])
        expiration_date = parse_date(row['ExpirationDate'])
        
        if join_date and expiration_date:
            members.append((
                row['Id'], 
                row['Name'], 
                row['MembershipType'], 
                join_date, 
                expiration_date
            ))
    
    # Insert members into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_members (member_id, member_name, membership_type, join_date, expiration_date)
            VALUES %s
            ON CONFLICT (member_id) DO NOTHING
            """,
            members
        )
        conn.commit()
        logger.info(f"Loaded {len(members)} records into dim_members")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading members: {e}")
        raise
    finally:
        cursor.close()

def load_member_preferences(conn):
    """Load member preferences into the dimension table"""
    logger.info("Loading member preferences dimension...")
    
    cursor = conn.cursor()
    
    # Read preferences data
    df = pd.read_csv('preferences.csv')
    
    # Prepare preference data
    preferences = [
        (row['MemberID'], row['Preference'])
        for _, row in df.iterrows() if not pd.isna(row['MemberID']) and not pd.isna(row['Preference'])
    ]
    
    # Insert preferences into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_member_preferences (member_id, preference_description)
            VALUES %s
            """,
            preferences
        )
        conn.commit()
        logger.info(f"Loaded {len(preferences)} records into dim_member_preferences")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading preferences: {e}")
        raise
    finally:
        cursor.close()

def load_campaigns(conn):
    """Load campaigns data into the dimension table"""
    logger.info("Loading campaign dimension...")
    
    cursor = conn.cursor()
    
    # Read campaigns data
    df = pd.read_csv('marketing.csv')
    
    # Prepare campaign data with date format conversion
    campaigns = []
    for _, row in df.iterrows():
        start_date = parse_date(row['CampaignStartDate'])
        end_date = parse_date(row['CampaignEndDate'])
        
        if start_date and end_date:
            campaigns.append((
                row['CampaignID'], 
                row['TargetAudience'], 
                row['StoreID'], 
                start_date, 
                end_date
            ))
    
    # Insert campaigns into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_campaigns (campaign_id, target_audience, store_id, campaign_start_date, campaign_end_date)
            VALUES %s
            ON CONFLICT (campaign_id) DO NOTHING
            """,
            campaigns
        )
        conn.commit()
        logger.info(f"Loaded {len(campaigns)} records into dim_campaigns")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading campaigns: {e}")
        raise
    finally:
        cursor.close()

def load_items(conn):
    """Load items data into the dimension table"""
    logger.info("Loading item dimension...")
    
    cursor = conn.cursor()
    
    # Read order items data
    df = pd.read_csv('order_items.csv')
    
    # Extract unique items
    unique_items = df['ItemName'].dropna().unique()
    
    # Prepare item data
    items = [(item_name,) for item_name in unique_items]
    
    # Insert items into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_items (item_name)
            VALUES %s
            ON CONFLICT (item_name) DO NOTHING
            """,
            items
        )
        conn.commit()
        logger.info(f"Loaded {len(items)} records into dim_items")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading items: {e}")
        raise
    finally:
        cursor.close()

def load_orders(conn):
    """Load orders data into the fact table"""
    logger.info("Loading orders fact table...")
    
    cursor = conn.cursor()
    
    # Read orders data
    df = pd.read_csv('order.csv')
    
    # Prepare order data with date format conversion and handle NaN values
    orders = []
    for _, row in df.iterrows():
        order_date = parse_date(row['OrderDate'])
        
        # Handle NULL values
        member_id = row['MemberID'] if not pd.isna(row['MemberID']) else None
        store_id = row['StoreID'] if not pd.isna(row['StoreID']) else 'NONE'
        campaign_id = row['CampaignID'] if not pd.isna(row['CampaignID']) else 'NONE'
        
        if order_date:
            orders.append((
                row['OrderID'], 
                member_id, 
                store_id, 
                campaign_id, 
                order_date, 
                row['SubTotal'], 
                row['Total']
            ))
    
    # Insert orders into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO fact_orders (order_id, member_id, store_id, campaign_id, order_date, subtotal, total)
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
            """,
            orders
        )
        conn.commit()
        logger.info(f"Loaded {len(orders)} records into fact_orders")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading orders: {e}")
        raise
    finally:
        cursor.close()

def load_order_items(conn):
    """Load order items data into the fact table"""
    logger.info("Loading order items fact table...")
    
    cursor = conn.cursor()
    
    # Read order items data
    df = pd.read_csv('order_items.csv')
    
    # Filter out rows with NaN values
    df = df.dropna(subset=['OrderID', 'ItemName'])
    
    # First, get the item_id mapping
    cursor.execute("SELECT item_id, item_name FROM dim_items")
    item_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Prepare order items data
    order_items = []
    for _, row in df.iterrows():
        item_id = item_mapping.get(row['ItemName'])
        if item_id:
            order_items.append((row['OrderID'], item_id, row['Price']))
    
    # Insert order items into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO fact_order_items (order_id, item_id, price)
            VALUES %s
            """,
            order_items
        )
        conn.commit()
        logger.info(f"Loaded {len(order_items)} records into fact_order_items")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading order items: {e}")
        raise
    finally:
        cursor.close()

def load_order_status(conn):
    """Load order status data into the fact table"""
    logger.info("Loading order status fact table...")
    
    cursor = conn.cursor()
    
    # Read order status data
    df = pd.read_csv('order_status.csv')
    
    # Filter out rows with NaN values
    df = df.dropna(subset=['OrderID', 'Status'])
    
    # Prepare order status data with timestamp format conversion
    order_statuses = []
    for _, row in df.iterrows():
        try:
            # Try to parse the timestamp
            timestamp_str = row['StatusTimestamp']
            if pd.isna(timestamp_str):
                continue
                
            # First try with time component
            try:
                timestamp = dt.strptime(timestamp_str, '%d/%m/%Y %H:%M:%S')
            except ValueError:
                # If that fails, try just the date
                timestamp = dt.strptime(timestamp_str, '%d/%m/%Y')
            
            order_statuses.append((
                row['OrderID'],
                row['Status'],
                timestamp.strftime('%Y-%m-%d %H:%M:%S')
            ))
        except ValueError as e:
            logger.warning(f"Could not parse timestamp for order {row['OrderID']}: {e}")
    
    # Insert order statuses into the table
    try:
        execute_values(
            cursor,
            """
            INSERT INTO fact_order_status (order_id, status, status_timestamp)
            VALUES %s
            """,
            order_statuses
        )
        conn.commit()
        logger.info(f"Loaded {len(order_statuses)} records into fact_order_status")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading order statuses: {e}")
        raise
    finally:
        cursor.close()

def main():
    """Main ETL process"""
    logger.info("Starting ETL process...")
    
    try:
        # Establish database connection
        conn = get_db_connection()
        
        # Truncate all tables before loading new data
        truncate_tables(conn)
        
        # Execute ETL processes
        load_dates(conn)
        load_stores(conn)
        
        # Create dummy entries for NULL foreign keys
        create_dummy_store(conn)
        
        load_members(conn)
        load_campaigns(conn)
        
        # Create dummy campaign after loading real campaigns
        create_dummy_campaign(conn)
        
        load_items(conn)
        load_member_preferences(conn)
        load_orders(conn)
        load_order_items(conn)
        load_order_status(conn)
        
        logger.info("ETL process completed successfully!")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()