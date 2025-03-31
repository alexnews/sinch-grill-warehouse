-- Dimension tables

-- Dimension: Stores
CREATE TABLE dim_stores (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(100),
    -- Additional store attributes can be added here
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Members
CREATE TABLE dim_members (
    member_id VARCHAR(50) PRIMARY KEY,
    member_name VARCHAR(100),
    membership_type VARCHAR(50),
    join_date DATE,
    expiration_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Member Preferences
CREATE TABLE dim_member_preferences (
    preference_id SERIAL PRIMARY KEY,
    member_id VARCHAR(50) REFERENCES dim_members(member_id),
    preference_description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Campaigns
CREATE TABLE dim_campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    target_audience VARCHAR(100),
    store_id VARCHAR(50) REFERENCES dim_stores(store_id),
    campaign_start_date DATE,
    campaign_end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Items
CREATE TABLE dim_items (
    item_id SERIAL PRIMARY KEY,
    item_name VARCHAR(100) UNIQUE,
    -- Additional item attributes can be added here
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Date (useful for time-based analysis)
CREATE TABLE dim_dates (
    date_id DATE PRIMARY KEY,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

-- Fact tables

-- Fact: Orders
CREATE TABLE fact_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    member_id VARCHAR(50) REFERENCES dim_members(member_id),
    store_id VARCHAR(50) REFERENCES dim_stores(store_id),
    campaign_id VARCHAR(50) REFERENCES dim_campaigns(campaign_id),
    order_date DATE REFERENCES dim_dates(date_id),
    subtotal NUMERIC(10, 2),
    total NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Order Items
CREATE TABLE fact_order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES fact_orders(order_id),
    item_id INTEGER REFERENCES dim_items(item_id),
    price NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Order Status
CREATE TABLE fact_order_status (
    status_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES fact_orders(order_id),
    status VARCHAR(50),
    status_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance optimization

-- Indexes on fact_orders
CREATE INDEX idx_fact_orders_member_id ON fact_orders(member_id);
CREATE INDEX idx_fact_orders_store_id ON fact_orders(store_id);
CREATE INDEX idx_fact_orders_campaign_id ON fact_orders(campaign_id);
CREATE INDEX idx_fact_orders_order_date ON fact_orders(order_date);

-- Indexes on fact_order_items
CREATE INDEX idx_fact_order_items_order_id ON fact_order_items(order_id);
CREATE INDEX idx_fact_order_items_item_id ON fact_order_items(item_id);

-- Indexes on fact_order_status
CREATE INDEX idx_fact_order_status_order_id ON fact_order_status(order_id);
CREATE INDEX idx_fact_order_status_status ON fact_order_status(status);
CREATE INDEX idx_fact_order_status_timestamp ON fact_order_status(status_timestamp);

-- Index on member preferences
CREATE INDEX idx_member_preferences_member_id ON dim_member_preferences(member_id);