# Sinch Grill Data Warehouse Design Documentation

## Schema Design Overview

The data warehouse design follows a star schema pattern, which is optimized for analytical queries and reporting. This design consists of:

- **Fact Tables**: These contain the measurable, quantitative data about the business (orders, order items, order statuses)
- **Dimension Tables**: These contain descriptive attributes related to the facts (members, stores, campaigns, items, dates)

## Design Choices

### Star Schema vs. Snowflake Schema

I chose a star schema over a snowflake schema for several reasons:
- **Query Performance**: Star schemas generally perform better for analytical queries due to fewer joins
- **Simplicity**: Easier to understand and maintain
- **Flexibility**: Better suited for ad-hoc queries

### Dimension Tables

#### 1. dim_members
- Contains all member information
- Primary key is member_id
- Includes membership type as a denormalized attribute rather than creating a separate membership_types dimension to reduce joins

#### 2. dim_member_preferences
- Contains member preferences
- Could have been included in dim_members, but separated to handle the many-to-one relationship between preferences and members

#### 3. dim_stores
- Contains store information
- Primary key is store_id

#### 4. dim_campaigns
- Contains marketing campaign details
- Primary key is campaign_id
- Includes store_id as a foreign key to link campaigns to specific stores

#### 5. dim_items
- Contains product catalog information
- Primary key is item_id
- Item names are stored with a UNIQUE constraint

#### 6. dim_dates
- A date dimension table for time-based analysis
- Includes various date attributes for time-based reporting
- Primary key is date_id (a date value)

### Fact Tables

#### 1. fact_orders
- Primary fact table for financial analysis
- Contains order header information
- Primary key is order_id
- Foreign keys to dimension tables: member_id, store_id, campaign_id, order_date
- Contains financial measures: subtotal and total

#### 2. fact_order_items
- Contains line item details for each order
- Composite foreign key relationship to fact_orders and dim_items
- Contains the price of each item

#### 3. fact_order_status
- Contains status history for each order
- Allows tracking of order progression through different statuses
- Enables calculation of processing and delivery times

## Performance Optimizations

### Indexes
- Created indexes on all foreign keys in fact tables to improve join performance
- Created additional indexes on commonly filtered fields (e.g., status in fact_order_status)
- Created a unique index on item_name in dim_items to ensure uniqueness and improve lookup performance

### Data Types
- Used appropriate data types for each column to minimize storage requirements
- Used NUMERIC(10,2) for financial values to ensure precision

### Partitioning Considerations
- For larger datasets, fact tables could be partitioned by date to improve query performance on recent data
- This partitioning strategy would require additional implementation in a production environment

## ETL Process

The ETL process follows these steps:

1. Load dimension tables first:
   - dim_dates (generated date range)
   - dim_stores (extracted from order data)
   - dim_members (from members.csv)
   - dim_campaigns (from marketing.csv)
   - dim_items (extracted from order_items.csv)
   - dim_member_preferences (from preferences.csv)

2. Load fact tables after dimensions:
   - fact_orders (from order.csv)
   - fact_order_items (from order_items.csv)
   - fact_order_status (from order_status.csv)

This sequence ensures that all foreign key relationships are satisfied during loading.

## Design Justifications for Required Use Cases

### 1. Analyze gross revenue by campaign, store, customer, and membership type
- fact_orders contains the necessary financial measures
- Foreign keys to dim_campaigns, dim_stores, and dim_members allow for easy aggregation and filtering
- The first SQL analysis query demonstrates this capability

### 2. Identify the most and least popular items
- fact_order_items linked to dim_items allows counting of items ordered
- The third SQL analysis query provides this functionality by ranking items based on order counts

### 3. Assess average order processing and delivery times
- fact_order_status tracks status changes with timestamps
- This enables calculation of time differences between statuses
- The fourth SQL analysis query demonstrates this calculation

## Assumptions Made

1. **Data Quality**:
   - All CSV data is clean and does not contain duplicates
   - Date formats are consistent across all files (YYYY-MM-DD)
   - Foreign key relationships are valid (e.g., all member_ids in orders exist in members)

2. **Business Rules**:
   - Orders always progress through a standard flow (Submitted → In Progress → Delivered)
   - Order items do not change after an order is placed
   - Campaigns are associated with specific stores

3. **Data Model Simplifications**:
   - Simplified the handling of nested items (combo meals) by treating each as a separate item
   - Did not implement a separate hierarchy for membership types
   - Used a store name generation pattern rather than actual store names (since they weren't in the data)

## Future Enhancements

1. **Data Partitioning**:
   - Implement table partitioning on fact tables for larger datasets
   - Consider partitioning by date for improved query performance

2. **Data Validation**:
   - Add more robust data validation in the ETL process
   - Implement error handling for missing or invalid data

3. **Additional Dimensions**:
   - Add time dimension for time-of-day analysis
   - Create a more detailed item hierarchy for better categorization

4. **Performance Monitoring**:
   - Add execution metrics to ETL process
   - Implement query performance monitoring

## Conclusion

This data warehouse design meets all the requirements specified in the assignment while maintaining good performance characteristics through proper indexing and schema design. The star schema approach provides a balance between query performance and maintenance simplicity.