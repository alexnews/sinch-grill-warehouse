# Sinch Grill Data Warehouse

This repository contains a complete data warehousing solution for the fictional "Sinch Grill" business, developed as part of the Sinch RTCx Data Engineer Assignment.

## Project Overview

The Sinch Grill data warehouse is designed to support analytical queries across three business domains:

1. **Members Domain** - Manages member data and preferences
2. **Marketing Domain** - Tracks marketing campaigns
3. **Orders Domain** - Processes customer orders and tracks their fulfillment

The solution includes:
- A star schema data warehouse design
- ETL scripts to load data from CSV files
- SQL queries for business intelligence reporting

## Repository Structure

```
sinch-grill-warehouse/
├── Data_Engineer_Assignment.zip # Data Engineer Assigments (provided)
├── README.md                    # Project overview and setup instructions
├── docs/
│   ├── erd.png                  # ERD diagram
│   ├── erd.mermaid              # ERD diagram in Mermaid format
│   └── design_documentation.md  # Design documentation
├── sql/
│   ├── create_schema.sql    # Database schema creation script
│   └── analysis_queries.sql # Analysis SQL queries
├── etl/
│   ├── etl.py                   # Main ETL script
│   └── requirements.txt         # Python dependencies
└── docker/
    └── docker-compose.yml       # Docker setup (provided)
    └── servers.json             # part of the pgAdmin configuration in the Docker setup


## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Git

### Installation Steps

1. Clone this repository:
   ```bash
   git clone https://github.com/alexnews/sinch-grill-warehouse.git
   cd sinch-grill-warehouse
   ```

2. Start the Docker containers:
   ```bash
   cd docker
   docker-compose up -d
   ```

3. Access pgAdmin at http://localhost:5050 and connect to PostgreSQL:
   - Host: postgres
   - Port: 5432
   - Username: postgres
   - Password: mysecretpassword

4. Create the database and schema:
   - Use pgAdmin to create a new database named "sinch_grill"
   - Run the schema creation script from `sql/schema/create_schema.sql`

5. Install Python dependencies:
   ```bash
   pip install -r etl/requirements.txt
   ```

6. Run the ETL script to load data:
   ```bash
   cd etl
   python etl.py
   ```

7. Run the analysis queries in pgAdmin using the scripts in `sql/analysis_queries.sql`

## Data Warehouse Design

The data warehouse follows a star schema design with:

- **Dimension Tables**:
  - dim_members
  - dim_member_preferences
  - dim_stores
  - dim_campaigns
  - dim_items
  - dim_dates

- **Fact Tables**:
  - fact_orders
  - fact_order_items
  - fact_order_status

For detailed information about design choices and optimizations, see [Design Documentation](docs/design_documentation.md).

## Analysis Capabilities

The data warehouse supports the following analytics:

1. Revenue analysis by campaign, store, customer, and membership type
2. Identification of most and least popular items
3. Assessment of order processing and delivery times
4. Monthly highest and lowest spending members

## Python ETL Process

The ETL process is implemented in Python and handles:

1. Loading dimension tables first to ensure referential integrity
2. Loading fact tables with proper foreign key relationships
3. Data transformation as needed for the warehouse schema

## License

This project is for demonstration purposes as part of an assignment.
