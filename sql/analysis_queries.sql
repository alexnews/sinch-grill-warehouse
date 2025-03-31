-- 1. Report gross revenue by store and campaign
-- This query provides a breakdown of total revenue by store and campaign
SELECT 
    s.store_id,
    s.store_name,
    c.campaign_id,
    c.target_audience,
    TO_CHAR(c.campaign_start_date, 'YYYY-MM-DD') AS campaign_start,
    TO_CHAR(c.campaign_end_date, 'YYYY-MM-DD') AS campaign_end,
    SUM(o.total) AS gross_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total) / COUNT(DISTINCT o.order_id) AS average_order_value
FROM 
    fact_orders o
JOIN 
    dim_stores s ON o.store_id = s.store_id
JOIN 
    dim_campaigns c ON o.campaign_id = c.campaign_id
GROUP BY 
    s.store_id, s.store_name, c.campaign_id, c.target_audience, c.campaign_start_date, c.campaign_end_date
ORDER BY 
    s.store_id, gross_revenue DESC;

-- 2. Report on highest and lowest spending members for each month
-- This query identifies top and bottom spenders by month
WITH monthly_member_spending AS (
    SELECT 
        EXTRACT(YEAR FROM d.date_id) AS year,
        EXTRACT(MONTH FROM d.date_id) AS month,
        m.member_id,
        m.member_name,
        m.membership_type,
        SUM(o.total) AS total_spent,
        -- Rank members by spending within each month (highest to lowest)
        RANK() OVER (
            PARTITION BY EXTRACT(YEAR FROM d.date_id), EXTRACT(MONTH FROM d.date_id)
            ORDER BY SUM(o.total) DESC
        ) AS high_rank,
        -- Rank members by spending within each month (lowest to highest)
        RANK() OVER (
            PARTITION BY EXTRACT(YEAR FROM d.date_id), EXTRACT(MONTH FROM d.date_id)
            ORDER BY SUM(o.total) ASC
        ) AS low_rank
    FROM 
        fact_orders o
    JOIN 
        dim_dates d ON o.order_date = d.date_id
    JOIN 
        dim_members m ON o.member_id = m.member_id
    WHERE
        m.member_id IS NOT NULL
    GROUP BY 
        EXTRACT(YEAR FROM d.date_id), 
        EXTRACT(MONTH FROM d.date_id), 
        m.member_id, 
        m.member_name, 
        m.membership_type
)
SELECT 
    year,
    month,
    -- Using TO_CHAR to get month name
    TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') AS month_name,
    'Highest Spender' AS spending_category,
    member_id,
    member_name,
    membership_type,
    total_spent
FROM 
    monthly_member_spending
WHERE 
    high_rank = 1
UNION ALL
SELECT 
    year,
    month,
    TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') AS month_name,
    'Lowest Spender' AS spending_category,
    member_id,
    member_name,
    membership_type,
    total_spent
FROM 
    monthly_member_spending
WHERE 
    low_rank = 1
ORDER BY 
    year, month, spending_category;

-- 3. Retrieve the top 2 most and least popular items for each month
-- This query identifies the most and least ordered items by month
WITH monthly_item_popularity AS (
    SELECT 
        EXTRACT(YEAR FROM d.date_id) AS year,
        EXTRACT(MONTH FROM d.date_id) AS month,
        i.item_id,
        i.item_name,
        COUNT(oi.order_item_id) AS order_count,
        SUM(oi.price) AS total_revenue,
        -- Rank items by order count within each month (highest to lowest)
        RANK() OVER (
            PARTITION BY EXTRACT(YEAR FROM d.date_id), EXTRACT(MONTH FROM d.date_id)
            ORDER BY COUNT(oi.order_item_id) DESC
        ) AS popularity_rank_desc,
        -- Rank items by order count within each month (lowest to highest)
        RANK() OVER (
            PARTITION BY EXTRACT(YEAR FROM d.date_id), EXTRACT(MONTH FROM d.date_id)
            ORDER BY COUNT(oi.order_item_id) ASC
        ) AS popularity_rank_asc
    FROM 
        fact_order_items oi
    JOIN 
        fact_orders o ON oi.order_id = o.order_id
    JOIN 
        dim_dates d ON o.order_date = d.date_id
    JOIN 
        dim_items i ON oi.item_id = i.item_id
    GROUP BY 
        EXTRACT(YEAR FROM d.date_id),
        EXTRACT(MONTH FROM d.date_id),
        i.item_id, 
        i.item_name
)
SELECT 
    year,
    month,
    TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') AS month_name,
    'Most Popular' AS popularity_category,
    popularity_rank_desc AS rank,
    item_name,
    order_count,
    total_revenue
FROM 
    monthly_item_popularity
WHERE 
    popularity_rank_desc <= 2  -- Top 2 most popular
UNION ALL
SELECT 
    year,
    month,
    TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') AS month_name,
    'Least Popular' AS popularity_category,
    popularity_rank_asc AS rank,
    item_name,
    order_count,
    total_revenue
FROM 
    monthly_item_popularity
WHERE 
    popularity_rank_asc <= 2  -- Top 2 least popular
ORDER BY 
    year, month, popularity_category, rank;

-- 4. Calculate average order processing and delivery times per store
-- This query analyzes the time between different order statuses by store
WITH order_timing AS (
    SELECT 
        o.order_id,
        o.store_id,
        -- Get the timestamp for 'Submitted' status
        MIN(CASE WHEN os.status = 'Submitted' THEN os.status_timestamp END) AS submitted_time,
        -- Get the timestamp for 'In Progress' status
        MIN(CASE WHEN os.status = 'In Progress' THEN os.status_timestamp END) AS in_progress_time,
        -- Get the timestamp for 'Delivered' status
        MIN(CASE WHEN os.status = 'Delivered' THEN os.status_timestamp END) AS delivered_time
    FROM 
        fact_orders o
    JOIN 
        fact_order_status os ON o.order_id = os.order_id
    GROUP BY 
        o.order_id, o.store_id
)
SELECT 
    s.store_id,
    s.store_name,
    -- Average time from submitted to in progress (processing time)
    AVG(EXTRACT(EPOCH FROM (in_progress_time - submitted_time)) / 60) AS avg_processing_time_minutes,
    -- Average time from in progress to delivered (delivery time)
    AVG(EXTRACT(EPOCH FROM (delivered_time - in_progress_time)) / 60) AS avg_delivery_time_minutes,
    -- Average time from submitted to delivered (total time)
    AVG(EXTRACT(EPOCH FROM (delivered_time - submitted_time)) / 60) AS avg_total_time_minutes,
    -- Count of orders per store
    COUNT(order_id) AS total_orders
FROM 
    order_timing ot
JOIN 
    dim_stores s ON ot.store_id = s.store_id
-- Only include orders that have all three statuses
WHERE 
    submitted_time IS NOT NULL
    AND in_progress_time IS NOT NULL
    AND delivered_time IS NOT NULL
GROUP BY 
    s.store_id, s.store_name
ORDER BY 
    avg_total_time_minutes;