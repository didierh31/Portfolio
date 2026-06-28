import dlt

# ============================================================================
# Gold Layer: Business-ready analytics tables
# ============================================================================
# Purpose: Aggregated, business-focused tables ready for BI and reporting
# Source: workspace.silver layer only (never bronze or raw sources)
# Revenue Definition: sum(l_extendedprice * (1 - l_discount)) - applied consistently across all tables
# Table Type: Materialized views (@dlt.table) - refreshed when pipeline runs
# ============================================================================

# ----------------------------------------------------------------------------
# Table 1: Monthly Revenue by Region
# ----------------------------------------------------------------------------
# Business Question: How much revenue did each region generate each month?
# Grain: One row per region per month
# Use Case: Regional performance tracking, trend analysis, executive dashboards
# ----------------------------------------------------------------------------
@dlt.table(
    name="gold_monthly_revenue_by_region",
    comment="Monthly revenue by region. Grain: one row per region per month. Revenue = sum(l_extendedprice * (1 - l_discount))."
)
def gold_monthly_revenue_by_region():
    return spark.sql("""
        SELECT 
            r.r_name AS region,                                      -- Geographic region name
            DATE_TRUNC('MONTH', o.o_orderdate) AS month,              -- First day of the month
            SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue   -- Total revenue (consistent formula)
        FROM workspace.silver.lineitem_clean l                        -- Start with line items (detailed transactions)
        INNER JOIN workspace.silver.orders_clean o ON l.l_orderkey = o.o_orderkey      -- Join to orders for date
        INNER JOIN workspace.silver.customer_clean c ON o.o_custkey = c.c_custkey      -- Join to customers
        INNER JOIN workspace.silver.nation_clean n ON c.c_nationkey = n.n_nationkey    -- Join to nation
        INNER JOIN workspace.silver.region_clean r ON n.n_regionkey = r.r_regionkey    -- Join to region
        GROUP BY r.r_name, DATE_TRUNC('MONTH', o.o_orderdate)        -- Aggregate to region-month grain
    """)


# ----------------------------------------------------------------------------
# Table 2: Customer Lifetime Value
# ----------------------------------------------------------------------------
# Business Question: What is the total value and behavior of each customer?
# Grain: One row per customer
# Use Case: Customer segmentation, retention analysis, targeted marketing
# Metrics: Total orders, revenue, avg order value, order dates, value segment
# ----------------------------------------------------------------------------
@dlt.table(
    name="gold_customer_lifetime_value",
    comment="Customer lifetime value metrics. Grain: one row per customer. Includes total orders, total revenue, average order value, first/last order dates, and value segment."
)
def gold_customer_lifetime_value():
    return spark.sql("""
        SELECT 
            c.c_custkey AS customer_key,                                                      -- Customer identifier
            c.c_name AS customer_name,                                                        -- Customer name
            COUNT(DISTINCT o.o_orderkey) AS total_orders,                                     -- How many orders placed
            SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,                     -- Total lifetime revenue
            SUM(l.l_extendedprice * (1 - l.l_discount)) / COUNT(DISTINCT o.o_orderkey) AS avg_order_value,  -- Average per order
            MIN(o.o_orderdate) AS first_order_date,                                           -- Customer acquisition date
            MAX(o.o_orderdate) AS last_order_date,                                            -- Most recent order
            CASE                                                                               -- Segment by total revenue
                WHEN SUM(l.l_extendedprice * (1 - l.l_discount)) >= 500000 THEN 'High Value'     -- Top tier: $500K+
                WHEN SUM(l.l_extendedprice * (1 - l.l_discount)) >= 100000 THEN 'Medium Value'   -- Mid tier: $100K-$500K
                ELSE 'Low Value'                                                                  -- Low tier: <$100K
            END AS value_segment
        FROM workspace.silver.customer_clean c
        INNER JOIN workspace.silver.orders_clean o ON c.c_custkey = o.o_custkey              -- Join to orders
        INNER JOIN workspace.silver.lineitem_clean l ON o.o_orderkey = l.l_orderkey          -- Join to line items for revenue
        GROUP BY c.c_custkey, c.c_name                                                       -- Aggregate to customer grain
    """)


# ----------------------------------------------------------------------------
# Table 3: Top Products Performance
# ----------------------------------------------------------------------------
# Business Question: Which products generate the most revenue and volume?
# Grain: One row per product
# Use Case: Inventory planning, product promotions, portfolio management
# Metrics: Revenue, quantity sold, order frequency, revenue ranking
# ----------------------------------------------------------------------------
@dlt.table(
    name="gold_top_products",
    comment="Product performance metrics. Grain: one row per product. Includes total revenue, total quantity, order count, and revenue rank."
)
def gold_top_products():
    return spark.sql("""
        SELECT 
            p.p_partkey AS product_key,                                                  -- Product identifier
            p.p_name AS product_name,                                                    -- Product name
            p.p_type AS product_type,                                                    -- Product category/type
            SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue,                -- Total revenue from this product
            SUM(l.l_quantity) AS total_quantity,                                         -- Total units sold
            COUNT(DISTINCT l.l_orderkey) AS order_count,                                 -- How many orders included this product
            RANK() OVER (ORDER BY SUM(l.l_extendedprice * (1 - l.l_discount)) DESC) AS revenue_rank  -- Rank by revenue (1=highest)
        FROM workspace.silver.part_clean p                                              -- Start with product master
        INNER JOIN workspace.silver.lineitem_clean l ON p.p_partkey = l.l_partkey      -- Join to line items for sales data
        GROUP BY p.p_partkey, p.p_name, p.p_type                                       -- Aggregate to product grain
    """)
