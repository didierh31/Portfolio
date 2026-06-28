# =============================================================================
# SILVER LAYER - Data Cleaning & Quality Enforcement
# =============================================================================
# Purpose:
#   Transform raw Bronze data into clean, validated Silver tables suitable for
#   analytics and downstream Gold layer aggregations.
#
# Architecture:
#   - Source: workspace.bronze schema (8 *_raw tables from TPC-H dataset)
#   - Target: workspace.silver schema (8 *_clean tables)
#   - Pipeline: Serverless Spark Declarative Pipeline (formerly DLT)
#
# Data Transformations:
#   1. String trimming: Remove leading/trailing whitespace from all text columns
#   2. Auto CDC: Apply change data capture for customer & orders (SCD Type 1)
#   3. Data quality: Enforce business rules via @dlt.expect_all_or_drop
#
# =============================================================================

import dlt
from pyspark.sql.functions import trim, col


# =============================================================================
# AUTO CDC TABLES - Change Data Capture (SCD Type 1)
# =============================================================================
# Pattern: Auto CDC with dlt.apply_changes() for handling updates/inserts
#
# Why Auto CDC for customer & orders:
#   - These are dimension/fact tables that may receive updates over time
#   - SCD Type 1: Keep only the latest version (updates replace old records)
#   - Keys: Primary key columns identify unique records (c_custkey, o_orderkey)
#   - Sequencing: _ingest_ts determines which record is "latest"
#
# Implementation requires 3 components:
#   1. dlt.create_streaming_table(): Creates the target table first
#   2. @dlt.view: Defines the transformation logic (read + clean)
#   3. dlt.apply_changes(): Merges changes from view into target table
#
# =============================================================================

# -----------------------------------------------------------------------------
# CUSTOMER_CLEAN - Customer Dimension with Auto CDC
# -----------------------------------------------------------------------------
# Source: workspace.bronze.customer_raw (~750K rows)
# Target: workspace.silver.customer_clean
# Key: c_custkey (primary key identifying unique customers)
# Sequence: _ingest_ts (timestamp determining record freshness)
#
# Transformations:
#   - Trim 5 string columns: c_name, c_address, c_phone, c_mktsegment, c_comment
#   - Auto CDC: Automatically handle inserts/updates based on c_custkey
#   - SCD Type 1: Keep only latest record per customer (no history)
# -----------------------------------------------------------------------------

# Step 1: Create the target streaming table (required before apply_changes)
dlt.create_streaming_table("customer_clean")

# Step 2: Define view with transformation logic
@dlt.view
def customer_clean_view():
    """
    Streaming view that reads from Bronze customer_raw and trims string columns.
    
    Note: Using fully qualified table name 'workspace.bronze.customer_raw' to
    explicitly read from Bronze schema (pipeline target is workspace.silver).
    """
    return (
        dlt.read_stream("workspace.bronze.customer_raw")
        .withColumn("c_name", trim(col("c_name")))           # Customer name
        .withColumn("c_address", trim(col("c_address")))     # Mailing address
        .withColumn("c_phone", trim(col("c_phone")))         # Phone number
        .withColumn("c_mktsegment", trim(col("c_mktsegment")))  # Market segment
        .withColumn("c_comment", trim(col("c_comment")))     # Free-form comment
    )


# Step 3: Apply CDC logic to merge changes into target table
dlt.apply_changes(
    target="customer_clean",           # Target table name
    source="customer_clean_view",      # Source view with transformed data
    keys=["c_custkey"],                # Primary key for matching records
    sequence_by="_ingest_ts",          # Timestamp column for ordering changes
    stored_as_scd_type=1               # Type 1: Replace old records (no history)
)


# -----------------------------------------------------------------------------
# ORDERS_CLEAN - Orders Fact Table with Auto CDC
# -----------------------------------------------------------------------------
# Source: workspace.bronze.orders_raw (~7.5M rows)
# Target: workspace.silver.orders_clean
# Key: o_orderkey (primary key identifying unique orders)
# Sequence: _ingest_ts (timestamp determining record freshness)
#
# Transformations:
#   - Trim 4 string columns: o_orderstatus, o_orderpriority, o_clerk, o_comment
#   - Auto CDC: Automatically handle inserts/updates based on o_orderkey
#   - SCD Type 1: Keep only latest record per order (no history)
# -----------------------------------------------------------------------------

# Step 1: Create the target streaming table
dlt.create_streaming_table("orders_clean")

# Step 2: Define view with transformation logic
@dlt.view
def orders_clean_view():
    """
    Streaming view that reads from Bronze orders_raw and trims string columns.
    """
    return (
        dlt.read_stream("workspace.bronze.orders_raw")
        .withColumn("o_orderstatus", trim(col("o_orderstatus")))      # Order status (e.g., 'F', 'O', 'P')
        .withColumn("o_orderpriority", trim(col("o_orderpriority")))  # Priority level
        .withColumn("o_clerk", trim(col("o_clerk")))                  # Clerk identifier
        .withColumn("o_comment", trim(col("o_comment")))              # Free-form comment
    )


# Step 3: Apply CDC logic to merge changes into target table
dlt.apply_changes(
    target="orders_clean",
    source="orders_clean_view",
    keys=["o_orderkey"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=1
)


# =============================================================================
# STREAMING TABLES WITH DATA QUALITY EXPECTATIONS
# =============================================================================
# Pattern: Standard streaming tables with @dlt.expect_all_or_drop decorators
#
# Why streaming tables (not Auto CDC) for these 6 tables:
#   - These are append-only tables without updates (immutable facts/dimensions)
#   - No need for CDC merge logic - simpler streaming table pattern sufficient
#
# Data Quality Enforcement:
#   - @dlt.expect_all_or_drop: Drops rows that fail ANY expectation
#   - Each expectation has a name (e.g., "valid_quantity") and a SQL expression
#   - Failed rows are logged to event log for monitoring
#   - This ensures only clean, valid data reaches Silver layer
#
# Contract Requirements:
#   - All NOT NULL constraints on primary/foreign keys
#   - Range validations (quantity > 0, discount 0-1, amounts >= 0)
#   - Referential integrity checks where applicable
# =============================================================================

# -----------------------------------------------------------------------------
# LINEITEM_CLEAN - Line Items Fact Table
# -----------------------------------------------------------------------------
# Source: workspace.bronze.lineitem_raw (~30M rows)
# Target: workspace.silver.lineitem_clean
#
# Data Quality Expectations (all must pass or row is dropped):
#   1. valid_lineitem_keys: l_orderkey and l_linenumber must not be NULL
#   2. valid_quantity: l_quantity must be > 0
#   3. valid_discount: l_discount must be between 0 and 1 (0% to 100%)
#   4. valid_amounts: l_extendedprice and l_tax must be >= 0
#
# Transformations:
#   - Trim 5 string columns: l_returnflag, l_linestatus, l_shipinstruct, 
#     l_shipmode, l_comment
# -----------------------------------------------------------------------------
@dlt.table(
    comment="Clean lineitem data with data quality checks"
)
@dlt.expect_all_or_drop({
    "valid_lineitem_keys": "l_orderkey IS NOT NULL AND l_linenumber IS NOT NULL",
    "valid_quantity": "l_quantity > 0",
    "valid_discount": "l_discount BETWEEN 0 AND 1",
    "valid_amounts": "l_extendedprice >= 0 AND l_tax >= 0"
})
def lineitem_clean():
    """
    Streaming table for cleaned line item records with quality checks.
    """
    return (
        dlt.read_stream("workspace.bronze.lineitem_raw")
        .withColumn("l_returnflag", trim(col("l_returnflag")))      # Return flag (e.g., 'R', 'A', 'N')
        .withColumn("l_linestatus", trim(col("l_linestatus")))      # Line status (e.g., 'O', 'F')
        .withColumn("l_shipinstruct", trim(col("l_shipinstruct")))  # Shipping instructions
        .withColumn("l_shipmode", trim(col("l_shipmode")))          # Shipping mode
        .withColumn("l_comment", trim(col("l_comment")))            # Free-form comment
    )


# -----------------------------------------------------------------------------
# PART_CLEAN - Parts Dimension Table
# -----------------------------------------------------------------------------
# Source: workspace.bronze.part_raw (~1M rows)
# Target: workspace.silver.part_clean
#
# Data Quality Expectations:
#   1. valid_part_key: p_partkey must not be NULL
#   2. valid_size: p_size must be >= 0
#   3. valid_price: p_retailprice must be >= 0
#
# Transformations:
#   - Trim 6 string columns: p_name, p_mfgr, p_brand, p_type, p_container, p_comment
# -----------------------------------------------------------------------------
@dlt.table(
    comment="Clean part data with data quality checks"
)
@dlt.expect_all_or_drop({
    "valid_part_key": "p_partkey IS NOT NULL",
    "valid_size": "p_size >= 0",
    "valid_price": "p_retailprice >= 0"
})
def part_clean():
    """
    Streaming table for cleaned part records with quality checks.
    """
    return (
        dlt.read_stream("workspace.bronze.part_raw")
        .withColumn("p_name", trim(col("p_name")))          # Part name
        .withColumn("p_mfgr", trim(col("p_mfgr")))          # Manufacturer
        .withColumn("p_brand", trim(col("p_brand")))        # Brand
        .withColumn("p_type", trim(col("p_type")))          # Part type
        .withColumn("p_container", trim(col("p_container")))  # Container type
        .withColumn("p_comment", trim(col("p_comment")))    # Free-form comment
    )


# -----------------------------------------------------------------------------
# SUPPLIER_CLEAN - Suppliers Dimension Table
# -----------------------------------------------------------------------------
# Source: workspace.bronze.supplier_raw (~50K rows)
# Target: workspace.silver.supplier_clean
#
# Data Quality Expectations:
#   1. valid_supplier_key: s_suppkey must not be NULL
#   2. valid_balance: s_acctbal must not be NULL
#
# Transformations:
#   - Trim 4 string columns: s_name, s_address, s_phone, s_comment
# -----------------------------------------------------------------------------
@dlt.table(
    comment="Clean supplier data with data quality checks"
)
@dlt.expect_all_or_drop({
    "valid_supplier_key": "s_suppkey IS NOT NULL",
    "valid_balance": "s_acctbal IS NOT NULL"
})
def supplier_clean():
    """
    Streaming table for cleaned supplier records with quality checks.
    """
    return (
        dlt.read_stream("workspace.bronze.supplier_raw")
        .withColumn("s_name", trim(col("s_name")))          # Supplier name
        .withColumn("s_address", trim(col("s_address")))    # Mailing address
        .withColumn("s_phone", trim(col("s_phone")))        # Phone number
        .withColumn("s_comment", trim(col("s_comment")))    # Free-form comment
    )


# -----------------------------------------------------------------------------
# PARTSUPP_CLEAN - Part-Supplier Relationship Table
# -----------------------------------------------------------------------------
# Source: workspace.bronze.partsupp_raw (~4M rows)
# Target: workspace.silver.partsupp_clean
#
# Data Quality Expectations:
#   1. valid_partsupp_keys: ps_partkey and ps_suppkey must not be NULL
#   2. valid_quantity: ps_availqty must be >= 0
#   3. valid_supplycost: ps_supplycost must be >= 0
#
# Transformations:
#   - Trim 1 string column: ps_comment
# -----------------------------------------------------------------------------
@dlt.table(
    comment="Clean partsupp data with data quality checks"
)
@dlt.expect_all_or_drop({
    "valid_partsupp_keys": "ps_partkey IS NOT NULL AND ps_suppkey IS NOT NULL",
    "valid_quantity": "ps_availqty >= 0",
    "valid_supplycost": "ps_supplycost >= 0"
})
def partsupp_clean():
    """
    Streaming table for cleaned part-supplier relationships with quality checks.
    """
    return (
        dlt.read_stream("workspace.bronze.partsupp_raw")
        .withColumn("ps_comment", trim(col("ps_comment")))  # Free-form comment
    )


# -----------------------------------------------------------------------------
# NATION_CLEAN - Nations Dimension Table
# -----------------------------------------------------------------------------
# Source: workspace.bronze.nation_raw (25 rows)
# Target: workspace.silver.nation_clean
#
# Data Quality Expectations:
#   1. valid_nation_key: n_nationkey must not be NULL
#   2. valid_region_ref: n_regionkey must not be NULL (foreign key to region)
#
# Transformations:
#   - Trim 2 string columns: n_name, n_comment
# -----------------------------------------------------------------------------
@dlt.table(
    comment="Clean nation data with data quality checks"
)
@dlt.expect_all_or_drop({
    "valid_nation_key": "n_nationkey IS NOT NULL",
    "valid_region_ref": "n_regionkey IS NOT NULL"
})
def nation_clean():
    """
    Streaming table for cleaned nation records with quality checks.
    """
    return (
        dlt.read_stream("workspace.bronze.nation_raw")
        .withColumn("n_name", trim(col("n_name")))          # Nation name
        .withColumn("n_comment", trim(col("n_comment")))    # Free-form comment
    )


# -----------------------------------------------------------------------------
# REGION_CLEAN - Regions Dimension Table
# -----------------------------------------------------------------------------
# Source: workspace.bronze.region_raw (5 rows)
# Target: workspace.silver.region_clean
#
# Data Quality Expectations:
#   1. valid_region_key: r_regionkey must not be NULL
#
# Transformations:
#   - Trim 2 string columns: r_name, r_comment
# -----------------------------------------------------------------------------
@dlt.table(
    comment="Clean region data with data quality checks"
)
@dlt.expect_all_or_drop({
    "valid_region_key": "r_regionkey IS NOT NULL"
})
def region_clean():
    """
    Streaming table for cleaned region records with quality checks.
    """
    return (
        dlt.read_stream("workspace.bronze.region_raw")
        .withColumn("r_name", trim(col("r_name")))          # Region name
        .withColumn("r_comment", trim(col("r_comment")))    # Free-form comment
    )
