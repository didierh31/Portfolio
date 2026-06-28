"""
BRONZE LAYER - RAW DATA INGESTION
==================================

PURPOSE:
    Bronze layer in Medallion architecture - ingests raw data from samples.tpch
    into workspace.bronze schema with minimal transformation and audit tracking.

CONTRACT REQUIREMENTS (from contracts.md):
    ✓ 8 streaming tables (customer, orders, lineitem, part, supplier, partsupp, nation, region)
    ✓ SELECT * only - preserve all source columns unchanged
    ✓ 2 audit columns added: _ingest_ts (timestamp), _source_table (string)
    ✓ Append-only streaming tables for incremental ingestion
    ✓ No transformations, filtering, or business logic
    ✓ Table naming: <source>_raw

SOURCE:
    samples.tpch - Standard TPC-H benchmark dataset
    (Customer orders, products, suppliers, geographic reference data)

OUTPUT:
    workspace.bronze.<source>_raw
    All tables published as streaming tables in workspace.bronze schema

BRONZE LAYER RULES:
    - Raw data preservation: No column renaming, type changes, or filtering
    - Audit tracking: Add metadata columns for lineage and troubleshooting
    - Streaming only: All tables use spark.readStream for incremental processing
    - No data quality checks: Bronze accepts data as-is (validation happens in Silver)

DOWNSTREAM:
    Silver layer reads from these bronze tables to apply transformations,
    business rules, data quality checks, and create curated datasets.
"""

import dlt
from pyspark.sql.functions import current_timestamp, lit


# =============================================================================
# CUSTOMER DATA - Master customer records
# =============================================================================

@dlt.table(
    comment="Bronze raw table for customer data from samples.tpch.customer"
)
def customer_raw():
    """
    Ingest customer master records from TPC-H dataset.
    
    BUSINESS CONTEXT:
        Customer master data containing demographics, account information, and
        market segment classification for all customers in the system.
    
    KEY COLUMNS:
        - c_custkey: Customer unique identifier (primary key)
        - c_name: Customer name
        - c_address: Customer address
        - c_nationkey: Foreign key to nation table
        - c_phone: Customer phone number
        - c_acctbal: Account balance
        - c_mktsegment: Market segment (AUTOMOBILE, BUILDING, FURNITURE, MACHINERY, HOUSEHOLD)
        - c_comment: Customer comments
    
    SIZE: ~750,000 rows
    
    SILVER PROCESSING:
        - Validate c_custkey uniqueness
        - Parse and standardize phone numbers
        - Join with nation_raw for geographic enrichment
        - Filter invalid account balances
    """
    return (
        spark.readStream.table("samples.tpch.customer")
        .select("*")  # Preserve all source columns unchanged
        .withColumn("_ingest_ts", current_timestamp())  # Audit: when row was ingested
        .withColumn("_source_table", lit("samples.tpch.customer"))  # Audit: source table name
    )


# =============================================================================
# ORDER DATA - Order headers
# =============================================================================

@dlt.table(
    comment="Bronze raw table for orders data from samples.tpch.orders"
)
def orders_raw():
    """
    Ingest order header records from TPC-H dataset.
    
    BUSINESS CONTEXT:
        Order header information containing customer references, dates, totals,
        and status. Each order can have multiple line items (see lineitem_raw).
    
    KEY COLUMNS:
        - o_orderkey: Order unique identifier (primary key)
        - o_custkey: Foreign key to customer table
        - o_orderstatus: Order status (F=Finished, O=Open, P=Pending)
        - o_totalprice: Total order price (sum of line items)
        - o_orderdate: Date order was placed
        - o_orderpriority: Order priority (1-URGENT to 5-LOW)
        - o_clerk: Clerk who processed the order
        - o_shippriority: Shipping priority
        - o_comment: Order comments
    
    SIZE: ~7,500,000 rows (second largest table)
    
    SILVER PROCESSING:
        - Join with customer_raw for customer details
        - Join with lineitem_raw for order line details
        - Calculate order metrics and aggregations
        - Validate order dates and status transitions
    """
    return (
        spark.readStream.table("samples.tpch.orders")
        .select("*")  # Preserve all source columns unchanged
        .withColumn("_ingest_ts", current_timestamp())  # Audit: when row was ingested
        .withColumn("_source_table", lit("samples.tpch.orders"))  # Audit: source table name
    )


# =============================================================================
# LINE ITEM DATA - Order line items (LARGEST TABLE)
# =============================================================================

@dlt.table(
    comment="Bronze raw table for lineitem data from samples.tpch.lineitem"
)
def lineitem_raw():
    """
    Ingest order line item records from TPC-H dataset.
    **LARGEST TABLE** - Contains detailed line items for every order.
    
    BUSINESS CONTEXT:
        Detailed line-item transactions containing product, supplier, pricing,
        discount, and shipping information. This is the fact table for revenue
        analysis and supply chain metrics.
    
    KEY COLUMNS:
        - l_orderkey: Foreign key to orders table (composite key)
        - l_partkey: Foreign key to part table
        - l_suppkey: Foreign key to supplier table
        - l_linenumber: Line number within order (composite key with l_orderkey)
        - l_quantity: Quantity ordered
        - l_extendedprice: Extended price (quantity * unit price)
        - l_discount: Discount percentage (0.00 to 0.10)
        - l_tax: Tax rate
        - l_returnflag: Return status (R=Returned, A=Accepted, N=None)
        - l_linestatus: Line status (O=Open, F=Finished)
        - l_shipdate: Ship date
        - l_commitdate: Commit date
        - l_receiptdate: Receipt date
        - l_shipinstruct: Shipping instructions
        - l_shipmode: Shipping mode (AIR, TRUCK, MAIL, SHIP, FOB, REG AIR, RAIL)
        - l_comment: Line item comments
    
    SIZE: ~30,000,000+ rows (LARGEST - may take several minutes to process)
    
    REVENUE CALCULATION:
        Net revenue = l_extendedprice * (1 - l_discount) * (1 + l_tax)
    
    SILVER PROCESSING:
        - Join with orders_raw, part_raw, supplier_raw for enrichment
        - Calculate revenue metrics and aggregations
        - Aggregate by product, supplier, customer dimensions
        - Validate date logic (shipdate <= receiptdate)
        - Apply data quality expectations (discount range, positive quantities)
    """
    return (
        spark.readStream.table("samples.tpch.lineitem")
        .select("*")  # Preserve all source columns unchanged
        .withColumn("_ingest_ts", current_timestamp())  # Audit: when row was ingested
        .withColumn("_source_table", lit("samples.tpch.lineitem"))  # Audit: source table name
    )


# =============================================================================
# PART DATA - Product catalog
# =============================================================================

@dlt.table(
    comment="Bronze raw table for part data from samples.tpch.part"
)
def part_raw():
    """
    Ingest part/product catalog records from TPC-H dataset.
    
    BUSINESS CONTEXT:
        Product master data containing part specifications, branding, and pricing.
        Used for product analytics and inventory management.
    
    KEY COLUMNS:
        - p_partkey: Part unique identifier (primary key)
        - p_name: Part name
        - p_mfgr: Manufacturer (Manufacturer#1 through #5)
        - p_brand: Brand (Brand#11 through #55)
        - p_type: Part type (e.g., ECONOMY ANODIZED STEEL, PROMO BURNISHED COPPER)
        - p_size: Part size (1-50)
        - p_container: Container type (SM CASE, LG BOX, MED BAG, JUMBO PACK, etc.)
        - p_retailprice: Retail price
        - p_comment: Part comments
    
    SIZE: ~1,000,000 rows
    
    SILVER PROCESSING:
        - Standardize part names and types
        - Calculate price tiers and categories
        - Join with partsupp_raw for supplier availability
        - Enrich with brand and manufacturer hierarchies
    """
    return (
        spark.readStream.table("samples.tpch.part")
        .select("*")  # Preserve all source columns unchanged
        .withColumn("_ingest_ts", current_timestamp())  # Audit: when row was ingested
        .withColumn("_source_table", lit("samples.tpch.part"))  # Audit: source table name
    )


# =============================================================================
# SUPPLIER DATA - Supplier master records
# =============================================================================

@dlt.table(
    comment="Bronze raw table for supplier data from samples.tpch.supplier"
)
def supplier_raw():
    """
    Ingest supplier master records from TPC-H dataset.
    
    BUSINESS CONTEXT:
        Supplier master data containing contact information, geographic location,
        and account details. Used for supply chain analytics and vendor management.
    
    KEY COLUMNS:
        - s_suppkey: Supplier unique identifier (primary key)
        - s_name: Supplier name
        - s_address: Supplier address
        - s_nationkey: Foreign key to nation table
        - s_phone: Supplier phone number
        - s_acctbal: Account balance
        - s_comment: Supplier comments
    
    SIZE: ~50,000 rows
    
    SILVER PROCESSING:
        - Validate s_suppkey uniqueness
        - Join with nation_raw for geographic analysis
        - Parse and standardize contact information
        - Calculate supplier performance metrics
    """
    return (
        spark.readStream.table("samples.tpch.supplier")
        .select("*")  # Preserve all source columns unchanged
        .withColumn("_ingest_ts", current_timestamp())  # Audit: when row was ingested
        .withColumn("_source_table", lit("samples.tpch.supplier"))  # Audit: source table name
    )


# =============================================================================
# PART-SUPPLIER DATA - Product availability by supplier
# =============================================================================

@dlt.table(
    comment="Bronze raw table for partsupp data from samples.tpch.partsupp"
)
def partsupp_raw():
    """
    Ingest part-supplier relationship records from TPC-H dataset.
    Junction table linking parts to their suppliers with availability and cost data.
    
    BUSINESS CONTEXT:
        Bridge table connecting parts to suppliers, containing supply cost and
        availability information. Used for supplier selection and cost optimization.
    
    KEY COLUMNS:
        - ps_partkey: Foreign key to part table (composite key)
        - ps_suppkey: Foreign key to supplier table (composite key)
        - ps_availqty: Available quantity from this supplier
        - ps_supplycost: Cost to purchase from this supplier
        - ps_comment: Part-supplier comments
    
    SIZE: ~4,000,000 rows
    
    SILVER PROCESSING:
        - Join with part_raw and supplier_raw for enrichment
        - Calculate optimal supplier per part (lowest cost, highest availability)
        - Track supply chain metrics and trends
        - Identify multi-source parts vs single-source parts
    """
    return (
        spark.readStream.table("samples.tpch.partsupp")
        .select("*")  # Preserve all source columns unchanged
        .withColumn("_ingest_ts", current_timestamp())  # Audit: when row was ingested
        .withColumn("_source_table", lit("samples.tpch.partsupp"))  # Audit: source table name
    )


# =============================================================================
# NATION DATA - Country reference table
# =============================================================================

@dlt.table(
    comment="Bronze raw table for nation data from samples.tpch.nation"
)
def nation_raw():
    """
    Ingest nation/country reference data from TPC-H dataset.
    Small reference table for geographic hierarchy.
    
    BUSINESS CONTEXT:
        Country-level reference data used for geographic segmentation and
        international analytics. Links to region table for continental grouping.
    
    KEY COLUMNS:
        - n_nationkey: Nation unique identifier (primary key)
        - n_name: Nation name (e.g., UNITED STATES, CHINA, BRAZIL, FRANCE)
        - n_regionkey: Foreign key to region table
        - n_comment: Nation comments
    
    SIZE: 25 rows (reference data - static list of countries)
    
    SILVER PROCESSING:
        - Join with region_raw for continent-level analysis
        - Enrich customer and supplier records with nation names
        - Use for geographic segmentation and reporting
        - Static reference data - rarely changes
    """
    return (
        spark.readStream.table("samples.tpch.nation")
        .select("*")  # Preserve all source columns unchanged
        .withColumn("_ingest_ts", current_timestamp())  # Audit: when row was ingested
        .withColumn("_source_table", lit("samples.tpch.nation"))  # Audit: source table name
    )


# =============================================================================
# REGION DATA - Geographic region reference table (SMALLEST TABLE)
# =============================================================================

@dlt.table(
    comment="Bronze raw table for region data from samples.tpch.region"
)
def region_raw():
    """
    Ingest region reference data from TPC-H dataset.
    **SMALLEST TABLE** - Top-level geographic hierarchy.
    
    BUSINESS CONTEXT:
        Continental/regional groupings for high-level geographic analysis.
        Top of the geographic hierarchy (region -> nation -> supplier/customer).
    
    KEY COLUMNS:
        - r_regionkey: Region unique identifier (primary key)
        - r_name: Region name (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)
        - r_comment: Region comments
    
    SIZE: 5 rows (reference data - one per continent/major region)
    
    SILVER PROCESSING:
        - Join with nation_raw for complete geographic hierarchy
        - Use for continent-level aggregations and analysis
        - Enable regional sales and performance reporting
        - Static reference data - rarely changes
    """
    return (
        spark.readStream.table("samples.tpch.region")
        .select("*")  # Preserve all source columns unchanged
        .withColumn("_ingest_ts", current_timestamp())  # Audit: when row was ingested
        .withColumn("_source_table", lit("samples.tpch.region"))  # Audit: source table name
    )
