An end-to-end ETL (Extract, Transform, Load) pipeline analyzing Walmart grocery store sales,
with a focus on supply and demand patterns around major U.S. public holidays
(Super Bowl, Labour Day, Thanksgiving, and Christmas).

## Project Overview

By the end of 2022, Walmart's e-commerce sales reached $80 billion — 13% of total revenue.
This project builds a data pipeline that integrates two data sources, cleans and transforms
the data, and produces an aggregated monthly sales analysis to support business decisions
around holiday periods.

## Data Sources

| Source | Type | Description |
|---|---|---|
| `grocery_sales` | PostgreSQL table | Weekly sales by store and department |
| `extra_data.parquet` | Parquet file | Complementary store and economic metrics |

**grocery_sales** columns: `Store_ID`, `Date`, `Dept`, `Weekly_Sales`

**extra_data.parquet** columns: `IsHoliday`, `Temperature`, `Fuel_Price`, `CPI`,
`Unemployment`, `MarkDown1–4`, `Dept`, `Size`, `Type`

## Pipeline Architecture

[PostgreSQL] ──┐
├──► extract() ──► transform() ──► load() ──► clean_data.csv
[Parquet file]─┘                                         └──► agg_data.csv



### Steps

1. **Extract** — Merges the SQL grocery sales table with the Parquet file on a shared index
2. **Transform** — Cleans and filters the merged dataset:
   - Fills missing values (`CPI`, `Weekly_Sales`, `Unemployment`) with column means
   - Parses dates and extracts a `Month` column
   - Filters records where `Weekly_Sales > $10,000`
   - Drops unused columns (temperature, fuel price, markdowns, store type/size)
3. **Load** — Exports `clean_data.csv` and `agg_data.csv`
4. **Validate** — Confirms output files exist, raising an exception if not

## Output

**clean_data.csv** — Cleaned dataset with columns:
`Store_ID`, `Month`, `Dept`, `IsHoliday`, `Weekly_Sales`, `CPI`, `Unemployment`

**agg_data.csv** — Average weekly sales per month:

| Month | Avg_Sales |
|-------|-----------|
| 1 | $33,174 |
| 11 | $36,594 |
| 12 | $39,238 |
| ... | ... |

November and December show the highest average sales, confirming holiday-driven demand peaks.

## Tech Stack

- **Python** — Pandas
- **SQL** — PostgreSQL
- **File formats** — Parquet, CSV



