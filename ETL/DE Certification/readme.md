Data Engineer Certification — Supplement Experiments ETL
Practical exam project for a Data Engineering certification.

Context: A fictional health-tech company, 1001-Experiments, collects data from wearables and health apps to run personalized supplement experiments for its users. The engineering task is to unify four disparate data sources into a single analysis-ready dataset.

What the notebook does:

Implements a merge_all_data() function that applies a Bronze → Silver → Gold medallion architecture:

Bronze — loads four raw CSV files: user health metrics, supplement usage, experiment metadata, and user profiles
Silver — cleans and standardizes data (parses dates, strips unit suffixes from sleep hours, converts dosage units from mg to grams, normalizes boolean fields)
Gold — joins all four sources into one unified DataFrame with 12 well-defined columns, covering each user's daily health metrics, supplement intake, demographic info, and age group classification
Key engineering decisions:

Outer join between health and supplement data to preserve all daily entries
Days without supplement intake encoded as 'No intake' instead of null
Age bucketed into labeled groups ('Under 18' → 'Over 65', 'Unknown' for missing)
Final output drops rows missing any required field (user_id, date, email)
Stack: Python, pandas, NumPy


