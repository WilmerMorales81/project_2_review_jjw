# scripts/convert_zip_to_csv.py
# One-time conversion: Excel to CSV for dbt compatibility

import pandas as pd

print("Converting ZIP_COUNTY xlsx to CSV...")

# Read Excel file
df = pd.read_excel("data/raw/ZIP_COUNTY_032025.xlsx")

# Save as CSV
df.to_csv("data/raw/ZIP_COUNTY_032025.csv", index=False)

print(f"Done! Saved {len(df):,} rows to data/raw/ZIP_COUNTY_032025.csv")
