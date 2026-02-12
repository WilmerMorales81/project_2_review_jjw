"""
View data from SQL files
"""

#Jing's practice script to read and view data from the raw CSV file used in stg_census.sql, and export a sample to CSV for easier viewing. This is useful for quickly checking the contents of the raw data files before cleaning or uploading to S3.

import polars as pl
import os
import sys

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Change working directory to project root
os.chdir(r"c:\Users\jingl\DE 2\project_2_review_jjw")

print("=" * 60)
print("Viewing Census Data (stg_census.sql)")
print("=" * 60)

# Read the raw data used by stg_census.sql
# Use infer_schema_length=0 to read all columns as strings initially
df = pl.read_csv("data/raw/ssa_fips_state_county_2025.csv", infer_schema_length=10000, ignore_errors=True)

print(f"\nTotal rows: {len(df)}")
print(f"Total columns: {len(df.columns)}")

print("\nColumn names:")
print(df.columns)

print("\nFirst 10 rows:")
print(df.head(10))

print("\nData statistics:")
print(df.describe())

print("\nCount counties by state:")
state_counts = df.group_by("state").agg(
    pl.count("fipscounty").alias("county_count")
).sort("county_count", descending=True)
print(state_counts.head(10))

# Save to CSV for easy viewing
output_file = "data/Cleaned/census_preview.csv"
df.head(100).write_csv(output_file)
print(f"\n✓ First 100 rows saved to: {output_file}")
