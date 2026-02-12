
import polars as pl

print("=" * 60)
print("Exploring NPPES Raw Data")
print("=" * 60)

# Read first 1000 rows to explore
df = pl.read_csv("data/raw/nppes_sample.csv", n_rows=1000)

# 1. How many columns?
print(f"\nTotal columns: {len(df.columns)}")

# 2. What are the column names?
print("\nColumn names:")
for i, col in enumerate(df.columns, 1):
    print(f"{i}. {col}")

# 3. What does the data look like?
print("\nFirst 5 rows:")
print(df.head())

# 4. Check for missing values
print("\nNull counts (first 20 columns):")
print(df.select(df.columns[:20]).null_count())

# 5. Data types
print("\nData types (first 20 columns):")
for col in df.columns[:20]:
    print(f"{col}: {df[col].dtype}")
