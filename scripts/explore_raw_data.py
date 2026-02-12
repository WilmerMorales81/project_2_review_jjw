
import polars as pl

print("=" * 60)
print("Exploring NPPES Raw Data")
print("=" * 60)

# Read first 1000 rows to explore
df = pl.read_csv("data/raw/npidata_pfile.csv", n_rows=1000)

# 1. How many columns?
print(f"\nTotal columns: {len(df.columns)}")

# 2. What are the column names? enumerate to print with numbers for easier reference, and only print the first 20 columns to avoid overwhelming output. If you want to see all columns, you can remove the slicing [:20].
print("\nColumn names:")
for i, col in enumerate(df.columns, 1):
    print(f"{i}. {col}")

# 3. What does the data look like? Print the first 5 rows to get a sense of the data structure and content. This will help you understand what kind of information is available and how it is formatted.
print("\nFirst 5 rows:")
print(df.head())

# 4. Check for missing values, null counts for each column, and print only the first 20 columns to avoid overwhelming output. If you want to see null counts for all columns, you can remove the slicing [:20].
print("\nNull counts (first 20 columns):")
print(df.select(df.columns[:20]).null_count())

# 5. Data types, print data types for the first 20 columns to understand the structure of the data. This will help you identify which columns are numeric, categorical, or text, and plan your cleaning and analysis steps accordingly. If you want to see data types for all columns, you can remove the slicing [:20].
print("\nData types (first 20 columns):")
for col in df.columns[:20]:
    print(f"{col}: {df[col].dtype}")