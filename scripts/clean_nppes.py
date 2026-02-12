
import polars as pl

print("=" * 60)
print("CLEANING NPPES DATA")
print("=" * 60)

# Read raw data
print("\nStep 1: Reading raw data...")
df = pl.read_csv("data/raw/nppes_sample.csv")
print(f"Original: {len(df):,} rows, {len(df.columns)} columns")

# Select columns needed for your 4 questions
print("\nStep 2: Selecting needed columns...")
columns_to_keep = [
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Healthcare Provider Taxonomy Code_1",
]

df_clean = df.select(columns_to_keep)
print(f"After selection: {len(df_clean.columns)} columns")

# Rename to shorter names
print("\nStep 3: Renaming columns...")
df_clean = df_clean.rename({
    "Provider Organization Name (Legal Business Name)": "organization_name",
    "Provider Last Name (Legal Name)": "last_name",
    "Provider First Name": "first_name",
    "Provider Business Practice Location Address State Name": "state",
    "Provider Business Practice Location Address Postal Code": "zip_code",
    "Healthcare Provider Taxonomy Code_1": "taxonomy_code",
    "Entity Type Code": "entity_type",
})

# Clean ZIP codes (keep only first 5 digits)
print("\nStep 4: Cleaning ZIP codes...")
df_clean = df_clean.with_columns(
    pl.col("zip_code").cast(pl.Utf8).str.slice(0, 5).alias("zip_code")
)

# Check for nulls
print("\nStep 5: Checking data quality...")
print("Null counts:")
print(df_clean.null_count())

# Remove rows with null NPI or null state
print("\nStep 6: Removing incomplete records...")
df_clean = df_clean.filter(
    (pl.col("NPI").is_not_null()) &
    (pl.col("state").is_not_null()) &
    (pl.col("zip_code").is_not_null())
)
print(f"After filtering: {len(df_clean):,} rows")

# Save
print("\nStep 7: Saving cleaned data...")
df_clean.write_parquet("data/cleaned/nppes_cleaned.parquet")

print(f"\n✅ Done! Saved {len(df_clean):,} providers")
print("=" * 60)
