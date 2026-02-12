# scripts/create_zip_county.py
import polars as pl
import pandas as pd

print("=" * 60)
print("CREATING ZIP-COUNTY MAPPING")
print("=" * 60)

print("\nReading local ZIP-County file...")
# Use local file
local_file = "data/raw/ZIP_COUNTY_032025.xlsx"

try:
    # Read with pandas first, then convert to polars， polar doesn't support xlsx yet, but pandas does. Then, counting rows and converting to polars for processing.
    df_pd = pd.read_excel(local_file)
    df = pl.from_pandas(df_pd)
    print(f"Original: {len(df):,} rows")

    # Select and rename columns
    zip_county = df.select([
        pl.col("ZIP").cast(pl.Utf8).str.zfill(5).alias("zip_code"), # Ensure ZIP codes are 5-digit strings，zero fill if needed,alias is to rename the column to zip_code
        pl.col("COUNTY").cast(pl.Utf8).str.zfill(5).alias("county_fips"), # Ensure county FIPS codes are 5-digit strings
        pl.col("RES_RATIO").alias("res_ratio"), # Residential ratio to determine primary county for ZIP
    ])

    # Keep primary county for each ZIP (highest residential ratio) descending order, group by ZIP code, and take the first entry (the one with the highest residential ratio)
    zip_county = (
        zip_county
        .sort("res_ratio", descending=True)
        .group_by("zip_code")
        .first()
        .select(["zip_code", "county_fips"])
    )

    print(f"\nFinal: {len(zip_county):,} ZIP codes mapped to counties")

    # Save
    zip_county.write_parquet("data/cleaned/zip_county.parquet")

    print(f"\nDone! Saved to data/cleaned/zip_county.parquet")
    print("=" * 60)

except Exception as e:
    print(f"\nError: {e}")
    print("Make sure the file exists: data/raw/ZIP_COUNTY_032025.xlsx")