"""
View cleaned Parquet data files
"""
import polars as pl
import os
import sys
from pathlib import Path

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Use dynamic path relative to script location
project_root = Path(__file__).parent.parent
os.chdir(project_root)

print("=" * 60)
print("Available Parquet Files in data/Cleaned/")
print("=" * 60)

parquet_files = [
    "data/Cleaned/nppes_cleaned.parquet",
    "data/Cleaned/zip_county.parquet"
]

for file in parquet_files:
    if os.path.exists(file):
        print(f"\n{'='*60}")
        print(f"File: {file}")
        print(f"{'='*60}")

        try:
            df = pl.read_parquet(file)

            # Check if file is empty
            if len(df) == 0:
                print(f"⚠️  Warning: File exists but contains 0 rows")
                print(f"   File size: {os.path.getsize(file):,} bytes")
                continue

            print(f"Rows: {len(df):,}")
            print(f"Columns: {len(df.columns)}")
            print(f"\nColumn names:\n{df.columns}")
            print(f"\nFirst 5 rows:")
            print(df.head(5))

            # Export sample to CSV
            output = file.replace(".parquet", "_sample.csv")
            df.head(100).write_csv(output)
            print(f"\n✓ Sample exported to: {output}")

        except Exception as e:
            print(f"✗ Error reading file: {e}")
            print(f"   File size: {os.path.getsize(file):,} bytes")
    else:
        print(f"\n✗ File not found: {file}")

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
