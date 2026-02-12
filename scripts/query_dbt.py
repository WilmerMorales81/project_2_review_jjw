"""
Simple script to query DuckDB data
"""
import duckdb
import os
from pathlib import Path

# Change to project root
project_root = Path(__file__).parent.parent
os.chdir(project_root)

# Connect to database
con = duckdb.connect('nnpes/nnpes.duckdb', read_only=True)

print("=" * 60)
print("AVAILABLE TABLES")
print("=" * 60)
tables = con.execute("SHOW TABLES").fetchall()
for i, (table_name,) in enumerate(tables, 1):
    print(f"{i}. {table_name}")

print("\n" + "=" * 60)
print("PROVIDER SUMMARY")
print("=" * 60)
result = con.execute("SELECT * FROM mart_provider_summary").fetchdf()
print(result)

print("\n" + "=" * 60)
print("TOP 10 STATES BY PROVIDER COUNT")
print("=" * 60)
result = con.execute("""
    SELECT state, total_providers
    FROM mart_providers_by_state
    ORDER BY total_providers DESC
    LIMIT 10
""").fetchdf()
print(result)

con.close()
print("\n Done!")
