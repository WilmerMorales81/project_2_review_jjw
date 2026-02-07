"""
Visualize data from SQL models
Creates charts and graphs for data exploration
"""
import polars as pl
import matplotlib.pyplot as plt
import matplotlib
import os

# Use a non-GUI backend for matplotlib
matplotlib.use('Agg')

# Change to project root
os.chdir(r"c:\Users\jingl\DE 2\project_2_review_jjw")

print("=" * 60)
print("Creating visualizations...")
print("=" * 60)

# Read census data
df = pl.read_csv("data/raw/ssa_fips_state_county_2025.csv", infer_schema_length=10000, ignore_errors=True)

# Create output directory for charts
os.makedirs("visualizations", exist_ok=True)

# Chart 1: Top 15 states by county count
print("\n1. Creating bar chart: Counties per State (Top 15)")
state_counts = df.group_by("state").agg(
    pl.count("fipscounty").alias("county_count")
).sort("county_count", descending=True).head(15)

plt.figure(figsize=(12, 6))
plt.bar(state_counts["state"], state_counts["county_count"], color='steelblue')
plt.xlabel('State', fontsize=12)
plt.ylabel('Number of Counties', fontsize=12)
plt.title('Top 15 States by County Count', fontsize=14, fontweight='bold')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('visualizations/counties_by_state.png', dpi=300)
plt.close()
print("   Saved: visualizations/counties_by_state.png")

# Chart 2: State distribution pie chart (top 10)
print("\n2. Creating pie chart: State Distribution (Top 10)")
top10 = state_counts.head(10)
plt.figure(figsize=(10, 10))
plt.pie(top10["county_count"], labels=top10["state"], autopct='%1.1f%%', startangle=90)
plt.title('County Distribution by State (Top 10)', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('visualizations/state_distribution.png', dpi=300)
plt.close()
print("   Saved: visualizations/state_distribution.png")

# Chart 3: CBSA coverage
print("\n3. Creating bar chart: CBSA Coverage")
cbsa_coverage = df.group_by(pl.col("cbsa_code").is_not_null()).agg(
    pl.count("fipscounty").alias("count")
)
labels = ['Counties WITHOUT CBSA', 'Counties WITH CBSA']
values = [
    cbsa_coverage.filter(pl.col("cbsa_code") == False)["count"][0] if len(cbsa_coverage.filter(pl.col("cbsa_code") == False)) > 0 else 0,
    cbsa_coverage.filter(pl.col("cbsa_code") == True)["count"][0] if len(cbsa_coverage.filter(pl.col("cbsa_code") == True)) > 0 else 0
]

plt.figure(figsize=(10, 6))
plt.bar(labels, values, color=['coral', 'lightgreen'])
plt.ylabel('Number of Counties', fontsize=12)
plt.title('Counties by CBSA Coverage', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('visualizations/cbsa_coverage.png', dpi=300)
plt.close()
print("   Saved: visualizations/cbsa_coverage.png")

print("\n" + "=" * 60)
print("✓ All visualizations created successfully!")
print("=" * 60)
print("\nTo view the charts:")
print("  Open the 'visualizations' folder in your project directory")
print("  Files created:")
print("  - counties_by_state.png")
print("  - state_distribution.png")
print("  - cbsa_coverage.png")
