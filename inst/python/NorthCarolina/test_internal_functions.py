import pandas as pd

from discovery import discover_nc_results_zips
from normalize import normalize_nc_results_cols
from aggregate import aggregate_to_contest_level
from io_utils import download_zip_bytes, read_results_pct_from_zip

pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 140)

print("\n=== STEP 1: DISCOVER ELECTIONS ===")
elections = discover_nc_results_zips()
print(f"Found {len(elections)} elections")
latest = elections[-1]
print("Latest:", latest)

print("\n=== STEP 2: DOWNLOAD ZIP ===")
zip_bytes = download_zip_bytes(latest.zip_url)
print(f"ZIP size: {len(zip_bytes):,} bytes")

print("\n=== STEP 3: READ RESULTS ===")
member, raw_df = read_results_pct_from_zip(zip_bytes)
print("Member:", member)
print("Raw shape:", raw_df.shape)
print(raw_df.head(3))

print("\n=== STEP 4: NORMALIZE ===")
norm_df = normalize_nc_results_cols(raw_df)
print(norm_df.head(3))

print("\n=== STEP 5: AGGREGATE ===")
contest_df = aggregate_to_contest_level(norm_df)
print(contest_df.head(5))

print("\n✅ LOW-LEVEL PIPELINE OK")
