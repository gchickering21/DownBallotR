import pandas as pd

from .pipeline import NcElectionPipeline
from .constants import NC_MIN_SUPPORTED_ELECTION_DATE

pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 140)

print("\n=== RUN FULL NC PIPELINE (LATEST) ===")

pipeline = NcElectionPipeline()

df, county_final, state_final = pipeline.run()  # default = all available elections
assert df.empty or df["year"].min() >= NC_MIN_SUPPORTED_ELECTION_DATE.year
print(f"✅ Confirmed: no results earlier than {NC_MIN_SUPPORTED_ELECTION_DATE}")

print(f"Final shape: {df.shape}")
print(df.head(10))

print("\n=== SCHEMA CHECK ===")
print(df.columns.tolist())

print("\nOffice counts:")
print(df["office"].value_counts(dropna=False).head(10))

print("\nJurisdiction examples:")
print(
    df[["office", "jurisdiction", "jurisdiction_type"]]
    .drop_duplicates()
   
    .head(10)
)

print("\nVote share summary:")
print(df["vote_share"].describe())

print("\nWinner flag summary:")
print(df["won"].value_counts(dropna=False))

print("\n✅ FULL NC PIPELINE RAN WITHOUT CRASHING")
