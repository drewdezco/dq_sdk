"""
Generate sample business/network CSV: 6 columns, ~1000 rows.
One column is fully null; the rest are ~90% complete (10% nulls).
Run from project root: python scripts/generate_sample_data.py
"""

import os
import random
import pandas as pd

ROWS = 1000
COMPLETENESS = 0.90  # 90% non-null in non-null columns
SEED = 42
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sample_data.csv")

def main():
    random.seed(SEED)
    # id: no nulls
    ids = list(range(1, ROWS + 1))
    # customer_id: ~90% complete
    customer_ids = [f"CUST-{i:05d}" if random.random() < COMPLETENESS else None for i in range(1, ROWS + 1)]
    # region: North, South, East, West
    regions = ["North", "South", "East", "West"]
    region_vals = [random.choice(regions) if random.random() < COMPLETENESS else None for _ in range(ROWS)]
    # status
    statuses = ["active", "inactive", "pending"]
    status_vals = [random.choice(statuses) if random.random() < COMPLETENESS else None for _ in range(ROWS)]
    # amount: 10 to 10000, ~90% complete
    amount_vals = [round(random.uniform(10, 10000), 2) if random.random() < COMPLETENESS else None for _ in range(ROWS)]
    # notes: fully null
    notes = [None] * ROWS

    df = pd.DataFrame({
        "id": ids,
        "customer_id": customer_ids,
        "region": region_vals,
        "status": status_vals,
        "amount": amount_vals,
        "notes": notes,
    })
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {OUTPUT_PATH} ({len(df)} rows, 6 columns)")
    print("Completeness (non-null %):", (df.notna().sum() / len(df) * 100).round(1).to_dict())

if __name__ == "__main__":
    main()
