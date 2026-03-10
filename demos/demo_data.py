"""
Build demo DataFrames for use case 1 (all validations), use case 2 (comparison), and use case 3 (auto-suggestions).
No dependency on scripts/generate_sample_data.py or existing CSVs.
"""

import pandas as pd


def build_demo_df_all_validations():
    """
    Build a single DataFrame with columns chosen so every expectation and
    similarity check can run. Includes intentional failures for demo effect.
    """
    # id: unique, not null (pass)
    # customer_id: some nulls (fail completeness), some duplicates for composite-unique
    # region: one invalid value
    # status: one invalid value
    # amount: one out of range
    # notes: all null
    # order_date: mix of recent and old; one out of date range
    # sku: mostly valid pattern, some invalid
    # category: one not in reference set
    # name_primary, name_alt: for Levenshtein similarity
    n = 24
    ids = list(range(1, n + 1))
    customer_ids = [f"CUST-{i:03d}" for i in range(1, n)] + [None]  # one null
    customer_ids[1] = customer_ids[0]  # duplicate for composite-unique demo
    regions = ["North", "South", "East", "West"] * (n // 4) + ["North", "South"][: n % 4]
    regions[2] = "Invalid"
    statuses = ["active", "inactive", "pending"] * (n // 3) + ["active", "inactive"][: n % 3]
    statuses[3] = "unknown"
    amounts = [100.0, 200.0, 300.0] * (n // 3) + [100.0] * (n % 3)
    amounts[4] = -1.0
    amounts[5] = 200000.0
    notes = [None] * n
    # order_date: mix of recent and old (relative to a fixed reference)
    base_dates = pd.date_range("2024-01-01", periods=n, freq="15D")
    order_dates = list(base_dates)
    order_dates[6] = pd.Timestamp("2020-06-01")  # old (fails "recent")
    order_dates[7] = pd.Timestamp("2030-01-01")  # out of range if range is 2023-2025
    order_dates[8] = pd.NaT  # null
    skus = ["A1", "B2", "C3"] * (n // 3) + ["A1", "B2"][: n % 3]
    skus[9] = "bad"
    skus[10] = "X"
    categories = ["Electronics", "Clothing", "Home"] * (n // 3) + ["Electronics", "Clothing"][: n % 3]
    categories[11] = "Other"  # not in reference set
    name_primary = ["Alice", "Bob", "Charlie", "Diana", "Eve"] * (n // 5) + ["Alice", "Bob", "Charlie", "Diana"][: n % 5]
    name_alt = ["Alicia", "Robert", "Charles", "Diane", "Eva"] * (n // 5) + ["Alicia", "Robert", "Charles", "Diane"][: n % 5]
    name_alt[12] = "xyz"  # low similarity with name_primary

    return pd.DataFrame({
        "id": ids,
        "customer_id": customer_ids,
        "region": regions,
        "status": statuses,
        "amount": amounts,
        "notes": notes,
        "order_date": order_dates,
        "sku": skus,
        "category": categories,
        "name_primary": name_primary,
        "name_alt": name_alt,
    })


def build_comparison_dfs():
    """
    Build two small DataFrames with a common key for reconciliation / compare-reports demo.
    Returns (df_left, df_right) with shared key_column = "id".
    """
    # Left: warehouse-style (3 rows)
    df_left = pd.DataFrame({
        "id": [1, 2, 3],
        "amount": [100.0, 200.0, 300.0],
        "status": ["active", "inactive", "active"],
        "name": ["Alice", "Bob", "Charlie"],
    })
    # Right: source-style (2 matching keys, 1 only in right, different amounts/names
    df_right = pd.DataFrame({
        "id": [1, 2, 4],
        "amount": [100.0, 250.0, 400.0],
        "status": ["active", "inactive", "pending"],
        "name": ["Alicia", "Bob", "Dave"],
    })
    return df_left, df_right


def build_demo_df_for_suggestions():
    """
    Build a DataFrame with clean data designed to trigger various auto-suggestion types.
    Columns are chosen to demonstrate different suggestion capabilities:
    - Low cardinality categorical → in_set
    - Email pattern → regex pattern
    - UUID pattern → regex pattern
    - Numeric range → in_range
    - Recent dates → recent or date_range
    - Low null rate → not_null
    - High uniqueness → unique
    """
    import uuid
    
    n = 30  # Enough samples for reliable suggestions
    
    # id: Unique integers (will suggest uniqueness)
    ids = list(range(1, n + 1))
    
    # email: Email addresses (will suggest regex pattern)
    emails = [
        f"user{i}@example.com" for i in range(1, n + 1)
    ]
    
    # user_id: UUIDs (will suggest regex pattern)
    user_ids = [str(uuid.uuid4()) for _ in range(n)]
    
    # status: Low cardinality categorical (will suggest in_set)
    statuses = ["active", "inactive", "pending"] * (n // 3) + ["active", "inactive", "pending"][:n % 3]
    
    # score: Numeric values in a reasonable range (will suggest in_range)
    scores = [10.0, 20.0, 30.0, 40.0, 50.0] * (n // 5) + [10.0, 20.0, 30.0, 40.0, 50.0][:n % 5]
    
    # created_at: Recent dates (will suggest recent or date_range)
    # Use dates within the last 30 days from a reference point
    base_date = pd.Timestamp("2024-06-01")
    created_dates = pd.date_range(base_date - pd.Timedelta(days=25), periods=n, freq="D")
    
    # name: String column with low/null null rate (will suggest not_null)
    names = [f"User_{i}" for i in range(1, n + 1)]
    
    return pd.DataFrame({
        "id": ids,
        "email": emails,
        "user_id": user_ids,
        "status": statuses,
        "score": scores,
        "created_at": created_dates,
        "name": names,
    })
