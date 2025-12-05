import duckdb

# Connect to DuckDB database (specify your DB file here)
con = duckdb.connect("data/xwines.duckdb")

# Columns in table, modify to match the CREATE TABLE
categorical_candidate_columns = [
    "type",
    "elaborate",
    "grapes",
    "harmonize",
    "body",
    "acidity",
    "code",
    "country",
    "region_name",
    "winery_name",
    "vintages",
]

# For each column, get number of unique values and print them if < 20
for col in categorical_candidate_columns:
    query = f"""
        SELECT COUNT(DISTINCT "{col}") FROM wines
        WHERE "{col}" IS NOT NULL
    """
    n_unique = con.execute(query).fetchone()[0]
    if n_unique < 50:
        print(f"Column: {col} ({n_unique} distinct values)")
        values_query = f"""
            SELECT DISTINCT "{col}" FROM wines
            WHERE "{col}" IS NOT NULL
            ORDER BY "{col}"
        """
        values = con.execute(values_query).fetchall()
        print([v[0] for v in values])
        print("---")
