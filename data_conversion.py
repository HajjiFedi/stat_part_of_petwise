import sqlite3
import pandas as pd
import numpy as np
import os

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
# Change this path to point to your existing SQLite database file.
SQLITE_DB_PATH = r"C:\Users\fedi\Documents\ESEN-HACK\petstore.db"

# Name of the output CSV file
OUTPUT_CSV_FILENAME = "generated_pet_sales.csv"


# ─────────────────────────────────────────────────────────────────────────────
#  1. CONNECT TO SQLITE DATABASE
# ─────────────────────────────────────────────────────────────────────────────
if not os.path.exists(SQLITE_DB_PATH):
    raise FileNotFoundError(f"SQLite database not found at: {SQLITE_DB_PATH}")

conn = sqlite3.connect(SQLITE_DB_PATH)

# ─────────────────────────────────────────────────────────────────────────────
#  2. QUERY PURCHASES JOINED WITH PRODUCTS
# ─────────────────────────────────────────────────────────────────────────────
query = """
SELECT
    p.id           AS purchase_id,
    pr.name        AS product_name,
    pr.animal_type AS animal_type,
    pr.category    AS category,
    pr.price       AS price,
    p.quantity     AS sales,          -- quantity purchased
    p.date         AS order_date,
    pr.quantity    AS stock_left       -- current stock in products table
FROM
    purchases p
JOIN
    products pr
  ON
    p.product_id = pr.id
ORDER BY
    p.date ASC;
"""

df = pd.read_sql_query(query, conn)

# Ensure order_date is parsed as datetime
df['order_date'] = pd.to_datetime(df['order_date'])


# ─────────────────────────────────────────────────────────────────────────────
#  3. SYNTHETICALLY ENRICH DATA TO MATCH TARGET HEADERS
# ─────────────────────────────────────────────────────────────────────────────

# 3.1 Extract "month" and "weekday" from 'order_date'
df['month']   = df['order_date'].dt.month
df['weekday'] = df['order_date'].dt.weekday   # Monday=0 … Sunday=6

# 3.2 Randomly assign ~30% of rows as discounted
np.random.seed(42)
df['is_discounted'] = np.random.choice([False, True], size=len(df), p=[0.7, 0.3])

# 3.3 If discounted, random percentage between 5%–30%; else 0.0
discounts = np.random.uniform(5.0, 30.0, size=len(df))
df['discount_pct'] = np.where(df['is_discounted'], np.round(discounts, 2), 0.0)

# 3.4 Random average review score between 3.0–5.0
df['avg_review_score'] = np.round(np.random.uniform(3.0, 5.0, size=len(df)), 2)

# 3.5 Random number of reviews between 0–200
df['num_reviews'] = np.random.randint(0, 201, size=len(df))

# 3.6 'in_stock': True if stock_left > 0, else False
df['in_stock'] = df['stock_left'] > 0

# 3.7 Random ad_spend between 0.00–100.00
df['ad_spend'] = np.round(np.random.uniform(0.0, 100.0, size=len(df)), 2)

# 3.8 Random click_through_rate between 0.000–0.100
df['click_through_rate'] = np.round(np.random.uniform(0.0, 0.10, size=len(df)), 3)

# 3.9 'holiday_season': True if month is November (11), December (12), or January (1)
df['holiday_season'] = df['month'].isin([11, 12, 1])


# ─────────────────────────────────────────────────────────────────────────────
#  4. REORDER COLUMNS & EXPORT TO CSV
# ─────────────────────────────────────────────────────────────────────────────
final_columns = [
    'product_name',
    'animal_type',
    'category',
    'price',
    'sales',
    'order_date',
    'month',
    'weekday',
    'is_discounted',
    'discount_pct',
    'avg_review_score',
    'num_reviews',
    'in_stock',
    'stock_left',
    'ad_spend',
    'click_through_rate',
    'holiday_season'
]

df_final = df[final_columns].copy()

# Save to CSV in the current working directory
output_path = os.path.join(os.getcwd(), OUTPUT_CSV_FILENAME)
df_final.to_csv(output_path, index=False)

print(f"\n✅ Generated CSV file with enhanced sales data:\n   {output_path}\n")
print("Sample of the first 5 rows:")
print(df_final.head(5))

# Close the database connection
conn.close()
