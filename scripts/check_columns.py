import sqlite3
import pandas as pd

# Check database columns
conn = sqlite3.connect('NCCI.db')
cursor = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='MUE_EDITS'")
result = cursor.fetchone()
if result:
    print("MUE_EDITS CREATE TABLE statement:")
    print(result[0])
else:
    print("Table not found")

# Try to get column names from a sample row
cursor = conn.execute("SELECT * FROM MUE_EDITS LIMIT 1")
row = cursor.fetchone()
if row:
    print("\nColumn names from cursor description:")
    print([desc[0] for desc in cursor.description])
else:
    print("\nTable is empty")

conn.close()

# Check CSV columns
try:
    df = pd.read_csv('NCCI/MCR_MUE_PractitionerServices_Eff_10-01-2025.csv', nrows=1, encoding='latin-1')
    print("\nMUE CSV column names:")
    print(list(df.columns))
except Exception as e:
    print(f"\nError reading CSV: {e}")

