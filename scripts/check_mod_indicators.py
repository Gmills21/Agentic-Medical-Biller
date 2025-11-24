import sqlite3

conn = sqlite3.connect('NCCI.db')
cursor = conn.cursor()

# Check mod_indicator values
cursor.execute("SELECT DISTINCT mod_indicator FROM PTP_EDITS WHERE mod_indicator IS NOT NULL AND mod_indicator != '' LIMIT 20")
mod_indicators = cursor.fetchall()
print(f"mod_indicator values: {mod_indicators}")

# Find pairs with mod_indicator = '1'
cursor.execute("SELECT code_1, code_2, mod_indicator FROM PTP_EDITS WHERE mod_indicator = '1' LIMIT 5")
pairs_mod1 = cursor.fetchall()
print(f"\nPairs with mod_indicator='1': {pairs_mod1}")

# Find pairs with mod_indicator = '0'
cursor.execute("SELECT code_1, code_2, mod_indicator FROM PTP_EDITS WHERE mod_indicator = '0' LIMIT 5")
pairs_mod0 = cursor.fetchall()
print(f"\nPairs with mod_indicator='0': {pairs_mod0}")

# Check what the actual mod_indicator values look like
cursor.execute("SELECT code_1, code_2, mod_indicator FROM PTP_EDITS WHERE mod_indicator IS NOT NULL LIMIT 10")
sample = cursor.fetchall()
print(f"\nSample pairs with mod_indicator: {sample}")

conn.close()

