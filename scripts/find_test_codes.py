import sqlite3

conn = sqlite3.connect('NCCI.db')
cursor = conn.cursor()

print("=== Finding Real Test Codes ===\n")

# 1. Find a real MUE violation - code with MAX_UNITS = 1
print("1. MUE Violation Test - Finding code with MAX_UNITS = 1:")
cursor.execute('SELECT "CPT", "MAX_UNITS" FROM MUE_EDITS WHERE "MAX_UNITS" = 1 LIMIT 5')
mue_codes = cursor.fetchall()
print(f"   Sample codes with MAX_UNITS=1: {mue_codes}")
if mue_codes:
    test_mue_code = mue_codes[0][0]
    print(f"   Using: {test_mue_code}\n")

# 2. Find a real PTP pair with mod_indicator = '1' (bypassable)
print("2. PTP Violation Test - Finding pair with mod_indicator = '1':")
cursor.execute("SELECT code_1, code_2, mod_indicator FROM PTP_EDITS WHERE mod_indicator = '1' LIMIT 5")
ptp_pairs = cursor.fetchall()
print(f"   Sample pairs with mod_indicator='1': {ptp_pairs}")
if ptp_pairs:
    test_ptp_code1 = ptp_pairs[0][0]
    test_ptp_code2 = ptp_pairs[0][1]
    print(f"   Using: {test_ptp_code1} / {test_ptp_code2}\n")

# 3. Find a real ADDON code
print("3. ADDON Violation Test - Finding an ADDON code:")
cursor.execute('SELECT DISTINCT ADDON_CODE FROM ADDON_EDITS LIMIT 5')
addon_codes = cursor.fetchall()
print(f"   Sample ADDON codes: {addon_codes}")
if addon_codes:
    test_addon_code = addon_codes[0][0]
    # Get its primary codes
    cursor.execute('SELECT PRIMARY_CODE FROM ADDON_EDITS WHERE ADDON_CODE = ? LIMIT 3', (test_addon_code,))
    primary_codes = cursor.fetchall()
    print(f"   Using ADDON code: {test_addon_code}")
    print(f"   Its primary codes: {[p[0] for p in primary_codes]}")
    if primary_codes:
        test_primary_code = primary_codes[0][0]
        print(f"   Using primary code: {test_primary_code}\n")

# Print test code suggestions
print("\n=== Suggested Test Codes ===")
if mue_codes:
    print(f"Test 1 (MUE): ['{test_mue_code}', '{test_mue_code}', '{test_mue_code}']")
if ptp_pairs:
    print(f"Test 2 (PTP no modifier): ['{test_ptp_code1}', '{test_ptp_code2}']")
    print(f"Test 3 (PTP with modifier): ['{test_ptp_code1}-25', '{test_ptp_code2}']")
if addon_codes and primary_codes:
    print(f"Test 4 (Orphaned ADDON): ['{test_addon_code}']")
    print(f"Test 5 (Correct ADDON): ['{test_primary_code}', '{test_addon_code}']")

conn.close()

