import sqlite3

conn = sqlite3.connect('NCCI.db')
cursor = conn.cursor()

# Check if specific test codes exist
print("=== Checking Test Codes ===\n")

# Check G0439 in MUE
cursor.execute('SELECT "CPT", "MAX_UNITS" FROM MUE_EDITS WHERE "CPT" = ?', ('G0439',))
mue_g0439 = cursor.fetchall()
print(f'G0439 in MUE_EDITS: {mue_g0439}')

# Check 99215/G0008 pair in PTP
cursor.execute('SELECT code_1, code_2, mod_indicator FROM PTP_EDITS WHERE code_1 = ? AND code_2 = ?', ('99215', 'G0008'))
ptp_pair = cursor.fetchall()
print(f'\n99215/G0008 pair in PTP_EDITS: {ptp_pair}')

# Check if 99215 exists at all in PTP
cursor.execute('SELECT code_1, code_2 FROM PTP_EDITS WHERE code_1 = ? LIMIT 5', ('99215',))
ptp_99215 = cursor.fetchall()
print(f'\nSample pairs with code_1=99215: {ptp_99215}')

# Check if G0008 exists at all in PTP
cursor.execute('SELECT code_1, code_2 FROM PTP_EDITS WHERE code_2 = ? LIMIT 5', ('G0008',))
ptp_g0008 = cursor.fetchall()
print(f'\nSample pairs with code_2=G0008: {ptp_g0008}')

# Check 99417 in ADDON
cursor.execute('SELECT ADDON_CODE, PRIMARY_CODE FROM ADDON_EDITS WHERE ADDON_CODE = ?', ('99417',))
addon_99417 = cursor.fetchall()
print(f'\n99417 in ADDON_EDITS: {addon_99417}')

# Check sample codes that DO exist
print("\n=== Sample Codes That Exist ===\n")
cursor.execute('SELECT "CPT", "MAX_UNITS" FROM MUE_EDITS LIMIT 5')
print(f'Sample MUE codes: {cursor.fetchall()}')

cursor.execute('SELECT code_1, code_2, mod_indicator FROM PTP_EDITS LIMIT 5')
print(f'\nSample PTP pairs: {cursor.fetchall()}')

cursor.execute('SELECT ADDON_CODE, PRIMARY_CODE FROM ADDON_EDITS LIMIT 5')
print(f'\nSample ADDON pairs: {cursor.fetchall()}')

conn.close()

