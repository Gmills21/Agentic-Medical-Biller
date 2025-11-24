import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
NCCI_DIR = BASE_DIR / "NCCI"
PTP_FILE = NCCI_DIR / "ccipra-v313r0-f1.txt"

print("=== PTP File Structure Analysis ===\n")

# Read a sample line and split by tabs
with open(PTP_FILE, 'r', encoding='latin-1') as f:
    lines = [f.readline() for _ in range(15)]

print("Header information (lines 0-5):")
for i in range(6):
    print(f"  Line {i}: {lines[i].rstrip()}")

print("\n" + "="*80)
print("First data line (line 8) split by tabs:")
print("="*80)

data_line = lines[8].rstrip()
columns = data_line.split('\t')
print(f"Raw line: {repr(data_line)}")
print(f"\nSplit into {len(columns)} columns:")
for i, col in enumerate(columns):
    print(f"  Column {i}: {repr(col)}")

print("\n" + "="*80)
print("Column mapping based on header:")
print("="*80)
print("  Column 0: code_1 (0001A)")
print("  Column 1: code_2 (0593T)")
print("  Column 2: (empty - prior existence indicator)")
print("  Column 3: Effective Date (20220101)")
print("  Column 4: Deletion Date (20231231)")
print("  Column 5: Modifier Indicator (1) <-- THIS IS WHAT WE NEED!")
print("  Column 6: PTP Edit Rationale")

print("\n" + "="*80)
print("Reading with pd.read_csv(sep='\\t') to verify:")
print("="*80)

df = pd.read_csv(
    PTP_FILE,
    sep='\t',
    header=None,
    skiprows=8,
    dtype=str,
    encoding='latin-1'
)

# Name columns based on what we see
df.columns = ['code_1', 'code_2', 'prior_exist', 'eff_date', 'del_date', 'mod_indicator', 'rationale']

print("\nFirst 10 rows:")
print(df[['code_1', 'code_2', 'mod_indicator']].head(10).to_string())

print("\nUnique mod_indicator values:")
print(df['mod_indicator'].value_counts().head(10))

print("\n" + "="*80)
print("Sample rows with different mod_indicator values:")
print("="*80)
print(df[df['mod_indicator'].isin(['0', '1', '9'])][['code_1', 'code_2', 'mod_indicator']].head(10).to_string())

