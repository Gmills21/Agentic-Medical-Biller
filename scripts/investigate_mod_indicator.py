import pandas as pd
from pathlib import Path

# Find the PTP file
BASE_DIR = Path(__file__).resolve().parent
NCCI_DIR = BASE_DIR / "NCCI"
PTP_FILE = NCCI_DIR / "ccipra-v313r0-f1.txt"

print("=== Investigating mod_indicator Column ===\n")

# Read a few lines to see the structure
print("First 15 lines of PTP file:")
with open(PTP_FILE, 'r', encoding='latin-1') as f:
    for i, line in enumerate(f):
        if i < 15:
            print(f"Line {i}: {repr(line[:100])}")
        else:
            break

print("\n" + "="*80)
print("\nReading with current col_specs [(0, 5), (6, 11), (40, 41)]:")
print("="*80)

# Read with current specs
df = pd.read_fwf(
    PTP_FILE,
    colspecs=[(0, 5), (6, 11), (40, 41)],
    names=['code_1', 'code_2', 'mod_indicator'],
    skiprows=8,
    header=None,
    encoding='latin-1',
    dtype=str
)

print("\nFirst 10 rows:")
print(df.head(10).to_string())

print("\nUnique mod_indicator values:")
print(df['mod_indicator'].value_counts().head(20))

print("\n" + "="*80)
print("\nChecking what's at different column positions:")
print("="*80)

# Read a sample line to see what's at different positions
with open(PTP_FILE, 'r', encoding='latin-1') as f:
    lines = [f.readline() for _ in range(20)]
    
# Find first data line (after skiprows=8)
if len(lines) > 8:
    data_line = lines[8].rstrip()
    print(f"\nFirst data line (line 8): {repr(data_line)}")
    print(f"Length: {len(data_line)}")
    print(f"\nCharacter positions:")
    print(f"  0-5 (code_1): {repr(data_line[0:6])}")
    print(f"  6-11 (code_2): {repr(data_line[6:12])}")
    print(f"  12-20: {repr(data_line[12:21])}")
    print(f"  20-30: {repr(data_line[20:31])}")
    print(f"  30-40: {repr(data_line[30:41])}")
    print(f"  40-41 (current mod_indicator): {repr(data_line[40:42])}")
    print(f"  40-50: {repr(data_line[40:51])}")
    print(f"  50-60: {repr(data_line[50:61])}")

print("\n" + "="*80)
print("\nChecking multiple data lines to find pattern:")
print("="*80)

# Check several data lines
with open(PTP_FILE, 'r', encoding='latin-1') as f:
    lines = [f.readline() for _ in range(20)]
    
for i in range(8, min(13, len(lines))):
    if i < len(lines):
        line = lines[i].rstrip()
        print(f"\nLine {i}: {repr(line[:80])}")
        if len(line) > 40:
            print(f"  Position 40-41: {repr(line[40:42])}")
            print(f"  Position 12-13: {repr(line[12:14])}")

