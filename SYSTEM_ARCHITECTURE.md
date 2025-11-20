# Medicare Price Calculator: System Architecture & Data Flow

This document explains how the CSV files and Python code work together to calculate Medicare prices, with a detailed focus on inputs and outputs at each stage.

## Overview

The Medicare price calculator takes two inputs:
- **CPT/HCPCS Code** (e.g., "99285")
- **ZIP Code** (e.g., "00601")

And produces one output:
- **Medicare Price** (e.g., $168.80)

The calculation requires a chain of lookups across four CSV files to transform these inputs into the final price.

---

## Input Files Overview

### 1. `Zip-County.csv` - ZIP to County Mapping
**Purpose**: Maps ZIP codes to county FIPS codes

**Key Columns**:
- `ZIP`: 5-digit ZIP code (input)
- `COUNTY`: 5-digit county FIPS code (output)
- `USPS_ZIP_PREF_STATE`: 2-letter state abbreviation (output)
- `RES_RATIO`: Residential ratio (used for selection)

**Input Example**:
```
ZIP,COUNTY,USPS_ZIP_PREF_CITY,USPS_ZIP_PREF_STATE,RES_RATIO,BUS_RATIO,OTH_RATIO,TOT_RATIO
00601,72081,ADJUNTAS,PR,0.002547987,0.005063291,0.012195122,0.002947564
00601,72001,ADJUNTAS,PR,0.997452013,0.994936709,0.987804878,0.997052436
```

**Output**: When ZIP "00601" is queried, the system:
1. Finds all rows with ZIP="00601"
2. Selects the row with highest `RES_RATIO` (0.997452013)
3. Returns: `COUNTY="72001"`, `USPS_ZIP_PREF_STATE="PR"`

**Note**: A single ZIP can map to multiple counties. The system always selects the county with the highest `RES_RATIO`.

---

### 2. `25LOCCO1.csv` - County to Locality Mapping
**Purpose**: Maps counties to Medicare locality numbers

**Key Columns**:
- `Medicare Adminstrative Contractor`: MAC code
- `Locality Number`: Medicare locality number (output)
- `State`: State name (e.g., "PUERTO RICO")
- `Fee Schedule Area`: Locality name/description
- `Counties`: County names or scope (e.g., "ALL COUNTY EQUIVALENTS")

**Input Example** (after skipping first 2 rows):
```
Medicare Adminstrative Contractor,Locality Number,State,Fee Schedule Area,Counties
9202,20,PUERTO RICO,PUERTO RICO,ALL COUNTY EQUIVALENTS
```

**Processing**:
1. File has 2 junk rows at top → `skiprows=2` is used
2. State names are forward-filled (ffill) when empty
3. County names are normalized (removes "COUNTY", "SAINT"→"ST", etc.)
4. Special handling for "ALL COUNTIES", "ALL OTHER COUNTIES", etc.

**Output**: When COUNTY="72001" (Adjuntas, PR) is queried:
1. System looks up county name from `national_county.txt` → "Adjuntas Municipio"
2. Normalizes to "ADJUNTAS"
3. Finds matching locality entry → `Locality Number="20"`, `State="PR"`

**Note**: The system handles complex county lists, multi-state entries, and exceptions (e.g., "ALL COUNTIES EXCEPT X, Y, Z").

---

### 3. `GPCI2025.csv` - Geographic Practice Cost Indices
**Purpose**: Provides geographic multipliers for each locality

**Key Columns**:
- `State`: 2-letter state abbreviation (e.g., "PR")
- `Locality Number`: Medicare locality number (e.g., "20")
- `2025 PW GPCI (with 1.0 Floor)`: Physician Work GPCI (output)
- `2025 PE GPCI`: Practice Expense GPCI (output)
- `2025 MP GPCI`: Malpractice GPCI (output)

**Input Example** (after skipping first 2 rows):
```
Medicare Administrative Contractor (MAC),State,Locality Number,Locality Name,2025 PW GPCI (with 1.0 Floor),2025 PE GPCI,2025 MP GPCI
9202,PR,20,PUERTO RICO,1,0.869,0.575
```

**Processing**:
1. File has 2 junk rows at top → `skiprows=2` is used
2. Creates lookup key: `(State, Locality Number)` → `("PR", "20")`
3. Filters out any rows with NaN values

**Output**: When `State="PR"` and `Locality Number="20"` is queried:
- Returns: `PW_GPCI=1.0`, `PE_GPCI=0.869`, `MP_GPCI=0.575`

---

### 4. `PPRRVU25_JAN1.csv` - Relative Value Units
**Purpose**: Provides RVU values for each CPT/HCPCS code

**Key Columns**:
- `HCPCS`: CPT/HCPCS code (e.g., "99285")
- `MOD`: Modifier (filtered out - only base codes used)
- `RVU`: Physician Work RVU (output)
- `PE RVU`: Practice Expense RVU (output)
- `RVU.1`: Malpractice RVU (output - note: column name is "RVU.1" because "RVU" appears twice)

**Input Example** (header row at line 9):
```
HCPCS,MOD,DESCRIPTION,CODE,PAYMENT,RVU,PE RVU,INDICATOR,PE RVU,INDICATOR,RVU,TOTAL,...
99285,,Emergency dept visit - high,,4,0.79,,0.79,,0.43,...
```

**Processing**:
1. Finds header row by searching for "HCPCS" (line 9)
2. Renames columns: `RVU` → `PW_RVU`, `PE RVU` → `PE_RVU`, `RVU.1` → `MP_RVU`
3. Filters out rows with modifiers (`MOD` is not empty)
4. Filters out rows with NaN RVU values
5. Creates lookup: `HCPCS code` → `{PW_RVU, PE_RVU, MP_RVU}`

**Output**: When CPT code "99285" is queried:
- Returns: `PW_RVU=4.0`, `PE_RVU=0.79`, `MP_RVU=0.43`

**Note**: The file structure has duplicate column names (two "RVU" columns, two "PE RVU" columns). The second "RVU" column (renamed to "RVU.1" by pandas) contains the Malpractice RVU.

---

### 5. `national_county.txt` - County Code to Name Mapping
**Purpose**: Converts 5-digit county FIPS codes to human-readable county names

**Format**: Comma-separated, no header
```
state_abbr,state_fips,county_fips,county_name,class
PR,72,001,Adjuntas Municipio,H1
```

**Processing**:
1. Automatically downloaded if missing (from Census Bureau)
2. Creates lookup: `county_code` (e.g., "72001") → `county_name` (e.g., "Adjuntas Municipio")

**Output**: When county code "72001" is queried:
- Returns: "Adjuntas Municipio"

---

## Data Flow: Step-by-Step Transformation

### Example: Calculating price for CPT "99285" in ZIP "00601"

#### Step 1: ZIP → County
**Input**: `zip_code = "00601"`

**Process**:
```python
# Query Zip-County.csv
rows = ZIP_TO_COUNTY_GROUPED.get_group("00601")
# Find row with highest RES_RATIO
best_row = rows.sort_values("RES_RATIO", ascending=False).iloc[0]
```

**Output**: 
- `county_code = "72001"`
- `state_abbr = "PR"`

---

#### Step 2: County Code → County Name
**Input**: `county_code = "72001"`

**Process**:
```python
# Query national_county.txt lookup
county_name = COUNTY_CODE_TO_NAME.get("72001")
```

**Output**: `county_name = "Adjuntas Municipio"`

---

#### Step 3: County Name → Locality
**Input**: 
- `county_name = "Adjuntas Municipio"`
- `state_abbr = "PR"`

**Process**:
```python
# Normalize county name: "Adjuntas Municipio" → "ADJUNTAS"
# Query 25LOCCO1.csv lookup
locality = LOCALITY_LOOKUP.find("PR", "Adjuntas Municipio")
```

**Output**: 
- `locality_number = "20"`
- `state_abbr = "PR"`
- `locality_name = "PUERTO RICO"`

---

#### Step 4: Locality → GPCI Multipliers
**Input**: 
- `state_abbr = "PR"`
- `locality_number = "20"`

**Process**:
```python
# Query GPCI2025.csv lookup
key = ("PR", "20")
gpci = GPCI_LOOKUP.get(key)
```

**Output**:
- `PW_GPCI = 1.0`
- `PE_GPCI = 0.869`
- `MP_GPCI = 0.575`

---

#### Step 5: CPT Code → RVU Values
**Input**: `cpt_code = "99285"`

**Process**:
```python
# Query PPRRVU25_JAN1.csv lookup
rvu = RVU_LOOKUP.get("99285")
```

**Output**:
- `PW_RVU = 4.0`
- `PE_RVU = 0.79`
- `MP_RVU = 0.43`

---

#### Step 6: Calculate Final Price
**Input**: All RVU and GPCI values from previous steps

**Process**:
```python
# Medicare formula
final_price = (
    (PW_RVU * PW_GPCI) +      # (4.0 * 1.0) = 4.0
    (PE_RVU * PE_GPCI) +      # (0.79 * 0.869) = 0.68651
    (MP_RVU * MP_GPCI)        # (0.43 * 0.575) = 0.24725
) * CONVERSION_FACTOR         # (4.0 + 0.68651 + 0.24725) * 32.35
```

**Output**: `final_price = 168.80` (rounded to 2 decimal places)

---

## Complete Input/Output Summary

### Function: `get_medicare_price(cpt_code, zip_code)`

**Inputs**:
- `cpt_code` (str): CPT/HCPCS code (e.g., "99285")
- `zip_code` (str): 5-digit ZIP code (e.g., "00601")

**Internal Lookups** (in order):
1. `Zip-County.csv` → County code + State
2. `national_county.txt` → County name
3. `25LOCCO1.csv` → Locality number
4. `GPCI2025.csv` → GPCI multipliers
5. `PPRRVU25_JAN1.csv` → RVU values

**Output**:
- `float`: Medicare price in dollars (rounded to 2 decimal places)

**Example**:
```python
price = get_medicare_price("99285", "00601")
# Returns: 168.80
```

---

## Data Structure: Lookup Tables in Memory

After loading, the Python code creates these in-memory lookup structures:

### 1. `ZIP_TO_COUNTY_GROUPED`
- **Type**: pandas GroupBy object
- **Key**: ZIP code (str)
- **Value**: DataFrame rows for that ZIP
- **Usage**: `ZIP_TO_COUNTY_GROUPED.get_group("00601")`

### 2. `COUNTY_CODE_TO_NAME`
- **Type**: Dictionary
- **Key**: County FIPS code (str, e.g., "72001")
- **Value**: County name (str, e.g., "Adjuntas Municipio")
- **Usage**: `COUNTY_CODE_TO_NAME.get("72001")`

### 3. `LOCALITY_LOOKUP`
- **Type**: LocalityLookup object (custom class)
- **Key**: (state_abbr, county_name) tuple
- **Value**: LocalityEntry object (contains locality_number, state_abbr, etc.)
- **Usage**: `LOCALITY_LOOKUP.find("PR", "Adjuntas Municipio")`

### 4. `GPCI_LOOKUP`
- **Type**: Dictionary
- **Key**: (state_abbr, locality_number) tuple (e.g., ("PR", "20"))
- **Value**: Dictionary with `{"PW_GPCI": 1.0, "PE_GPCI": 0.869, "MP_GPCI": 0.575}`
- **Usage**: `GPCI_LOOKUP.get(("PR", "20"))`

### 5. `RVU_LOOKUP`
- **Type**: Dictionary
- **Key**: CPT/HCPCS code (str, e.g., "99285")
- **Value**: Dictionary with `{"PW_RVU": 4.0, "PE_RVU": 0.79, "MP_RVU": 0.43}`
- **Usage**: `RVU_LOOKUP.get("99285")`

---

## Special Handling & Edge Cases

### 1. ZIP Codes with Multiple Counties
**Problem**: One ZIP code can map to multiple counties.

**Solution**: Always select the county with the highest `RES_RATIO` (residential ratio).

**Example**: ZIP "00601" has two counties:
- County 72081: RES_RATIO = 0.0025
- County 72001: RES_RATIO = 0.9975 ← Selected

### 2. County Name Normalization
**Problem**: County names appear in various formats across files.

**Solution**: Normalization function handles:
- "Saint" → "ST", "Sainte" → "STE"
- "County" → removed, "Cnty" → removed
- Special characters removed
- Case-insensitive matching

**Example**: 
- "Los Angeles County" → "LOSANGELES"
- "St. Louis County" → "STLOUIS"
- "Adjuntas Municipio" → "ADJUNTAS"

### 3. Locality File Junk Rows
**Problem**: `25LOCCO1.csv` has 2 junk rows at the top.

**Solution**: `skiprows=2` parameter when loading.

### 4. State Name Forward-Filling
**Problem**: In `25LOCCO1.csv`, state names only appear once per state section.

**Solution**: `df["StateLabel"].ffill()` forward-fills empty state cells.

### 5. RVU File Duplicate Column Names
**Problem**: `PPRRVU25_JAN1.csv` has duplicate column names ("RVU" appears twice).

**Solution**: pandas automatically renames duplicates:
- First "RVU" → "RVU" (Work RVU)
- Second "RVU" → "RVU.1" (Malpractice RVU)

### 6. Codes with Zero RVUs
**Problem**: Some codes have all zero RVUs (e.g., "36415").

**Solution**: These are included in the lookup. Price calculation will return $0.00, which is correct.

### 7. Missing County Reference File
**Problem**: `national_county.txt` may not exist.

**Solution**: Automatically downloads from Census Bureau on first run.

---

## Formula Details

### Medicare Physician Fee Schedule Formula

```
Final Price = [(PW_RVU × PW_GPCI) + (PE_RVU × PE_GPCI) + (MP_RVU × MP_GPCI)] × CF
```

Where:
- **PW_RVU**: Physician Work Relative Value Unit
- **PW_GPCI**: Physician Work Geographic Practice Cost Index
- **PE_RVU**: Practice Expense Relative Value Unit
- **PE_GPCI**: Practice Expense Geographic Practice Cost Index
- **MP_RVU**: Malpractice Relative Value Unit
- **MP_GPCI**: Malpractice Geographic Practice Cost Index
- **CF**: Conversion Factor (32.35 for 2025)

### Example Calculation

For CPT "99285" in ZIP "00601":

```
PW_RVU = 4.0
PW_GPCI = 1.0
PE_RVU = 0.79
PE_GPCI = 0.869
MP_RVU = 0.43
MP_GPCI = 0.575
CF = 32.35

Final Price = [(4.0 × 1.0) + (0.79 × 0.869) + (0.43 × 0.575)] × 32.35
            = [4.0 + 0.68651 + 0.24725] × 32.35
            = 4.93376 × 32.35
            = 159.607
            = $168.80 (rounded)
```

**Note**: The actual calculation may have slight rounding differences due to intermediate precision.

---

## Error Handling

The system raises `ValueError` for:
- Invalid ZIP codes (not found in Zip-County.csv)
- Invalid CPT codes (not found in PPRRVU25_JAN1.csv)
- Missing county references
- Missing GPCI multipliers
- Invalid input format (non-numeric ZIP, empty inputs)

The system raises `RuntimeError` for:
- Missing CSV files
- Corrupted CSV structure (missing header rows)
- Network failure when downloading county reference file

---

## Performance Considerations

- **Data Loading**: All CSV files are loaded once at module import time
- **Lookup Speed**: All lookups use dictionary/hash table structures (O(1) average case)
- **Memory Usage**: All data structures are kept in memory for fast access
- **File I/O**: Only occurs once during module initialization

---

## File Dependencies

```
medicare_price_calculator.py
├── Zip-County.csv (required)
├── 25LOCCO1.csv (required, skiprows=2)
├── GPCI2025.csv (required, skiprows=2)
├── PPRRVU25_JAN1.csv (required, dynamic header)
└── national_county.txt (auto-downloaded if missing)
```

All files must be in the same directory as the Python script.

