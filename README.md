# Billing Agent

A Python-based billing system for Medicare price calculations and NCCI (National Correct Coding Initiative) violation checking.

## Project Structure

```
Billing AGent/
├── medicare/              # Medicare Price Calculator
│   ├── medicare_price_calculator.py  # Main calculator module
│   ├── create_database.py            # Database creation script
│   ├── test_calculator.py            # Test script
│   ├── SYSTEM_ARCHITECTURE.md        # System documentation
│   ├── HOW_TO_TEST.md                # Testing instructions
│   ├── medicare.db                   # SQLite database
│   ├── national_county.txt           # County reference file
│   └── Medicare CSVS/                # CSV data files
│       ├── 25LOCCO1.csv
│       ├── GPCI2025.csv
│       ├── PPRRVU25_JAN1.csv
│       └── Zip-County.csv
│
├── ncci/                  # NCCI Violation Checker
│   ├── check_ncci.py                 # Main NCCI checker
│   ├── create_ncci_database.py       # Database creation script
│   ├── NCCI.db                       # SQLite database
│   └── [NCCI data files]             # CMS NCCI data files
│       ├── ccipra-v313r0-f1.TXT
│       ├── MCR_MUE_PractitionerServices_Eff_10-01-2025.csv
│       └── AOC_V2025Q4_01-MCR.txt
│
├── scripts/               # Utility and development scripts
│   ├── check_db.py
│   ├── check_mod_indicators.py
│   ├── check_columns.py
│   ├── find_test_codes.py
│   ├── investigate_mod_indicator.py
│   ├── show_ptp_structure.py
│   └── verify_db.py
│
└── .gitignore            # Git ignore file
```

## Features

### Medicare Price Calculator
- Calculates Medicare reimbursement prices based on CPT codes and ZIP codes
- Uses RVU (Relative Value Units), GPCI (Geographic Practice Cost Indices), and conversion factors
- Supports both CSV-based and SQLite database lookups

### NCCI Violation Checker
- Checks for PTP (Procedure-to-Procedure) violations
- Checks for MUE (Medically Unlikely Edits) violations
- Checks for orphaned ADDON code violations
- Supports modifier parsing and bypass logic

## Setup

1. Install required dependencies:
```bash
pip install pandas sqlite3
```

2. Create the Medicare database:
```bash
cd medicare
python create_database.py
```

3. Create the NCCI database:
```bash
cd ncci
python create_ncci_database.py
```

## Usage

### Medicare Price Calculator
```python
from medicare.medicare_price_calculator import get_medicare_price

price = get_medicare_price('99285', '00601')
print(f"Medicare price: ${price}")
```

### NCCI Violation Checker
```python
from ncci.check_ncci import check_ncci_violations

codes = ['0001U', '0001U', '0001U']  # Example: billing same code 3 times
check_ncci_violations(codes)
```

## Notes

- CSV and database files are excluded from Git (see `.gitignore`)
- The `scripts/` folder contains utility scripts for development and debugging
- Both systems use SQLite databases for scalable data storage

