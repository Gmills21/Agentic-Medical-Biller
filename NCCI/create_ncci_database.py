"""NCCI Database Creation Script

This script ingests three CMS NCCI data files and stores them in an SQLite database
named 'NCCI.db' with three indexed tables: PTP_EDITS, MUE_EDITS, and ADDON_EDITS.
"""

import sqlite3
from pathlib import Path

import pandas as pd

# Configuration
BASE_DIR = Path(__file__).resolve().parent
NCCI_DIR = BASE_DIR  # Data files are now in the same directory as the script
DB_PATH = BASE_DIR / "NCCI.db"

# File paths
PTP_FILE = NCCI_DIR / "ccipra-v313r0-f1.TXT"
MUE_FILE = NCCI_DIR / "MCR_MUE_PractitionerServices_Eff_10-01-2025.csv"
ADDON_FILE = NCCI_DIR / "AOC_V2025Q4_01-MCR.txt"  # Using actual filename found


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all database tables with proper schema and indexes."""
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS PTP_EDITS")
    cursor.execute("DROP TABLE IF EXISTS MUE_EDITS")
    cursor.execute("DROP TABLE IF EXISTS ADDON_EDITS")
    
    # Create PTP_EDITS table
    cursor.execute("""
        CREATE TABLE PTP_EDITS (
            code_1 TEXT NOT NULL,
            code_2 TEXT NOT NULL,
            eff_date TEXT,
            del_date TEXT,
            mod_indicator TEXT,
            PRIMARY KEY (code_1, code_2)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ptp_code_1 ON PTP_EDITS(code_1)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ptp_code_2 ON PTP_EDITS(code_2)")
    
    # Create MUE_EDITS table (schema will be determined from CSV header)
    # We'll create it after reading the file to get all columns
    
    # Create ADDON_EDITS table (schema will be determined from file header)
    # We'll create it after reading the file to get all columns
    
    conn.commit()
    print("Database schema created successfully.")


def load_ptp_edits(conn: sqlite3.Connection) -> None:
    """Load PTP Edits from tab-delimited file with no header."""
    print("\nLoading PTP Edits...")
    
    if not PTP_FILE.exists():
        print(f"  ERROR: File not found: {PTP_FILE}")
        return
    
    # Read PTP file - tab-delimited, skip 8 junk rows (copyright, blank lines, headers)
    # Columns: code_1 (col 0), code_2 (col 1), mod_indicator (col 5)
    df = pd.read_csv(
        PTP_FILE,
        sep='\t',
        header=None,
        skiprows=8,  # Skip all junk rows before data starts
        usecols=[0, 1, 5],  # Only read code_1, code_2, and mod_indicator columns
        names=['code_1', 'code_2', 'mod_indicator'],
        dtype=str,
        encoding='latin-1',
        on_bad_lines='skip'
    )
    
    # Clean up the data (remove whitespace, drop empty)
    # Strip whitespace from all code columns before filtering
    df['code_1'] = df['code_1'].str.strip()
    df['code_2'] = df['code_2'].str.strip()
    df['mod_indicator'] = df['mod_indicator'].str.strip()
    
    # Filter out rows where code_1 or code_2 is NULL/empty
    initial_count = len(df)
    df = df.dropna(subset=['code_1', 'code_2'])
    df = df[(df['code_1'] != '') & (df['code_2'] != '')]
    filtered_count = initial_count - len(df)
    
    if filtered_count > 0:
        print(f"  Warning: Filtered out {filtered_count:,} rows with NULL/empty code_1 or code_2")
    
    # Remove duplicate rows based on code_1 and code_2
    before_dedup_count = len(df)
    df = df.drop_duplicates(subset=['code_1', 'code_2'], keep='first')
    duplicate_count = before_dedup_count - len(df)
    
    if duplicate_count > 0:
        print(f"  Removed {duplicate_count:,} duplicate rows based on code_1 and code_2")
    
    # Final strip of all code columns right before saving to ensure no whitespace
    df['code_1'] = df['code_1'].astype(str).str.strip()
    df['code_2'] = df['code_2'].astype(str).str.strip()
    df['mod_indicator'] = df['mod_indicator'].astype(str).str.strip()
    
    # Filter out any rows that became empty after stripping (handle 'nan' strings)
    df = df[(df['code_1'] != '') & (df['code_1'] != 'nan') & 
            (df['code_2'] != '') & (df['code_2'] != 'nan')]
    
    # Write to database
    df.to_sql('PTP_EDITS', conn, if_exists='append', index=False)
    
    print(f"  Loaded {len(df):,} PTP edit records into PTP_EDITS table.")


def load_mue_edits(conn: sqlite3.Connection) -> None:
    """Load MUE Edits from fixed-width file with header."""
    print("\nLoading MUE Edits...")
    
    if not MUE_FILE.exists():
        print(f"  ERROR: File not found: {MUE_FILE}")
        return
    
    # Define the column names and their character positions
    # CPT: chars 0-5, MAX_UNITS (Practitioner units): chars 6-10
    col_specs = [(0, 5), (6, 10)]
    col_names = ['CPT', 'MAX_UNITS']
    
    # Read MUE file - fixed-width, skip copyright lines and header row
    # Header is on line 8, data starts on line 9
    df = pd.read_fwf(
        MUE_FILE,
        colspecs=col_specs,
        names=col_names,
        skiprows=8,  # Skip copyright lines and header row
        header=None,  # No header row
        encoding='latin-1',
        dtype=str
    )
    
    # Clean up the data
    df['CPT'] = df['CPT'].str.strip()
    df['MAX_UNITS'] = df['MAX_UNITS'].str.strip()
    
    # Extract first number from MAX_UNITS (handles cases like "1,2" -> "1")
    # Split on comma and take first value
    df['MAX_UNITS'] = df['MAX_UNITS'].str.split(',').str[0]
    df['MAX_UNITS'] = df['MAX_UNITS'].str.strip()
    
    # Convert MAX_UNITS to integer
    df['MAX_UNITS'] = pd.to_numeric(df['MAX_UNITS'], errors='coerce').astype('Int64')
    
    # Create table
    cursor = conn.cursor()
    
    create_sql = """
        CREATE TABLE IF NOT EXISTS MUE_EDITS (
            "CPT" TEXT NOT NULL,
            "MAX_UNITS" INTEGER
        )
    """
    cursor.execute(create_sql)
    
    # Create index on CPT code column
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_mue_cpt ON MUE_EDITS("CPT")')
    conn.commit()
    
    # Write to database
    df.to_sql('MUE_EDITS', conn, if_exists='append', index=False)
    
    print(f"  Loaded {len(df):,} MUE edit records into MUE_EDITS table.")


def load_addon_edits(conn: sqlite3.Connection) -> None:
    """Load ADDON Edits from fixed-width file with no header."""
    print("\nLoading ADDON Edits...")
    
    if not ADDON_FILE.exists():
        print(f"  ERROR: File not found: {ADDON_FILE}")
        return
    
    cursor = conn.cursor()
    
    # Define the column names and their character positions
    # ADDON_CODE: chars 0-6, PRIMARY_CODE: chars 11-16 (5 spaces between)
    col_specs = [(0, 6), (11, 16)]
    col_names = ['ADDON_CODE', 'PRIMARY_CODE']
    
    try:
        df = pd.read_fwf(
            ADDON_FILE,
            colspecs=col_specs,      # Explicitly define column positions
            names=col_names,        # Name those columns
            skiprows=0,             # No junk rows, data starts on first line
            header=None,            # No header row
            encoding='latin-1',
            dtype=str               # Read everything as text
        )
        
        # Clean up the data (remove whitespace, drop empty)
        df['ADDON_CODE'] = df['ADDON_CODE'].str.strip()
        df['PRIMARY_CODE'] = df['PRIMARY_CODE'].str.strip()
        df.dropna(subset=['ADDON_CODE', 'PRIMARY_CODE'], inplace=True)
        
        # Remove duplicate pairs before inserting into database
        before_dedup_count = len(df)
        df.drop_duplicates(subset=['ADDON_CODE', 'PRIMARY_CODE'], inplace=True)
        duplicate_count = before_dedup_count - len(df)
        
        if duplicate_count > 0:
            print(f"  Removed {duplicate_count:,} duplicate rows based on ADDON_CODE and PRIMARY_CODE")
        
        # Final strip of all code columns right before saving to ensure no whitespace
        df['ADDON_CODE'] = df['ADDON_CODE'].astype(str).str.strip()
        df['PRIMARY_CODE'] = df['PRIMARY_CODE'].astype(str).str.strip()
        
        # Filter out any rows that became empty after stripping (handle 'nan' strings)
        df = df[(df['ADDON_CODE'] != '') & (df['ADDON_CODE'] != 'nan') & 
                (df['PRIMARY_CODE'] != '') & (df['PRIMARY_CODE'] != 'nan')]
        
        # Create table and load data
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ADDON_EDITS (
            ADDON_CODE TEXT NOT NULL,
            PRIMARY_CODE TEXT NOT NULL,
            UNIQUE(ADDON_CODE, PRIMARY_CODE)
        )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_addon_code ON ADDON_EDITS(ADDON_CODE)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_primary_code ON ADDON_EDITS(PRIMARY_CODE)')
        conn.commit()
        
        df.to_sql('ADDON_EDITS', conn, if_exists='append', index=False)
        
        count = len(df)
        print(f"  Loaded {count} ADDON edit records into ADDON_EDITS table.")
        
    except Exception as e:
        print(f"  ERROR loading ADDON edits: {e}")


def main():
    """Main function to create database and load all NCCI data."""
    print("=" * 60)
    print("NCCI Database Creation")
    print("=" * 60)
    
    # Check if NCCI directory exists
    if not NCCI_DIR.exists():
        print(f"\nERROR: NCCI directory not found: {NCCI_DIR}")
        print("Please ensure the NCCI folder exists with the required files.")
        return
    
    # Connect to database
    conn = sqlite3.connect(str(DB_PATH))
    
    try:
        # Create schema
        create_schema(conn)
        
        # Load all three tables
        load_ptp_edits(conn)
        load_mue_edits(conn)
        load_addon_edits(conn)
        
        # Print summary
        print("\n" + "=" * 60)
        print("Database Creation Summary")
        print("=" * 60)
        
        cursor = conn.cursor()
        tables = ["PTP_EDITS", "MUE_EDITS", "ADDON_EDITS"]
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count:,} records")
            except sqlite3.OperationalError:
                print(f"  {table}: Table not created (file may be missing)")
        
        print("\n" + "=" * 60)
        print("NCCI database creation completed successfully!")
        print(f"Database location: {DB_PATH}")
        print("=" * 60)
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()

