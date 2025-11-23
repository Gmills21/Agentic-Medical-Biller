"""NCCI Database Creation Script

This script ingests three CMS NCCI data files and stores them in an SQLite database
named 'NCCI.db' with three indexed tables: PTP_EDITS, MUE_EDITS, and ADDON_EDITS.
"""

import sqlite3
from pathlib import Path

import pandas as pd

# Configuration
BASE_DIR = Path(__file__).resolve().parent
NCCI_DIR = BASE_DIR / "NCCI"
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
    
    # Read PTP file - tab-delimited, no header
    # Read all columns first to see structure
    df = pd.read_csv(
        PTP_FILE,
        sep='\t',
        header=None,
        dtype=str,  # Read all as string to preserve leading zeros
        encoding='utf-8',
        on_bad_lines='skip',
        low_memory=False  # Avoid dtype warning
    )
    
    # Map columns based on expected schema: code_1, code_2, eff_date, del_date, mod_indicator
    # Take only first 5 columns and assign names
    if len(df.columns) >= 5:
        df = df.iloc[:, :5]  # Take first 5 columns
        df.columns = ['code_1', 'code_2', 'eff_date', 'del_date', 'mod_indicator']
    else:
        # If fewer columns, pad with None
        while len(df.columns) < 5:
            df[f'col_{len(df.columns)}'] = None
        df.columns = ['code_1', 'code_2', 'eff_date', 'del_date', 'mod_indicator']
    
    # Replace empty strings and NaN with None
    df = df.replace('', None)
    df = df.replace('nan', None)
    df = df.where(pd.notnull(df), None)
    
    # Filter out rows where code_1 or code_2 is NULL/empty
    initial_count = len(df)
    
    # Convert to string first, then check for empty/None
    df['code_1'] = df['code_1'].astype(str)
    df['code_2'] = df['code_2'].astype(str)
    
    # Filter out rows with NULL, empty, or whitespace-only codes
    mask = (
        df['code_1'].notna() & 
        (df['code_1'] != 'None') & 
        (df['code_1'] != 'nan') &
        (df['code_1'].str.strip() != '')
    ) & (
        df['code_2'].notna() & 
        (df['code_2'] != 'None') & 
        (df['code_2'] != 'nan') &
        (df['code_2'].str.strip() != '')
    )
    
    df = df[mask]
    filtered_count = initial_count - len(df)
    
    if filtered_count > 0:
        print(f"  Warning: Filtered out {filtered_count:,} rows with NULL/empty code_1 or code_2")
    
    # Strip whitespace from code columns
    df['code_1'] = df['code_1'].str.strip()
    df['code_2'] = df['code_2'].str.strip()
    
    # Remove duplicate rows based on code_1 and code_2
    before_dedup_count = len(df)
    df = df.drop_duplicates(subset=['code_1', 'code_2'], keep='first')
    duplicate_count = before_dedup_count - len(df)
    
    if duplicate_count > 0:
        print(f"  Removed {duplicate_count:,} duplicate rows based on code_1 and code_2")
    
    # Write to database
    df.to_sql('PTP_EDITS', conn, if_exists='append', index=False)
    
    print(f"  ✓ Loaded {len(df):,} PTP edit records into PTP_EDITS table.")


def load_mue_edits(conn: sqlite3.Connection) -> None:
    """Load MUE Edits from comma-separated file with header."""
    print("\nLoading MUE Edits...")
    
    if not MUE_FILE.exists():
        print(f"  ERROR: File not found: {MUE_FILE}")
        return
    
    # Read MUE file - comma-separated with header
    # Read first to determine columns
    df = pd.read_csv(
        MUE_FILE,
        dtype=str,  # Read all as string first to preserve leading zeros
        encoding='latin-1',
        on_bad_lines='skip'
    )
    
    # Ensure hcpcs_code is TEXT (preserve leading zeros)
    if 'hcpcs_code' in df.columns:
        df['hcpcs_code'] = df['hcpcs_code'].astype(str)
    
    # Convert mue_value and mai_type to integers if they exist
    if 'mue_value' in df.columns:
        df['mue_value'] = pd.to_numeric(df['mue_value'], errors='coerce').astype('Int64')
    if 'mai_type' in df.columns:
        df['mai_type'] = pd.to_numeric(df['mai_type'], errors='coerce').astype('Int64')
    
    # Create table dynamically based on columns
    cursor = conn.cursor()
    
    # Build CREATE TABLE statement
    # SQLite requires column names with special characters or reserved words to be quoted
    columns = []
    for col in df.columns:
        # Quote column names to handle special characters and reserved words
        quoted_col = f'"{col}"'
        col_lower = col.lower()
        if col_lower == 'hcpcs_code':
            columns.append(f"{quoted_col} TEXT")
        elif col_lower in ['mue_value', 'mai_type']:
            columns.append(f"{quoted_col} INTEGER")
        else:
            # Default to TEXT for other columns
            columns.append(f"{quoted_col} TEXT")
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS MUE_EDITS (
            {', '.join(columns)}
        )
    """
    cursor.execute(create_sql)
    
    # Create indexes
    if 'hcpcs_code' in df.columns:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mue_hcpcs_code ON MUE_EDITS("hcpcs_code")')
    conn.commit()
    
    # Write to database
    df.to_sql('MUE_EDITS', conn, if_exists='append', index=False)
    
    print(f"  ✓ Loaded {len(df):,} MUE edit records into MUE_EDITS table.")
    print(f"    Columns: {', '.join(df.columns.tolist())}")


def load_addon_edits(conn: sqlite3.Connection) -> None:
    """Load ADDON Edits from tab-delimited file with header."""
    print("\nLoading ADDON Edits...")
    
    if not ADDON_FILE.exists():
        print(f"  ERROR: File not found: {ADDON_FILE}")
        return
    
    # Read ADDON file - tab-delimited with header
    df = pd.read_csv(
        ADDON_FILE,
        sep='\t',
        dtype=str,  # Read all as string first to preserve leading zeros
        encoding='utf-8',
        on_bad_lines='skip'
    )
    
    # Ensure any code columns are TEXT (preserve leading zeros)
    code_columns = [col for col in df.columns if 'code' in col.lower() or 'hcpcs' in col.lower()]
    for col in code_columns:
        df[col] = df[col].astype(str)
    
    # Create table dynamically based on columns
    cursor = conn.cursor()
    
    # Build CREATE TABLE statement
    # SQLite requires column names with special characters or reserved words to be quoted
    columns = []
    for col in df.columns:
        # Quote column names to handle special characters and reserved words
        quoted_col = f'"{col}"'
        col_lower = col.lower()
        if 'code' in col_lower or 'hcpcs' in col_lower:
            columns.append(f"{quoted_col} TEXT")
        else:
            # Default to TEXT for other columns
            columns.append(f"{quoted_col} TEXT")
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS ADDON_EDITS (
            {', '.join(columns)}
        )
    """
    cursor.execute(create_sql)
    
    # Create indexes on code columns
    for col in code_columns:
        quoted_col = f'"{col}"'
        # Sanitize column name for index name (replace spaces/special chars with underscore)
        index_name = col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_addon_{index_name} ON ADDON_EDITS({quoted_col})')
    conn.commit()
    
    # Write to database
    df.to_sql('ADDON_EDITS', conn, if_exists='append', index=False)
    
    print(f"  ✓ Loaded {len(df):,} ADDON edit records into ADDON_EDITS table.")
    print(f"    Columns: {', '.join(df.columns.tolist())}")


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

