"""Database schema creation and migration script.

This script creates the medicare.db database schema and populates it
from the CSV files for better scalability than in-memory lookups.
"""

import sqlite3
from pathlib import Path

from medicare_price_calculator import (
    BASE_DIR,
    DB_PATH,
    FILES,
    _ensure_county_reference_file,
    _find_header_row,
    _normalize_locality_number,
    get_db_connection,
)

import pandas as pd


def create_schema(conn: sqlite3.Connection, drop_existing: bool = False) -> None:
    """Create all database tables with proper schema."""
    cursor = conn.cursor()
    
    if drop_existing:
        # Drop existing tables if they exist
        tables = ["zip_to_county", "county_reference", "county_locality", "gpci", "rvu"]
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        # Drop indexes
        indexes = [
            "idx_zip", "idx_zip_res_ratio", "idx_state_locality", 
            "idx_gpci_state_locality", "idx_hcpcs"
        ]
        for idx in indexes:
            cursor.execute(f"DROP INDEX IF EXISTS {idx}")
        conn.commit()
    
    # ZIP to County mapping
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS zip_to_county (
            zip TEXT NOT NULL,
            county TEXT NOT NULL,
            city TEXT,
            state_abbr TEXT NOT NULL,
            res_ratio REAL NOT NULL,
            PRIMARY KEY (zip, county)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zip ON zip_to_county(zip)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zip_res_ratio ON zip_to_county(zip, res_ratio DESC)")
    
    # County reference (FIPS codes to names)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS county_reference (
            county_code TEXT PRIMARY KEY,
            state_abbr TEXT NOT NULL,
            state_fips TEXT NOT NULL,
            county_fips TEXT NOT NULL,
            county_name TEXT NOT NULL,
            class_code TEXT
        )
    """)
    
    # County to Locality mapping
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS county_locality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mac TEXT,
            locality_number TEXT NOT NULL,
            state_label TEXT NOT NULL,
            locality_name TEXT,
            counties TEXT NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_state_locality ON county_locality(state_label, locality_number)")
    
    # GPCI multipliers
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gpci (
            state_abbr TEXT NOT NULL,
            locality_number TEXT NOT NULL,
            pw_gpci REAL NOT NULL,
            pe_gpci REAL NOT NULL,
            mp_gpci REAL NOT NULL,
            PRIMARY KEY (state_abbr, locality_number)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gpci_state_locality ON gpci(state_abbr, locality_number)")
    
    # RVU values
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rvu (
            hcpcs TEXT PRIMARY KEY,
            pw_rvu REAL NOT NULL,
            pe_rvu REAL NOT NULL,
            mp_rvu REAL NOT NULL
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hcpcs ON rvu(hcpcs)")
    
    conn.commit()
    print("Database schema created successfully.")


def migrate_zip_to_county(conn: sqlite3.Connection) -> None:
    """Migrate ZIP to County data from CSV."""
    print("Migrating ZIP to County data...")
    
    df = pd.read_csv(
        FILES["zip_to_county"],
        usecols=["ZIP", "COUNTY", "USPS_ZIP_PREF_CITY", "USPS_ZIP_PREF_STATE", "RES_RATIO"],
        dtype={
            "ZIP": str,
            "COUNTY": str,
            "USPS_ZIP_PREF_CITY": str,
            "USPS_ZIP_PREF_STATE": str,
            "RES_RATIO": float,
        },
    )
    df["ZIP"] = df["ZIP"].str.zfill(5)
    df["COUNTY"] = df["COUNTY"].str.zfill(5)
    df["RES_RATIO"] = pd.to_numeric(df["RES_RATIO"], errors="coerce")
    df = df.dropna(subset=["RES_RATIO"])
    
    # Rename columns to match database schema (lowercase)
    df = df.rename(columns={
        "ZIP": "zip",
        "COUNTY": "county",
        "USPS_ZIP_PREF_CITY": "city",
        "USPS_ZIP_PREF_STATE": "state_abbr",
        "RES_RATIO": "res_ratio",
    })
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM zip_to_county")
    
    # Insert row by row to ensure proper data types
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO zip_to_county (zip, county, city, state_abbr, res_ratio)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(row["zip"]),
                str(row["county"]),
                str(row["city"]) if pd.notna(row["city"]) else None,
                str(row["state_abbr"]),
                float(row["res_ratio"]),
            ),
        )
    
    conn.commit()
    print(f"  Migrated {len(df)} ZIP to County records.")


def migrate_county_reference(conn: sqlite3.Connection) -> None:
    """Migrate county reference data from text file."""
    print("Migrating County Reference data...")
    
    _ensure_county_reference_file()
    df = pd.read_csv(
        FILES["county_reference"],
        header=None,
        names=["state_abbr", "state_fips", "county_fips", "county_name", "class"],
        dtype=str,
    )
    df["county_code"] = df["state_fips"].str.zfill(2) + df["county_fips"].str.zfill(3)
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM county_reference")
    
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO county_reference 
            (county_code, state_abbr, state_fips, county_fips, county_name, class_code)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row["county_code"],
                row["state_abbr"],
                row["state_fips"],
                row["county_fips"],
                row["county_name"],
                row["class"],
            ),
        )
    
    conn.commit()
    print(f"  Migrated {len(df)} County Reference records.")


def migrate_county_locality(conn: sqlite3.Connection) -> None:
    """Migrate county to locality mapping from CSV."""
    print("Migrating County to Locality data...")
    
    df = pd.read_csv(FILES["county_locality"], skiprows=2, dtype=str)
    df = df.rename(
        columns={
            "Medicare Adminstrative Contractor": "MAC",
            "Locality Number": "LocalityNumber",
            "State ": "StateLabel",
            "Fee Schedule Area ": "LocalityName",
            "Counties": "Counties",
        }
    )
    df = df.dropna(how="all")
    df["StateLabel"] = df["StateLabel"].ffill()
    df = df.dropna(subset=["StateLabel", "LocalityNumber", "Counties"])
    df["LocalityName"] = df["LocalityName"].fillna("")
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM county_locality")
    
    for _, row in df.iterrows():
        try:
            locality_number = _normalize_locality_number(row["LocalityNumber"])
        except ValueError:
            continue
        
        cursor.execute(
            """
            INSERT INTO county_locality 
            (mac, locality_number, state_label, locality_name, counties)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(row["MAC"]).strip() if pd.notna(row["MAC"]) else None,
                locality_number,
                str(row["StateLabel"]).strip(),
                str(row["LocalityName"]).strip(),
                str(row["Counties"]).strip(),
            ),
        )
    
    conn.commit()
    print(f"  Migrated {len(df)} County to Locality records.")


def migrate_gpci(conn: sqlite3.Connection) -> None:
    """Migrate GPCI data from CSV."""
    print("Migrating GPCI data...")
    
    df = pd.read_csv(FILES["gpci"], skiprows=2, dtype=str)
    df = df.rename(
        columns={
            "Medicare Administrative Contractor (MAC)": "MAC",
            "Locality Number": "LocalityNumber",
            "Locality Name": "LocalityName",
            "2025 PW GPCI (with 1.0 Floor)": "PW_GPCI",
            "2025 PE GPCI": "PE_GPCI",
            "2025 MP GPCI": "MP_GPCI",
        }
    )
    df = df[df["LocalityNumber"].notna() & df["State"].notna()]
    df["LocalityNumber"] = df["LocalityNumber"].apply(_normalize_locality_number)
    
    for column in ("PW_GPCI", "PE_GPCI", "MP_GPCI"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["PW_GPCI", "PE_GPCI", "MP_GPCI"])
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM gpci")
    
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO gpci (state_abbr, locality_number, pw_gpci, pe_gpci, mp_gpci)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(row["State"]).strip(),
                str(row["LocalityNumber"]),
                float(row["PW_GPCI"]),
                float(row["PE_GPCI"]),
                float(row["MP_GPCI"]),
            ),
        )
    
    conn.commit()
    print(f"  Migrated {len(df)} GPCI records.")


def migrate_rvu(conn: sqlite3.Connection) -> None:
    """Migrate RVU data from CSV."""
    print("Migrating RVU data...")
    
    header_row = _find_header_row(FILES["rvu"], "HCPCS")
    df = pd.read_csv(FILES["rvu"], skiprows=header_row, dtype=str)
    df.columns = [col.strip() for col in df.columns]
    df = df.rename(
        columns={
            "RVU": "PW_RVU",
            "PE RVU": "PE_RVU",
            "RVU.1": "MP_RVU",
        }
    )
    df = df[df["HCPCS"].notna()]
    df["HCPCS"] = df["HCPCS"].astype(str).str.strip()
    df = df[df["HCPCS"] != ""]
    df = df[df["MOD"].isna() | (df["MOD"].astype(str).str.strip() == "")]
    
    for column in ["PW_RVU", "PE_RVU", "MP_RVU"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["PW_RVU", "PE_RVU", "MP_RVU"])
    df = df.drop_duplicates(subset=["HCPCS"], keep="first")
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM rvu")
    
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO rvu (hcpcs, pw_rvu, pe_rvu, mp_rvu)
            VALUES (?, ?, ?, ?)
            """,
            (
                str(row["HCPCS"]),
                float(row["PW_RVU"]),
                float(row["PE_RVU"]),
                float(row["MP_RVU"]),
            ),
        )
    
    conn.commit()
    print(f"  Migrated {len(df)} RVU records.")


def main():
    """Main migration function."""
    print("=" * 60)
    print("Medicare Database Migration")
    print("=" * 60)
    
    # Create connection and schema
    drop_existing = DB_PATH.exists()
    if drop_existing:
        print(f"Database exists. Will drop and recreate tables.")
    
    conn = get_db_connection()
    try:
        create_schema(conn, drop_existing=drop_existing)
        print()
        
        # Migrate all data
        migrate_zip_to_county(conn)
        migrate_county_reference(conn)
        migrate_county_locality(conn)
        migrate_gpci(conn)
        migrate_rvu(conn)
        
        print()
        print("=" * 60)
        print("Migration completed successfully!")
        print("=" * 60)
        
        # Show statistics
        cursor = conn.cursor()
        tables = ["zip_to_county", "county_reference", "county_locality", "gpci", "rvu"]
        print("\nDatabase Statistics:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table}: {count:,} records")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()

