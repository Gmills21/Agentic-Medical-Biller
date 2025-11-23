"""Medicare price calculator for 2025 physician fee schedule."""

from __future__ import annotations

import difflib
import re
import sqlite3
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pandas as pd

CONVERSION_FACTOR = 32.35

BASE_DIR = Path(__file__).resolve().parent
CSV_DIR = BASE_DIR / "Medicare CSVS"
DB_PATH = BASE_DIR / "medicare.db"


def get_db_connection() -> sqlite3.Connection:
    """Connect to the medicare.db SQLite database and return the connection object.
    
    Returns:
        sqlite3.Connection: Database connection object
        
    Note:
        The connection uses row_factory=sqlite3.Row for easier dictionary-like access.
    """
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row  # Enable dictionary-like access to rows
    return conn
FILES = {
    "zip_to_county": CSV_DIR / "Zip-County.csv",
    "county_locality": CSV_DIR / "25LOCCO1.csv",
    "gpci": CSV_DIR / "GPCI2025.csv",
    "rvu": CSV_DIR / "PPRRVU25_JAN1.csv",
    "county_reference": BASE_DIR / "national_county.txt",
}
COUNTY_REFERENCE_URL = (
    "https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt"
)

STATE_ABBR_TO_NAME = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AZ": "ARIZONA",
    "AR": "ARKANSAS",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DC": "DISTRICT OF COLUMBIA",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "IA": "IOWA",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "MA": "MASSACHUSETTS",
    "MD": "MARYLAND",
    "ME": "MAINE",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MO": "MISSOURI",
    "MS": "MISSISSIPPI",
    "MT": "MONTANA",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "NE": "NEBRASKA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NV": "NEVADA",
    "NY": "NEW YORK",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "PR": "PUERTO RICO",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VA": "VIRGINIA",
    "VI": "VIRGIN ISLANDS",
    "VT": "VERMONT",
    "WA": "WASHINGTON",
    "WI": "WISCONSIN",
    "WV": "WEST VIRGINIA",
    "WY": "WYOMING",
    "GU": "GUAM",
    "MP": "NORTHERN MARIANA ISLANDS",
}
STATE_NAME_TO_ABBR = {v: k for k, v in STATE_ABBR_TO_NAME.items()}


@dataclass(frozen=True)
class LocalityEntry:
    state_name: str
    state_abbr: str
    locality_number: str
    locality_name: str
    mac: str


def _ensure_county_reference_file() -> None:
    path = FILES["county_reference"]
    if path.exists():
        return
    try:
        with urllib.request.urlopen(COUNTY_REFERENCE_URL) as response:
            data = response.read()
        path.write_bytes(data)
    except Exception as exc:  # pragma: no cover - network failure path
        raise RuntimeError(
            "Unable to download county reference data required for mappings."
        ) from exc


def _normalize_locality_number(value: str) -> str:
    value = str(value).strip()
    if not value or value.lower() == "nan":
        raise ValueError("Missing locality number.")
    return str(int(float(value)))


def _normalize_state_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().upper())


def _normalize_county_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    value = name.upper()
    value = value.replace("SAINTE", "STE")
    value = value.replace("SAINT", "ST")
    value = value.replace("ST.", "ST")
    value = value.replace("CNTY", "COUNTY")
    value = value.replace("'", "")
    value = value.replace(".", " ")
    value = value.replace("-", " ")
    value = value.replace("\u2019", "")
    suffixes = [
        " COUNTY EQUIVALENTS",
        " COUNTY",
        " COUNTIES",
        " PARISH",
        " PARISHES",
        " MUNICIPIO",
        " MUNICIPIOS",
        " MUNICIPALITY",
        " CITY",
        " BOROUGH",
        " CENSUS AREA",
        " ISLANDS",
        " ISLAND",
    ]
    for suffix in suffixes:
        if value.endswith(suffix):
            value = value[: -len(suffix)]
    value = re.sub(r"[^A-Z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value.replace(" ", "")


def _classify_county_scope(text: str) -> str:
    upper = text.upper()
    if "ALL OTHER COUNTIES" in upper:
        return "rest"
    if "ALL COUNTY EQUIVALENTS" in upper:
        return "all"
    if upper.startswith("ALL COUNTIES") and "EXCEPT" in upper:
        return "rest"
    if upper.startswith("ALL COUNTIES"):
        return "all"
    if upper.startswith("ALL COUNTY"):
        return "all"
    return "specific"


def _split_segment_states(segment_state_text: str) -> List[str]:
    names = []
    for token in re.split(r"[\/,&]", segment_state_text):
        candidate = token.strip().upper()
        if not candidate:
            continue
        candidate = candidate.replace("  ", " ")
        if candidate.endswith(" IN"):
            candidate = candidate[:-3].strip()
        names.append(candidate)
    return names


def _split_county_list(text: str) -> List[str]:
    text = text.replace("\n", " ")
    text = text.replace("/", ",")
    text = text.replace("&", ",")
    text = re.sub(r"\bAND\b", ",", text, flags=re.IGNORECASE)
    tokens = []
    for part in text.split(","):
        part = part.strip()
        if part:
            tokens.append(part)
    return tokens


def _parse_county_segments(counties_text: str, default_states: Sequence[str]) -> List[Tuple[List[str], List[str]]]:
    sanitized = counties_text.replace("\n", " ")
    raw_segments = [seg.strip() for seg in sanitized.split(";") if seg.strip()]
    if not raw_segments:
        raw_segments = [sanitized.strip()]
    result: List[Tuple[List[str], List[str]]] = []
    for segment in raw_segments:
        working_segment = segment
        segment_states = list(default_states)
        match = re.search(r"\bIN ([A-Z .'/&-]+)$", working_segment.upper())
        if match:
            state_text = working_segment[match.start(1) : match.end(1)]
            segment_states = _split_segment_states(state_text)
            working_segment = working_segment[: match.start()].strip()
        counties = _split_county_list(working_segment)
        if not counties:
            continue
        if not segment_states:
            segment_states = list(default_states)
        result.append((segment_states, counties))
    return result


class LocalityLookup:
    """Resolve (state, county) to locality entry."""

    def __init__(self, df: pd.DataFrame):
        self.state_specific: Dict[str, Dict[str, LocalityEntry]] = defaultdict(dict)
        self.state_rest: Dict[str, LocalityEntry] = {}
        self.state_all: Dict[str, LocalityEntry] = {}
        self._build(df)

    def _build(self, df: pd.DataFrame) -> None:
        for row in df.itertuples(index=False):
            try:
                locality_number = _normalize_locality_number(row.LocalityNumber)
            except ValueError:
                continue
            locality_name = (row.LocalityName or "").strip()
            mac = (row.MAC or "").strip()
            state_field = (row.StateLabel or "").strip()
            if not state_field:
                continue
            row_states = [
                _normalize_state_name(part)
                for part in state_field.split("/")
                if part and part.strip()
            ]
            counties_text = (row.Counties or "").strip()
            if not counties_text:
                continue
            scope = _classify_county_scope(counties_text)
            if scope in {"rest", "all"}:
                for state_name in row_states:
                    abbr = STATE_NAME_TO_ABBR.get(state_name)
                    if not abbr:
                        continue
                    entry = LocalityEntry(
                        state_name=state_name,
                        state_abbr=abbr,
                        locality_number=locality_number,
                        locality_name=locality_name,
                        mac=mac,
                    )
                    target = self.state_rest if scope == "rest" else self.state_all
                    target[state_name] = entry
                continue
            segments = _parse_county_segments(counties_text, row_states)
            for segment_states, counties in segments:
                for seg_state in segment_states:
                    state_name = _normalize_state_name(seg_state)
                    abbr = STATE_NAME_TO_ABBR.get(state_name)
                    if not abbr:
                        continue
                    entry = LocalityEntry(
                        state_name=state_name,
                        state_abbr=abbr,
                        locality_number=locality_number,
                        locality_name=locality_name,
                        mac=mac,
                    )
                    for county in counties:
                        county_key = _normalize_county_name(county)
                        if not county_key:
                            continue
                        self.state_specific[state_name][county_key] = entry

    def find(self, state_abbr: str, county_name: str) -> LocalityEntry:
        state_name = STATE_ABBR_TO_NAME.get(state_abbr)
        if not state_name:
            raise ValueError(f"Unsupported state abbreviation: {state_abbr}")
        county_key = _normalize_county_name(county_name)
        specific = self.state_specific.get(state_name, {})
        if county_key in specific:
            return specific[county_key]
        # Fuzzy fallback for minor spelling differences (e.g., typos in source csv)
        best_entry = self._fuzzy_match(state_name, county_key)
        if best_entry:
            return best_entry
        if state_name in self.state_rest:
            return self.state_rest[state_name]
        if state_name in self.state_all:
            return self.state_all[state_name]
        raise ValueError(f"No locality mapping found for {county_name} in {state_abbr}.")

    def _fuzzy_match(self, state_name: str, target: str) -> LocalityEntry | None:
        candidates = self.state_specific.get(state_name, {})
        best_score = 0.0
        best_entry: LocalityEntry | None = None
        for candidate_key, entry in candidates.items():
            score = difflib.SequenceMatcher(None, candidate_key, target).ratio()
            if score > best_score:
                best_score = score
                best_entry = entry
        if best_score >= 0.9:
            return best_entry
        return None


def _load_zip_to_county() -> pd.DataFrame:
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
    return df


def _load_county_reference() -> Dict[str, str]:
    _ensure_county_reference_file()
    df = pd.read_csv(
        FILES["county_reference"],
        header=None,
        names=["state_abbr", "state_fips", "county_fips", "county_name", "class"],
        dtype=str,
    )
    df["county_code"] = df["state_fips"].str.zfill(2) + df["county_fips"].str.zfill(3)
    return df.set_index("county_code")["county_name"].to_dict()


def _load_county_locality() -> pd.DataFrame:
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
    return df


def _load_gpci_lookup() -> Dict[Tuple[str, str], Dict[str, float]]:
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
    lookup: Dict[Tuple[str, str], Dict[str, float]] = {}
    for row in df.itertuples(index=False):
        key = (row.State.strip(), row.LocalityNumber)
        lookup[key] = {
            "PW_GPCI": float(row.PW_GPCI),
            "PE_GPCI": float(row.PE_GPCI),
            "MP_GPCI": float(row.MP_GPCI),
        }
    return lookup


def _find_header_row(path: Path, sentinel: str) -> int:
    with path.open("r", encoding="utf-8-sig") as handle:
        for idx, line in enumerate(handle):
            if line.startswith(sentinel):
                return idx
    raise RuntimeError(f"Could not find header row starting with '{sentinel}'.")


def _load_rvu_lookup() -> Dict[str, Dict[str, float]]:
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
    return df.set_index("HCPCS")[["PW_RVU", "PE_RVU", "MP_RVU"]].to_dict("index")


# Check if database exists and has tables, otherwise use CSV-based lookups
USE_DATABASE = False
if DB_PATH.exists():
    try:
        conn = get_db_connection()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='zip_to_county'"
        )
        USE_DATABASE = cursor.fetchone() is not None
        conn.close()
    except Exception:
        USE_DATABASE = False

if USE_DATABASE:
    # SQL-based lookups (more scalable)
    pass  # Functions defined below use SQL queries
else:
    # CSV-based lookups (fallback for backward compatibility)
    ZIP_TO_COUNTY = _load_zip_to_county()
    ZIP_TO_COUNTY_GROUPED = ZIP_TO_COUNTY.groupby("ZIP")
    COUNTY_CODE_TO_NAME = _load_county_reference()
    LOCALITY_LOOKUP = LocalityLookup(_load_county_locality())
    GPCI_LOOKUP = _load_gpci_lookup()
    RVU_LOOKUP = _load_rvu_lookup()


def _select_county(zip_code: str) -> Tuple[str, str]:
    """Select county for ZIP code using highest RES_RATIO."""
    if USE_DATABASE:
        conn = get_db_connection()
        try:
            cursor = conn.execute(
                """
                SELECT county, state_abbr 
                FROM zip_to_county 
                WHERE zip = ? 
                ORDER BY res_ratio DESC 
                LIMIT 1
                """,
                (zip_code,),
            )
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"ZIP code {zip_code} not found in ZIP-County data.")
            result = (row["county"], row["state_abbr"])
            return result
        finally:
            conn.close()
    else:
        # CSV-based fallback
        try:
            rows = ZIP_TO_COUNTY_GROUPED.get_group(zip_code)
        except KeyError:
            raise ValueError(f"ZIP code {zip_code} not found in ZIP-County data.")
        best_row = rows.sort_values("RES_RATIO", ascending=False).iloc[0]
        return best_row["COUNTY"], best_row["USPS_ZIP_PREF_STATE"]


def _get_county_name(county_code: str) -> str:
    """Get county name from county code."""
    if USE_DATABASE:
        conn = get_db_connection()
        try:
            cursor = conn.execute(
                "SELECT county_name FROM county_reference WHERE county_code = ?",
                (county_code,),
            )
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"No county reference found for code {county_code}.")
            result = row["county_name"]
            return result
        finally:
            conn.close()
    else:
        # CSV-based fallback
        county_name = COUNTY_CODE_TO_NAME.get(county_code)
        if not county_name:
            raise ValueError(f"No county reference found for code {county_code}.")
        return county_name


def _resolve_locality(county_code: str, state_abbr: str) -> LocalityEntry:
    """Resolve county to locality using SQL or CSV lookup."""
    county_name = _get_county_name(county_code)
    
    if USE_DATABASE:
        # SQL-based locality lookup
        # First, try to find exact match in county_locality table
        # This requires parsing the counties field, which is complex
        # For now, use the CSV-based LocalityLookup class with database-loaded data
        conn = get_db_connection()
        try:
            # Load locality data from database into LocalityLookup
            cursor = conn.execute(
                "SELECT mac, locality_number, state_label, locality_name, counties FROM county_locality"
            )
            rows = cursor.fetchall()
            
            # Convert to DataFrame for LocalityLookup
            import pandas as pd
            data = {
                "MAC": [r["mac"] for r in rows],
                "LocalityNumber": [r["locality_number"] for r in rows],
                "StateLabel": [r["state_label"] for r in rows],
                "LocalityName": [r["locality_name"] for r in rows],
                "Counties": [r["counties"] for r in rows],
            }
            df = pd.DataFrame(data)
            lookup = LocalityLookup(df)
            return lookup.find(state_abbr, county_name)
        finally:
            conn.close()
    else:
        # CSV-based fallback
        return LOCALITY_LOOKUP.find(state_abbr, county_name)


def _fetch_gpci(entry: LocalityEntry) -> Dict[str, float]:
    """Fetch GPCI multipliers for locality."""
    if USE_DATABASE:
        conn = get_db_connection()
        try:
            cursor = conn.execute(
                """
                SELECT pw_gpci, pe_gpci, mp_gpci 
                FROM gpci 
                WHERE state_abbr = ? AND locality_number = ?
                """,
                (entry.state_abbr, entry.locality_number),
            )
            row = cursor.fetchone()
            if not row:
                raise ValueError(
                    f"GPCI multipliers missing for state {entry.state_abbr} "
                    f"locality {entry.locality_number}."
                )
            result = {
                "PW_GPCI": float(row["pw_gpci"]),
                "PE_GPCI": float(row["pe_gpci"]),
                "MP_GPCI": float(row["mp_gpci"]),
            }
            return result
        finally:
            conn.close()
    else:
        # CSV-based fallback
        key = (entry.state_abbr, entry.locality_number)
        gpci = GPCI_LOOKUP.get(key)
        if not gpci:
            raise ValueError(
                f"GPCI multipliers missing for state {entry.state_abbr} "
                f"locality {entry.locality_number}."
            )
        return gpci


def _fetch_rvu(cpt_code: str) -> Dict[str, float]:
    """Fetch RVU values for CPT code."""
    if USE_DATABASE:
        conn = get_db_connection()
        try:
            cursor = conn.execute(
                "SELECT pw_rvu, pe_rvu, mp_rvu FROM rvu WHERE hcpcs = ?",
                (cpt_code,),
            )
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"CPT/HCPCS code {cpt_code} not found in RVU file.")
            result = {
                "PW_RVU": float(row["pw_rvu"]),
                "PE_RVU": float(row["pe_rvu"]),
                "MP_RVU": float(row["mp_rvu"]),
            }
            return result
        finally:
            conn.close()
    else:
        # CSV-based fallback
        rvu = RVU_LOOKUP.get(cpt_code)
        if not rvu:
            raise ValueError(f"CPT/HCPCS code {cpt_code} not found in RVU file.")
        return rvu


def get_medicare_price(cpt_code: str, zip_code: str) -> float:
    """Return Medicare price for the given CPT code and ZIP code."""
    if not cpt_code:
        raise ValueError("cpt_code is required.")
    if not zip_code:
        raise ValueError("zip_code is required.")
    normalized_zip = str(zip_code).strip()
    if not normalized_zip.isdigit():
        raise ValueError("zip_code must be numeric.")
    normalized_zip = normalized_zip.zfill(5)
    normalized_cpt = str(cpt_code).strip().upper()
    county_code, state_abbr = _select_county(normalized_zip)
    locality = _resolve_locality(county_code, state_abbr)
    gpci = _fetch_gpci(locality)
    rvu = _fetch_rvu(normalized_cpt)
    final_price = (
        (rvu["PW_RVU"] * gpci["PW_GPCI"])
        + (rvu["PE_RVU"] * gpci["PE_GPCI"])
        + (rvu["MP_RVU"] * gpci["MP_GPCI"])
    ) * CONVERSION_FACTOR
    return round(final_price, 2)


if __name__ == "__main__":
    example_price = get_medicare_price("99285", "00601")
    print(
        f"The Medicare price for 99285 in ZIP 00601 is: ${example_price}"
    )

