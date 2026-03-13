"""
Parse HG Insights Excel/CSV uploads.

Expected columns (case-insensitive matching):
  Company Name | Domain | Technology | Source
"""

from typing import Dict, List, Union
from pathlib import Path
from dataclasses import dataclass
import openpyxl


@dataclass
class HGRecord:
    row_number: int
    company_name: str
    domain: str
    technology: str
    source: str


class ParseError(Exception):
    pass


# Maps expected logical column names to possible header variations
COLUMN_ALIASES = {
    "company_name": {"company name", "company", "name", "organization", "org"},
    "domain": {"domain", "website", "url", "website url", "company domain", "web"},
    "technology": {"technology", "tech", "tech stack", "product", "technology name"},
    "source": {"source", "data source"},
}


def _find_columns(headers: List[str]) -> Dict[str, int]:
    """
    Match spreadsheet headers to our expected columns.
    Returns a dict like {"company_name": 0, "domain": 1, ...}.
    """
    lower_headers = [h.strip().lower() if h else "" for h in headers]
    mapping: Dict[str, int] = {}

    for field, aliases in COLUMN_ALIASES.items():
        for idx, h in enumerate(lower_headers):
            if h in aliases:
                mapping[field] = idx
                break

    missing = set(COLUMN_ALIASES.keys()) - set(mapping.keys())
    # Source is optional — default to "HG Insights"
    missing.discard("source")

    if missing:
        raise ParseError(
            f"Could not find required columns: {', '.join(missing)}. "
            f"Found headers: {headers}"
        )
    return mapping


def parse_excel(file_path: Union[str, Path]) -> List[HGRecord]:
    """Parse an Excel (.xlsx) file and return a list of HGRecords."""
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        raise ParseError("The uploaded file is empty.")

    # First row = headers
    headers = [str(cell) if cell else "" for cell in rows[0]]
    col_map = _find_columns(headers)

    records: List[HGRecord] = []
    for i, row in enumerate(rows[1:], start=2):
        cells = list(row)

        company = str(cells[col_map["company_name"]]).strip() if col_map.get("company_name") is not None and cells[col_map["company_name"]] else ""
        domain = str(cells[col_map["domain"]]).strip() if col_map.get("domain") is not None and cells[col_map["domain"]] else ""
        technology = str(cells[col_map["technology"]]).strip() if col_map.get("technology") is not None and cells[col_map["technology"]] else ""
        source = ""
        if "source" in col_map and cells[col_map["source"]]:
            source = str(cells[col_map["source"]]).strip()
        else:
            source = "HG Insights"

        # Skip completely empty rows
        if not domain and not company:
            continue

        records.append(HGRecord(
            row_number=i,
            company_name=company,
            domain=domain,
            technology=technology,
            source=source,
        ))

    return records


def parse_csv(file_path: Union[str, Path]) -> List[HGRecord]:
    """Parse a CSV file and return a list of HGRecords."""
    import csv

    # Try multiple encodings — HG Insights exports may use various formats
    rows = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(file_path, newline="", encoding=encoding) as f:
                reader = csv.reader(f)
                rows = list(reader)
            break
        except (UnicodeDecodeError, UnicodeError):
            continue

    if rows is None:
        raise ParseError("Could not decode the CSV file. Please save it as UTF-8 and try again.")

    if not rows:
        raise ParseError("The uploaded file is empty.")

    headers = rows[0]
    col_map = _find_columns(headers)

    records: List[HGRecord] = []
    for i, row in enumerate(rows[1:], start=2):
        if not row:
            continue

        company = row[col_map["company_name"]].strip() if col_map.get("company_name") is not None and len(row) > col_map["company_name"] else ""
        domain = row[col_map["domain"]].strip() if col_map.get("domain") is not None and len(row) > col_map["domain"] else ""
        technology = row[col_map["technology"]].strip() if col_map.get("technology") is not None and len(row) > col_map["technology"] else ""
        source = ""
        if "source" in col_map and len(row) > col_map["source"]:
            source = row[col_map["source"]].strip()
        else:
            source = "HG Insights"

        if not domain and not company:
            continue

        records.append(HGRecord(
            row_number=i,
            company_name=company,
            domain=domain,
            technology=technology,
            source=source,
        ))

    return records


def _detect_format(file_path: Path) -> str:
    """Detect actual file format from magic bytes, fallback to extension."""
    with open(file_path, "rb") as f:
        header = f.read(4)
    # XLSX/ZIP files start with PK\x03\x04
    if header[:4] == b"PK\x03\x04":
        return "xlsx"
    # XLS (old Excel) starts with D0 CF 11 E0
    if header[:4] == b"\xd0\xcf\x11\xe0":
        return "xls"
    # Otherwise treat as CSV
    return "csv"


def parse_file(file_path: Union[str, Path]) -> List[HGRecord]:
    """Auto-detect file type from content and parse."""
    path = Path(file_path)
    fmt = _detect_format(path)
    if fmt in ("xlsx", "xls"):
        return parse_excel(path)
    else:
        return parse_csv(path)
