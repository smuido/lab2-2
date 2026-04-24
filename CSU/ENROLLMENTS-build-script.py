import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

TABLE_NAME = "ENROLLMENTS"
COLUMNS = ["Campus", "Year", "TotalEnrollment_AY", "FTE_AY"]
INPUT_CSV = BASE_DIR / "enrollments.csv"
OUTPUT_SQL = BASE_DIR / "CSU-build-ENROLLMENTS.sql"

FIELD_SPECS = [
    ("Campus", "Campus", "int"),
    ("Year", "Year", "int"),
    ("TotalEnrollment_AY", "TotalEnrollment_AY", "int"),
    ("FTE_AY", "FTE_AY", "int")
]

CSV_HEADER_ALIASES: dict[str, str] = {}

def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def normalize_header(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def get_csv_value(row: dict[str, str], csv_column: str) -> str | None:
    normalized_target = normalize_header(csv_column)
    for header, value in row.items():
        if normalize_header(header) == normalized_target:
            return value

    alias = CSV_HEADER_ALIASES.get(csv_column)
    if alias is not None:
        normalized_alias = normalize_header(alias)
        for header, value in row.items():
            if normalize_header(header) == normalized_alias:
                return value

    return None

def sql_value(raw_value: str | None, kind: str) -> str:
    if raw_value is None:
        return "NULL"

    value = raw_value.strip().strip("'\"")
    if value == "":
        return "NULL"

    if kind == "int":
        return str(int(value))

    return sql_quote(value)

def main() -> None:
    with INPUT_CSV.open("r", newline="", encoding="utf-8-sig") as infile, OUTPUT_SQL.open(
        "w", encoding="utf-8"
    ) as outfile:
        reader = csv.DictReader(infile)
        for row in reader:
            values = [
                sql_value(get_csv_value(row, csv_column), kind)
                for _, csv_column, kind in FIELD_SPECS
            ]
            outfile.write(
                f"INSERT INTO {TABLE_NAME} ({', '.join(COLUMNS)}) VALUES ({', '.join(values)});\n"
            )

    print(f"Wrote {OUTPUT_SQL}")

if __name__ == "__main__":
    main()