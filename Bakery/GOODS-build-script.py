import csv
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

TABLE_NAME = "GOODS"
COLUMNS = ["Id", "Flavor", "Food", "Price"]
INPUT_CSV = BASE_DIR / "goods.csv"
OUTPUT_SQL = BASE_DIR / "Bakery-build-GOODS.sql"

FIELD_SPECS = [
    ("Id", "Id", "int"),
    ("Flavor", "Flavor", "varchar(50)"),
    ("Food", "Food", "varchar(50)"),
    ("Price", "Price", "decimal(6,2)"),
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

    if kind == "decimal(6,2)":
        return str(float(value))

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
