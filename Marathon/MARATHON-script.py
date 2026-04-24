import csv
from datetime import datetime
from pathlib import Path

TABLE_NAME = "MARATHON"
COLUMNS = ["Place", "Time", "Pace", "GroupPlace", "'Group'", "Age", "Sex", "BIBNumber", "FirstName", "LastName", "Town", "State"]
INPUT_CSV = Path("MARATHON.csv")
OUTPUT_SQL = Path("Marathon-build-MARATHON.sql")

FIELD_SPECS = [
    ("Place", "Place", "int"),
    ("Time", "Time", "text"),
    ("Pace", "Pace", "text"),
    ("GroupPlace", "GroupPlace", "int"),
    ("'Group'", "Group", "text"),
    ("Age", "Age", "int"),
    ("Sex", "Sex", "text"),
    ("BIBNumber", "BIBNumber", "int"),
    ("FirstName", "FirstName", "text"),
    ("LastName", "LastName", "text"),
    ("Town", "Town", "text"),
    ("State", "State", "text"),
]

DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%Y/%m/%d",
    "%d-%b-%Y",
)


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def normalize_date(raw_value: str) -> str:
    cleaned = raw_value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError(f"Unsupported date format: {raw_value}")


def sql_value(raw_value: str | None, kind: str) -> str:
    if raw_value is None:
        return "NULL"

    value = raw_value.strip()
    if value == "":
        return "NULL"

    if kind == "int":
        return str(int(value))

    if kind == "date":
        return sql_quote(normalize_date(value))

    return sql_quote(value)


def main() -> None:
    with INPUT_CSV.open("r", newline="", encoding="utf-8-sig") as infile, OUTPUT_SQL.open(
        "w", encoding="utf-8"
    ) as outfile:
        reader = csv.DictReader(infile)
        for row in reader:
            values = [
                sql_value(row.get(csv_column), kind)
                for _, csv_column, kind in FIELD_SPECS
            ]
            outfile.write(
                f"INSERT INTO {TABLE_NAME} ({', '.join(COLUMNS)}) VALUES ({', '.join(values)});\n"
            )

    print(f"Wrote {OUTPUT_SQL}")


if __name__ == "__main__":
    main()
