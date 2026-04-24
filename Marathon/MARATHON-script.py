import csv
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

TABLE_NAME = "MARATHON"
COLUMNS = ["Place", "Time", "Pace", "GroupPlace", "`Group`", "Age", "Sex", "BIBNumber", "FirstName", "LastName", "Town", "State"]
INPUT_CSV = BASE_DIR / "marathon.csv"
OUTPUT_SQL = BASE_DIR / "Marathon-build-MARATHON.sql"

FIELD_SPECS = [
    ("Place", "Place", "int"),
    ("Time", "Time", "time"),
    ("Pace", "Pace", "text"),
    ("GroupPlace", "GroupPlace", "int"),
    ("`Group`", "Group", "text"),
    ("Age", "Age", "int"),
    ("Sex", "Sex", "text"),
    ("BIBNumber", "BIBNumber", "int"),
    ("FirstName", "FirstName", "text"),
    ("LastName", "LastName", "text"),
    ("Town", "Town", "text"),
    ("State", "State", "text")
]

CSV_HEADER_ALIASES = {
    "LastName": "LasName",
}

DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%Y/%m/%d",
    "%d-%b-%Y",
)


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


def normalize_date(raw_value: str) -> str:
    cleaned = raw_value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError(f"Unsupported date format: {raw_value}")


def normalize_time(raw_value: str) -> str:
    cleaned = raw_value.strip().strip("'\"")
    parts = cleaned.split(":")

    if len(parts) == 3:
        hours_str, minutes_str, seconds_str = parts
    elif len(parts) == 2:
        hours_str = "0"
        minutes_str, seconds_str = parts
    else:
        raise ValueError(f"Unsupported time format: {raw_value}")

    if not (hours_str.isdigit() and minutes_str.isdigit() and seconds_str.isdigit()):
        raise ValueError(f"Unsupported time format: {raw_value}")

    hours = int(hours_str)
    minutes = int(minutes_str)
    seconds = int(seconds_str)

    if minutes >= 60 or seconds >= 60:
        raise ValueError(f"Invalid time value: {raw_value}")

    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def sql_value(raw_value: str | None, kind: str) -> str:
    if raw_value is None:
        return "NULL"

    value = raw_value.strip().strip("'\"")
    if value == "":
        return "NULL"

    if kind == "int":
        return str(int(value))

    if kind == "date":
        return sql_quote(normalize_date(value))

    if kind == "time":
        return sql_quote(normalize_time(value))

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
