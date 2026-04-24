import csv
from datetime import datetime
from pathlib import Path

TABLE_NAME = "RECEIPTS"
COLUMNS = ["ReceiptNumber", "Date", "CustomerId"]
INPUT_CSV = Path("RECEIPTS.csv")
OUTPUT_SQL = Path("Bakery-build-RECEIPTS.sql")

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


def main() -> None:
    with INPUT_CSV.open("r", newline="", encoding="utf-8-sig") as infile, OUTPUT_SQL.open(
        "w", encoding="utf-8"
    ) as outfile:
        reader = csv.DictReader(infile)
        for row in reader:
            receipt_number = int(row["ReceiptNumber"])
            receipt_date = sql_quote(normalize_date(row["Date"]))
            customer_id = int(row["CustomerId"])
            outfile.write(
                f"INSERT INTO {TABLE_NAME} ({', '.join(COLUMNS)}) VALUES ({receipt_number}, {receipt_date}, {customer_id});\n"
            )

    print(f"Wrote {OUTPUT_SQL}")


if __name__ == "__main__":
    main()
