import csv
from pathlib import Path

TABLE_NAME = "ITEMS"
COLUMNS = ["Receipt", "Ordinal", "Item"]
INPUT_CSV = Path("ITEMS.csv")
OUTPUT_SQL = Path("Bakery-build-ITEMS.sql")


def main() -> None:
    with INPUT_CSV.open("r", newline="", encoding="utf-8-sig") as infile, OUTPUT_SQL.open(
        "w", encoding="utf-8"
    ) as outfile:
        reader = csv.DictReader(infile)
        for row in reader:
            receipt = int(row["Receipt"])
            ordinal = int(row["Ordinal"])
            item = int(row["Item"])
            outfile.write(
                f"INSERT INTO {TABLE_NAME} ({', '.join(COLUMNS)}) VALUES ({receipt}, {ordinal}, {item});\n"
            )

    print(f"Wrote {OUTPUT_SQL}")


if __name__ == "__main__":
    main()
