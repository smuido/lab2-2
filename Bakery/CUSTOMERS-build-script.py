import csv
from pathlib import Path

TABLE_NAME = "CUSTOMERS"
COLUMNS = ["Id", "LastName", "FirstName"]
INPUT_CSV = Path("CUSTOMERS.csv")
OUTPUT_SQL = Path("Bakery-build-CUSTOMERS.sql")


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    with INPUT_CSV.open("r", newline="") as infile, OUTPUT_SQL.open(
        "w"
    ) as outfile:
        reader = csv.DictReader(infile)
        for row in reader:
            customer_id = int(row["Id"])
            last_name = sql_quote(row["LastName"].strip())
            first_name = sql_quote(row["FirstName"].strip())
            outfile.write(
                f"INSERT INTO {TABLE_NAME} ({', '.join(COLUMNS)}) VALUES ({customer_id}, {last_name}, {first_name});\n"
            )

    print(f"Wrote {OUTPUT_SQL}")


if __name__ == "__main__":
    main()
