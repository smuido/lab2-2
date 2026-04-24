from pathlib import Path
import csv
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "generated_sql"


def clean(value):
    if value is None:
        return None
    value = value.strip()
    if value == "" or value.lower() == "null":
        return None
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        value = value[1:-1]
    return value


def sql_text(value):
    value = clean(value)
    if value is None:
        return "NULL"
    value = value.replace("'", "''")
    return f"'{value}'"


def sql_int(value):
    value = clean(value)
    if value is None:
        return "NULL"
    return value


def sql_float(value):
    value = clean(value)
    if value is None:
        return "NULL"
    return value


def sql_date(value):
    value = clean(value)
    if value is None:
        return "NULL"
    return f"STR_TO_DATE('{value}', '%d-%b-%y')"


def write_table(csv_path, out_path, table_name, columns, formatters):
    with csv_path.open(newline="", encoding="utf-8-sig") as handle, \
         out_path.open("w", encoding="utf-8") as out:
        reader = csv.DictReader(handle, skipinitialspace=True)
        for row in reader:
            values = []
            for column, formatter in zip(columns, formatters):
                values.append(formatter(row.get(column)))
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
            out.write(sql + "\n")


def main():
    OUT_DIR.mkdir(exist_ok=True)

    rooms_file = OUT_DIR / "ROOMS-build.sql"
    reservations_file = OUT_DIR / "RESERVATIONS-build.sql"
    combined_file = OUT_DIR / "INN-dataset-build.sql"

    write_table(
        BASE_DIR / "Rooms.csv",
        rooms_file,
        "ROOMS",
        ["RoomCode", "RoomName", "Beds", "bedType", "maxOccupancy", "basePrice", "decor"],
        [sql_text, sql_text, sql_int, sql_text, sql_int, sql_float, sql_text],
    )

    write_table(
        BASE_DIR / "Reservations.csv",
        reservations_file,
        "RESERVATIONS",
        ["Code", "Room", "CheckIn", "CheckOut", "Rate", "LastName", "FirstName", "Adults", "Kids"],
        [sql_int, sql_text, sql_date, sql_date, sql_float, sql_text, sql_text, sql_int, sql_int],
    )

    with combined_file.open("w", encoding="utf-8") as out:
        out.write("DELETE FROM RESERVATIONS;\n")
        out.write("DELETE FROM ROOMS;\n\n")

        for path in [rooms_file, reservations_file]:
            out.write(path.read_text(encoding="utf-8"))
            out.write("\n")


if __name__ == "__main__":
    main()