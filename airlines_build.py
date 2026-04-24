from pathlib import Path
import csv

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "generated_sql"


def sql_text(value):
    if value is None:
        return "NULL"
    value = value.strip()
    if value == "" or value.lower() == "null":
        return "NULL"
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        value = value[1:-1]
    value = value.replace("'", "''")
    return f"'{value}'"


def sql_int(value):
    if value is None:
        return "NULL"
    value = value.strip()
    if value == "" or value.lower() == "null":
        return "NULL"
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        value = value[1:-1]
    return value


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

    airlines_file = OUT_DIR / "AIRLINES-build.sql"
    airport_file = OUT_DIR / "AIRPORT-build.sql"
    flights_file = OUT_DIR / "FLIGHTS-build.sql"
    combined_file = OUT_DIR / "AIRLINES-dataset-build.sql"

    write_table(
        BASE_DIR / "airlines.csv",
        airlines_file,
        "AIRLINES",
        ["Id", "Airline", "Abbreviation", "Country"],
        [sql_int, sql_text, sql_text, sql_text],
    )

    write_table(
        BASE_DIR / "airports100.csv",
        airport_file,
        "AIRPORT",
        ["City", "AirportCode", "AirportName", "Country", "CountryAbbrev"],
        [sql_text, sql_text, sql_text, sql_text, sql_text],
    )

    write_table(
        BASE_DIR / "flights.csv",
        flights_file,
        "FLIGHTS",
        ["Airline", "FlightNo", "SourceAirport", "DestAirport"],
        [sql_int, sql_int, sql_text, sql_text],
    )

    with combined_file.open("w", encoding="utf-8") as out:
        out.write("DELETE FROM FLIGHTS;\n")
        out.write("DELETE FROM AIRPORT;\n")
        out.write("DELETE FROM AIRLINES;\n\n")

        for path in [airlines_file, airport_file, flights_file]:
            out.write(path.read_text(encoding="utf-8"))
            out.write("\n")


if __name__ == "__main__":
    main()