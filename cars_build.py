from pathlib import Path
import csv

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


def sql_float_or_null(value):
    value = clean(value)
    if value is None:
        return "NULL"
    return value


def sql_int_or_null(value):
    value = clean(value)
    if value is None:
        return "NULL"
    return value


def write_table(csv_path, out_path, table_name, columns, formatters, row_filter=None):
    with csv_path.open(newline="", encoding="utf-8-sig") as handle, \
         out_path.open("w", encoding="utf-8") as out:
        reader = csv.DictReader(handle, skipinitialspace=True)
        for row in reader:
            if row_filter is not None and not row_filter(row):
                continue
            values = []
            for column, formatter in zip(columns, formatters):
                values.append(formatter(row.get(column)))
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
            out.write(sql + "\n")


def keep_car_maker(row):
    return clean(row.get("Id")) != "10"


def keep_model_list(row):
    return clean(row.get("Maker")) != "10" and clean(row.get("ModelId")) != "14"


def keep_car_names(row):
    return clean(row.get("Id")) != "35"


def keep_cars_data(row):
    return clean(row.get("Id")) != "35"


def main():
    OUT_DIR.mkdir(exist_ok=True)

    continents_file = OUT_DIR / "CONTINENTS-build.sql"
    countries_file = OUT_DIR / "COUNTRIES-build.sql"
    makers_file = OUT_DIR / "CAR_MAKERS-build.sql"
    models_file = OUT_DIR / "MODEL_LIST-build.sql"
    names_file = OUT_DIR / "CAR_NAMES-build.sql"
    data_file = OUT_DIR / "CARS_DATA-build.sql"
    combined_file = OUT_DIR / "CARS-dataset-build.sql"

    write_table(
        BASE_DIR / "continents.csv",
        continents_file,
        "CONTINENTS",
        ["ContId", "Continent"],
        [sql_int, sql_text],
    )

    write_table(
        BASE_DIR / "countries.csv",
        countries_file,
        "COUNTRIES",
        ["CountryId", "CountryName", "Continent"],
        [sql_int, sql_text, sql_int],
    )

    write_table(
        BASE_DIR / "car-makers.csv",
        makers_file,
        "CAR_MAKERS",
        ["Id", "Maker", "FullName", "Country"],
        [sql_int, sql_text, sql_text, sql_int],
        row_filter=keep_car_maker,
    )

    write_table(
        BASE_DIR / "model-list.csv",
        models_file,
        "MODEL_LIST",
        ["ModelId", "Maker", "Model"],
        [sql_int, sql_int, sql_text],
        row_filter=keep_model_list,
    )

    write_table(
        BASE_DIR / "car-names.csv",
        names_file,
        "CAR_NAMES",
        ["Id", "Make", "Model", "MakeId"],
        [sql_int, sql_text, sql_text, sql_int],
        row_filter=keep_car_names,
    )

    write_table(
        BASE_DIR / "cars-data.csv",
        data_file,
        "CARS_DATA",
        ["Id", "MPG", "Cylinders", "Edispl", "Horsepower", "Weight", "Accelerate", "Year"],
        [sql_int, sql_int_or_null, sql_int_or_null, sql_float_or_null, sql_int_or_null, sql_int_or_null, sql_float_or_null, sql_int_or_null],
        row_filter=keep_cars_data,
    )

    with combined_file.open("w", encoding="utf-8") as out:
        out.write("DELETE FROM CARS_DATA;\n")
        out.write("DELETE FROM CAR_NAMES;\n")
        out.write("DELETE FROM MODEL_LIST;\n")
        out.write("DELETE FROM CAR_MAKERS;\n")
        out.write("DELETE FROM COUNTRIES;\n")
        out.write("DELETE FROM CONTINENTS;\n\n")

        for path in [continents_file, countries_file, makers_file, models_file, names_file, data_file]:
            out.write(path.read_text(encoding="utf-8"))
            out.write("\n")


if __name__ == "__main__":
    main()