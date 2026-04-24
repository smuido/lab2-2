import mysql.connector as SQLC

# Connect to the database
db = SQLC.connect(host="localhost", user="root", password="", database="Katzenjammer")
cursor = db.cursor()

# Create the database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS Marathon")

# Create the table
cursor.execute("CREATE TABLE IF NOT EXISTS MARATHON (" \
    "Place INT," \
    "Time TIME," \
    "Pace VARCHAR(10)," \
    "GroupPlace INT," \
    "`Group` VARCHAR(50)," \
    "Age INT," \
    "Sex CHAR(1)," \
    "BIBNumber INT," \
    "FirstName VARCHAR(100)," \
    "LastName VARCHAR(100)," \
    "Town VARCHAR(100)," \
    "State VARCHAR(50)," \
    "PRIMARY KEY(Place, FirstName, LastName))")

# Insert data by reading CSV
with open("MARATHON.csv", "r") as file:
    next(file)  # Skip the header
    for line in file:
        place, time, pace, group_place, group, age, sex, bib_number, first_name, last_name, town, state = line.strip().split(",")
        cursor.execute("INSERT INTO MARATHON (Place, Time, Pace, GroupPlace, `Group`, Age, Sex, BIBNumber, FirstName, LastName, Town, State)" \
            "VALUES (%i, %s, %s, %i, %s, %i, %s, %i, %s, %s, %s, %s)",
                (place, time, pace, group_place, group, age, sex, bib_number, first_name, last_name, town, state))
    db.commit()