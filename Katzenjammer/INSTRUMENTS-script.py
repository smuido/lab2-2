import mysql.connector as SQLC

# Connect to the database
db = SQLC.connect(host="localhost", user="root", password="", database="Katzenjammer")
cursor = db.cursor()

# Create the database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS Katzenjammer")

# Create the table
cursor.execute("CREATE TABLE IF NOT EXISTS INSTRUMENTS (" \
    "SongId INT NOT NULL FOREIGN KEY REFERENCES SONGS(SongId)," \
    "BandmateId INT NOT NULL FOREIGN KEY REFERENCES BAND(Id)," \
    "Instrument VARCHAR(100) NOT NULL," \
    "PRIMARY KEY (SongId, BandmateId, Instrument),"
)

# Insert data by reading CSV
with open("INSTRUMENTS.csv", "r") as file:
    next(file)  # Skip the header
    for line in file:
        song_id, bandmate_id, instrument = line.strip().split(",")
        cursor.execute("INSERT INTO INSTRUMENTS (SongId, BandmateId, Instrument) VALUES (%i, %i, %s)", 
                       (song_id, bandmate_id, instrument))
    db.commit()