import mysql.connector as SQLC

# Connect to the database
db = SQLC.connect(host="localhost", user="root", password="", database="Katzenjammer")
cursor = db.cursor()

# Create the database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS Katzenjammer")

# Create the table
cursor.execute("CREATE TABLE PERFORMANCE (" \
    "SongId INT NOT NULL FOREIGN KEY REFERENCES SONGS(SongId)," \
    "Bandmate INT NOT NULL FOREIGN KEY REFERENCES BAND(Id)," \
    "StagePosition VARCHAR(100) NOT NULL," \
    "PRIMARY KEY (SongId, Bandmate, StagePosition))")

# Insert data by reading CSV
with open("PERFORMANCE.csv", "r") as file:
    next(file)  # Skip the header
    for line in file:
        song_id, bandmate_id, stage_position = line.strip().split(",")
        cursor.execute("INSERT INTO PERFORMANCE (SongId, Bandmate, StagePosition) VALUES (%i, %i, %s)", 
                       (song_id, bandmate_id, stage_position))
    db.commit()