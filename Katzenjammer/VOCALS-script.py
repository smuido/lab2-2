import mysql.connector as SQLC

# Connect to the database
db = SQLC.connect(host="localhost", user="root", password="", database="Katzenjammer")
cursor = db.cursor()

# Create the database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS Katzenjammer")

# Create the table
cursor.execute("CREATE TABLE VOCALS (" \
    "SongId INT NOT NULL FOREIGN KEY REFERENCES SONGS(SongId)," \
    "Bandmate INT NOT NULL FOREIGN KEY REFERENCES BAND(Id)," \
    "Type VARCHAR(50) NOT NULL," \
    "PRIMARY KEY (SongId, Bandmate, Type),")

# Insert data by reading CSV
with open("VOCALS.csv", "r") as file:
    next(file)  # Skip the header
    for line in file:
        song_id, bandmate_id, vocal_type = line.strip().split(",")
        cursor.execute("INSERT INTO VOCALS (SongId, Bandmate, Type) VALUES (%i, %i, %s)", 
                       (song_id, bandmate_id, vocal_type))
    db.commit()