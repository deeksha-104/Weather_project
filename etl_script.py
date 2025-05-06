import os
import sqlite3
from datetime import datetime

DATA_DIR = 'C:\\Users\\deeks\\Project\\Weather_data\\'  # <- Your input folder
DB_PATH = 'C:\\Users\\deeks\\Project\\weather.db'       # <- Local SQLite DB
LOG_FILE = 'C:\\Users\\deeks\\Project\\etl_log.txt'  # Log file path

# Open log file
def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a') as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"[{timestamp}] {msg}")

# Connect to SQLite DB (create if not exists)
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create necessary tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS Weather_Stations (
    station_id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_name TEXT NOT NULL,
    state TEXT NOT NULL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS weather_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    record_date INTEGER NOT NULL,
    max_temp INTEGER,
    min_temp INTEGER,
    precipitation INTEGER,
    FOREIGN KEY (station_id) REFERENCES Weather_Stations(station_id),
	CHECK (record_date BETWEEN 19850101 AND 20141231),
    UNIQUE (station_id, record_date)               -- Composite uniqueness constraint
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS processed_files (
    filename TEXT PRIMARY KEY,
    processed_at TEXT
)
''')

conn.commit()

# Helpers
def parse_station_from_filename(filename):
    name = os.path.splitext(filename)[0]
    parts = name.split('_')
    if len(parts) < 2:
        raise ValueError("Filename must be in State_StationName.txt format")
    state = parts[0].capitalize()
    station_name = '_'.join(parts[1:]).upper()
    return station_name, state

def get_or_create_station_id(station_name, state):
    cursor.execute("SELECT station_id FROM Weather_Stations WHERE station_name=? AND state=?", (station_name, state))
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        cursor.execute(
            "INSERT INTO Weather_Stations (station_name, state) VALUES (?, ?)",
            (station_name, state)
        )
        conn.commit()
        return cursor.lastrowid

def run_etl():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.txt')]
    new_files = []

    for f in files:
        cursor.execute("SELECT 1 FROM processed_files WHERE filename=?", (f,))
        if not cursor.fetchone():
            new_files.append(f)

    print(f"[INFO] Found {len(new_files)} new file(s).")

    total_inserted = 0

    for filename in new_files:
        filepath = os.path.join(DATA_DIR, filename)
        station_name, state = parse_station_from_filename(filename)
        station_id = get_or_create_station_id(station_name, state)

        inserted = 0
        with open(filepath, 'r') as file:
            for line in file:
                parts = line.strip().split('\t')
                if len(parts) != 4:
                    continue
                record_date, max_temp_raw, min_temp_raw, precip_raw = parts

                max_temp = int(max_temp_raw)
                min_temp = int(min_temp_raw)
                precip = int(precip_raw)

                max_temp = None if max_temp == -9999 else max_temp / 10
                min_temp = None if min_temp == -9999 else min_temp / 10
                precip = None if precip == -9999 else precip / 10

                # Skip duplicates
                cursor.execute("SELECT 1 FROM weather_records WHERE record_date=? AND station_id=?", (record_date, station_id))
                if cursor.fetchone():
                    continue

                cursor.execute('''
                    INSERT INTO weather_records (station_id, record_date, max_temp, min_temp, precipitation)
                    VALUES (?, ?, ?, ?, ?)
                ''', (station_id, record_date, max_temp, min_temp, precip))
                inserted += 1

        cursor.execute("INSERT INTO processed_files (filename, processed_at) VALUES (?, ?)", 
                       (filename, datetime.utcnow().isoformat()))
        conn.commit()
        log(f"{filename}: Inserted {inserted} new records.")
        total_inserted += inserted

    log(f"ETL job complete. Total inserted: {total_inserted}\n")

if __name__ == '__main__':
    run_etl()
