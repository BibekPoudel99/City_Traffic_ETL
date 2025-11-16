from db import get_connection

# SQL commands to create tables
TABLE_QUERIES = [
    """
    CREATE TABLE IF NOT EXISTS vehicles_batch (
        vehicle_id SERIAL PRIMARY KEY,
        vehicle_number VARCHAR(20) NOT NULL,
        vehicle_type VARCHAR(20),
        departure_time TIMESTAMP,
        arrival_time TIMESTAMP,
        origin VARCHAR(50),
        destination VARCHAR(50),
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS vehicles_realtime (
        vehicle_id SERIAL PRIMARY KEY,
        vehicle_number VARCHAR(20) NOT NULL,
        vehicle_type VARCHAR(20),
        latitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL,
        speed FLOAT,
        status VARCHAR(20),
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
]

def create_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        for query in TABLE_QUERIES:
            cur.execute(query)
            print("Table created or already exists.")
        conn.commit()
        cur.close()
    except Exception as e:
        print("Error creating tables:", e)
    finally:
        if conn:
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    create_tables()
