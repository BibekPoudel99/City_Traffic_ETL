# City Traffic ETL Pipeline

A robust ETL pipeline that transforms traffic count data into individual vehicle records for analysis and storage in PostgreSQL.

##  Overview

This project automatically processes traffic count CSV files and converts aggregate counts into detailed individual vehicle records, making traffic data analysis more granular and insightful.

##  Data Source

This ETL pipeline processes traffic count data from **Nepal road traffic survey stations**. The data includes:

- **152 CSV files** spanning multiple years (2011-2024)
- **Multiple monitoring locations**: F00101, F00201, F03801, H0101, NH01-001, etc.
- **Vehicle categories**: Cars, buses, trucks, motorcycles, utility vehicles, and more
- **Temporal coverage**: Hourly traffic counts across different dates
- **Data format**: Aggregate counts per vehicle type per hour

**Sample file structure**:
```
data/batch/ssrn/
├── F00101_traffic_count_2011.csv
├── F00101_traffic_count_2012.csv
├── NH01-001_traffic_count_2020.csv
└── ... (152 total files)
```

##  ETL Process

###  **Extract**
- **Auto-detection**: Automatically identifies traffic count CSV files vs regular vehicle data
- **Recursive search**: Finds all CSV files in `data/batch/` including subfolders
- **Multi-format support**: Handles various CSV structures and date formats
- **Error resilience**: Continues processing even if individual files fail

###  **Transform**
- **Count-to-records conversion**: Transforms `Car: 5` → 5 individual vehicle records
- **Smart data generation**: Creates realistic vehicle IDs, timestamps, and routes
- **Schema mapping**: Maps traffic counts to database schema:
  ```
  Traffic Count Data → Individual Vehicle Records
  Date + Time + Car: 3 → 3 records with unique IDs, times, origins
  ```
- **Vehicle routing logic**: Assigns origins/destinations based on vehicle type
- **Data validation**: Cleans and validates all records before loading

###  **Load**
- **Bulk insert**: Efficiently loads data in 2000-record chunks
- **PostgreSQL storage**: Structured storage in `vehicles_batch` table
- **Transaction safety**: Rollback on errors, commit on success
- **Progress tracking**: Real-time feedback on processing status

##  Database Schema

```sql
CREATE TABLE vehicles_batch (
    vehicle_id SERIAL PRIMARY KEY,
    vehicle_number VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(20),
    departure_time TIMESTAMP,
    arrival_time TIMESTAMP,
    origin VARCHAR(50),
    destination VARCHAR(50),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

##  Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL database
- CSV files in `data/batch/` directory

### Setup
1. **Clone and setup environment**:
   ```bash
   git clone https://github.com/BibekPoudel99/City_Traffic_ETL.git
   cd City_Traffic_ETL
   python -m venv venv
   venv\Scripts\activate  # Windows
   pip install -r requirements.txt
   ```

2. **Configure database**:
   ```bash
   # Create .env file with your database credentials
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=traffic_db
   DB_USER=your_user
   DB_PASS=your_password
   ```

3. **Initialize database**:
   ```bash
   python src/create_tables.py
   ```

4. **Run ETL pipeline**:
   ```bash
   python src/load_batch.py
   ```

##  Performance

- **Processed**: 152 CSV files
- **Loaded**: 4.7+ million vehicle records
- **Speed**: ~2000 records/second
- **Success rate**: 99%+ (continues on individual file errors)
- **Zero configuration**: Automatic file detection and processing

