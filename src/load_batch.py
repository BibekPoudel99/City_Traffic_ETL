import os
import sys
import glob
import traceback
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
import uuid

import pandas as pd
from psycopg2.extras import execute_values

try:
    from db import get_connection
except Exception as e:
    print("ERROR: Could not import get_connection from src/db.py. Fix it first.")
    raise

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
batch_dir = os.path.join(project_root, "data", "batch")
chunk_size = 2000

insert_sql = """
INSERT INTO vehicles_batch (vehicle_number, vehicle_type, departure_time, arrival_time, origin, destination) VALUES %s"""

required_column = "vehicle_number"

def find_csv_files(batch_dir: str) -> List[str]:
    pattern = os.path.join(batch_dir, "**/*.csv")  # Made recursive to find files in subfolders
    return sorted(glob.glob(pattern, recursive=True))

def is_traffic_count_file(df: pd.DataFrame) -> bool:
    """Detect if this is a traffic count file based on column structure"""
    # Check for traffic count indicators
    traffic_indicators = ['Start Time', 'Motorized Vehicle', 'Non-motorized Vehicle', 
                         'Truck', 'Bus', 'Car', 'Motor Cycle']
    
    # Convert column names to string and check
    col_str = ' '.join([str(col) for col in df.columns])
    return any(indicator in col_str for indicator in traffic_indicators)

def transform_traffic_count_to_vehicles(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    """Transform traffic count data into individual vehicle records"""
    
    # Find where actual data starts (skip headers)
    data_start_idx = 0
    for i, row in df.iterrows():
        first_col = str(df.iloc[i, 0]) if not pd.isna(df.iloc[i, 0]) else ""
        if first_col.startswith('2011') or first_col.startswith('2012') or first_col.startswith('2013') or first_col.startswith('2014') or first_col.startswith('2015') or first_col.startswith('2016') or first_col.startswith('2018') or first_col.startswith('2020') or first_col.startswith('2021') or first_col.startswith('2022') or first_col.startswith('2024'):
            data_start_idx = i
            break
    
    # Get data rows only
    data_df = df.iloc[data_start_idx:].copy()
    data_df = data_df.reset_index(drop=True)
    
    # Filter out summary rows
    data_df = data_df[~data_df.iloc[:, 0].astype(str).str.contains(
        'Sub-total|Total|Average|Composition|Grand Total', case=False, na=False)]
    
    vehicles = []
    location = os.path.basename(filepath).replace('.csv', '')
    
    for _, row in data_df.iterrows():
        try:
            # Get date and time
            date_str = str(row.iloc[0])  # First column is Date
            time_str = str(row.iloc[1]) if len(row) > 1 else "00:00:00"  # Second column is time
            
            if not date_str.startswith('20'):  # Skip invalid date rows
                continue
                
            # Parse base datetime
            try:
                base_datetime = pd.to_datetime(f"{date_str} {time_str}")
            except:
                continue
            
            # Extract vehicle counts from the row
            # Based on your CSV structure, counts start from column 2
            counts_data = row.iloc[2:].tolist()
            
            # Vehicle type mapping based on your CSV structure
            vehicle_types = [
                ('Multi_Axle_Truck', 0), ('Heavy_Truck', 1), ('Light_Truck', 2),
                ('Big_Bus', 3), ('Mini_Bus', 4), ('Micro_Bus', 5),
                ('Car', 6), ('Car_b', 7), ('Motor_Cycle', 8), ('Motor_Cycle_b', 9),
                ('Utility_Vehicle', 10), ('Utility_Vehicle_b', 11),
                ('Tractor', 12), ('Tractor_b', 13), ('Three_Wheeler', 14), ('Three_Wheeler_b', 15),
                ('Four_Wheel_Drive', 16), ('Four_Wheel_Drive_b', 17),
                ('Power_Tiller', 18), ('Power_Tiller_b', 19),
                ('Rickshaw', 20), ('Rickshaw_b', 21), ('Hand_Cart', 22), ('Hand_Cart_b', 23)
            ]
            
            # Generate individual vehicle records
            for vehicle_type, col_idx in vehicle_types:
                if col_idx < len(counts_data):
                    try:
                        count = int(float(counts_data[col_idx])) if pd.notna(counts_data[col_idx]) and str(counts_data[col_idx]).strip() != '' else 0
                    except (ValueError, TypeError):
                        count = 0
                    
                    # Create individual records for each vehicle
                    for i in range(count):
                        # Generate unique vehicle number (max 20 chars)
                        date_short = date_str.replace('-', '')[-6:]  # Last 6 digits of date (YYMMDD)
                        time_short = time_str.replace(':', '')[:4]   # First 4 digits of time (HHMM)
                        vehicle_number = f"{vehicle_type[:3].upper()}{date_short}{time_short}{i%1000:03d}"
                        
                        # Create realistic departure and arrival times
                        # Spread departures within the hour
                        departure_offset = timedelta(minutes=i * 2 + (i % 5) * 10)
                        departure_time = base_datetime + departure_offset
                        
                        # Arrival time 30 minutes to 2 hours later
                        arrival_offset = timedelta(hours=1, minutes=30 + (i % 4) * 15)
                        arrival_time = departure_time + arrival_offset
                        
                        # Determine origin and destination based on vehicle type
                        if 'Bus' in vehicle_type:
                            origin = f"{location}_Bus_Station"
                            destination = "City_Center"
                        elif 'Truck' in vehicle_type:
                            origin = f"{location}_Industrial_Area"
                            destination = "Commercial_District"
                        else:
                            origin = f"{location}_Entry_Point"
                            destination = "Various_Destinations"
                        
                        vehicles.append({
                            'vehicle_number': vehicle_number,
                            'vehicle_type': vehicle_type.replace('_', ' '),
                            'departure_time': departure_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'arrival_time': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'origin': origin,
                            'destination': destination
                        })
                        
        except Exception as e:
            print(f"Error processing row in traffic count transformation: {e}")
            continue
    
    print(f"Transformed {len(vehicles)} vehicle records from traffic count data")
    return pd.DataFrame(vehicles)

def normalize_dataframe(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    # Check if this is a traffic count file and transform if needed
    if is_traffic_count_file(df):
        print(f"Detected traffic count format in {os.path.basename(filepath)}")
        df = transform_traffic_count_to_vehicles(df, filepath)
    
    expected_cols = ["vehicle_number", "vehicle_type", "departure_time", "arrival_time", "origin", "destination"]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None

    # strip strings
    str_cols = ["vehicle_number", "vehicle_type", "origin", "destination"]
    for c in str_cols:
        df[c] = df[c].astype("string").str.strip()

    # parse timestamps - allow multiple common formats ; coerce errors to NaT
    df["departure_time"] = pd.to_datetime(df["departure_time"], errors="coerce", utc=False)
    df["arrival_time"] = pd.to_datetime(df["arrival_time"], errors="coerce", utc=False)

    # drop rows without vehicle number
    df = df[df[required_column].notna() & (df[required_column].str.len() > 0)]

    # reset index
    df = df.reset_index(drop=True)
    return df

def df_to_tuples(df: pd.DataFrame) -> List[Tuple]:
    """
    Convert DataFrame to list of tuples matching INSERT order.
    Convert NaT to None so psycopg2 maps it to SQL NULL.
    """
        
    rows = []
    for _,r in df.iterrows():
        dep = r["departure_time"]
        arr = r["arrival_time"]

        dep_value = None if pd.isna(dep) else dep.to_pydatetime()
        arr_value = None if pd.isna(arr) else arr.to_pydatetime()

        tup = (
            r["vehicle_number"] if not pd.isna(r["vehicle_number"]) else None,
            r["vehicle_type"] if not pd.isna(r["vehicle_type"]) else None,
            dep_value,
            arr_value,
            r["origin"] if not pd.isna(r["origin"]) else None,
            r["destination"] if not pd.isna(r["destination"]) else None,
        )
        rows.append(tup)
    
    return rows

def bulk_insert(conn, rows: List[Tuple]):
    # execute bulk insert using pyscopg2.extras.execute_values for performance
    if not rows:
        return 0
    
    cur = conn.cursor()
    inserted = 0
    try:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            execute_values(cur, insert_sql, chunk, template=None, page_size=chunk_size)
            inserted += len(chunk)
        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

def load_file(conn, filepath : str) -> Tuple[int, int]:
    # load a single csv file. returns (inserted_counts, skipped_counts)
    try:
        df = pd.read_csv(filepath, dtype=str) #read everything as str first
    except Exception as e:
        print(f"Failed to read csv '{filepath}': {e}")
        return 0, 0
    
    df = normalize_dataframe(df, filepath)  # Pass filepath for transformation context
    total_rows = len(df)
    if total_rows == 0:
        print (f"No valid rows found in {os.path.basename(filepath)}")
        return 0, 0
    
    rows = df_to_tuples(df)
    inserted = bulk_insert(conn, rows)
    skipped = total_rows - inserted
    return inserted, skipped

def main():
    csv_files = find_csv_files(batch_dir)
    if not csv_files:
        print (f"No csv files in the {batch_dir}. put your batch files there and re-run.")
        return
    
    # open single connection for all files
    try:
        conn = get_connection()
    except Exception as e:
        print("ERROR! Could not get DB connection. check src/db.py and .env.")
        traceback.print_exc()
        sys.exit(1)

    total_inserted = 0
    total_skipped = 0

    print(f"Found {len(csv_files)} file(s). Starting load...")

    for f in csv_files:
        print(f"Processing: {os.path.basename(f)}")
        try:
            inserted, skipped = load_file(conn, f)
            total_inserted += inserted
            total_skipped += skipped
        except Exception as e:
            print(f"Error loading '{os.path.basename(f)}': {e}")
            traceback.print_exc()

            continue

    conn.close()
    print(f"Done. Total inserted: {total_inserted}. Total skipped: {total_skipped}.")

if __name__ == "__main__":
    main()
    