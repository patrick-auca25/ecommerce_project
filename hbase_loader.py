

import happybase
import json
import os

# --- HBase Connection ---
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
TABLE_NAME = 'sessions'

def connect_to_hbase():
    """Connect to HBase via Thrift"""
    try:
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
        connection.open()
        print(" Connected to HBase successfully!")
        return connection
    except Exception as e:
        print(f" Failed to connect to HBase: {e}")
        return None


def create_sessions_table(connection):
    """
    Get HBase table for sessions (table already created via HBase shell).
    
    Schema Design:
    ==============
    Row Key: user_id + "_" + timestamp (e.g., "user_000042_2025-03-12T14:37:22")
             This allows efficient scanning of all sessions for a specific user.
    
    Column Families:
    - session_info: Basic session metadata (duration, conversion status, referrer)
    - device: Device information (type, os, browser)
    - geo: Geographic data (city, state, country, ip)
    - activity: Browsing activity (viewed products, cart contents)
    """
    
    print("\n--- Connecting to HBase Table ---")
    
    # Check if table exists
    existing_tables = [t.decode() for t in connection.tables()]
    
    if TABLE_NAME in existing_tables:
        print(f" Table '{TABLE_NAME}' exists with column families: session_info, device, geo, activity")
    else:
        print(f" Table '{TABLE_NAME}' not found! Please create it in HBase shell first:")
        print(f"   create 'sessions', 'session_info', 'device', 'geo', 'activity'")
        return None
    
    return connection.table(TABLE_NAME)


def generate_row_key(user_id, start_time):
    """
    Generate row key for efficient querying.
    
    Format: user_id + "_" + timestamp
    Example: user_000042_2025-03-12T14:37:22
    
    Why this design?
    - Rows are sorted by key in HBase
    - All sessions for same user are stored together
    - Can scan by user_id prefix to get all user's sessions
    - Timestamp allows ordering sessions chronologically
    """
    return f"{user_id}_{start_time}"


def load_sessions(connection, table, data_dir="."):
    """Load session data from JSON files into HBase"""
    
    print("\n--- Loading Sessions into HBase ---")
    
    # Find all session files
    session_files = sorted([f for f in os.listdir(data_dir) if f.startswith('sessions_') and f.endswith('.json')])
    
    if not session_files:
        print(" No session files found!")
        return 0
    
    print(f"Found {len(session_files)} session file(s): {session_files}")
    
    total_loaded = 0
    batch_size = 1000
    
    for session_file in session_files:
        filepath = os.path.join(data_dir, session_file)
        print(f"\nLoading {session_file}...")
        
        with open(filepath, 'r') as f:
            sessions = json.load(f)
        
        print(f"   {len(sessions):,} sessions in file")
        
        # Use batch for faster loading
        batch = table.batch()
        batch_count = 0
        
        for i, session in enumerate(sessions):
            # Generate row key
            row_key = generate_row_key(session['user_id'], session['start_time'])
            
            # Prepare data for each column family
            data = {
                # Session info
                b'session_info:session_id': session['session_id'].encode(),
                b'session_info:duration': str(session['duration_seconds']).encode(),
                b'session_info:conversion_status': session['conversion_status'].encode(),
                b'session_info:referrer': session['referrer'].encode(),
                b'session_info:start_time': session['start_time'].encode(),
                b'session_info:end_time': session['end_time'].encode(),
                
                # Device info
                b'device:type': session['device_profile']['type'].encode(),
                b'device:os': session['device_profile']['os'].encode(),
                b'device:browser': session['device_profile']['browser'].encode(),
                
                # Geo info
                b'geo:city': session['geo_data']['city'].encode(),
                b'geo:state': session['geo_data']['state'].encode(),
                b'geo:country': session['geo_data']['country'].encode(),
                b'geo:ip_address': session['geo_data']['ip_address'].encode(),
                
                # Activity info
                b'activity:viewed_products': json.dumps(session['viewed_products']).encode(),
                b'activity:cart_contents': json.dumps(session['cart_contents']).encode(),
                b'activity:page_views_count': str(len(session['page_views'])).encode()
            }
            
            # Put data
            batch.put(row_key.encode(), data)
            batch_count += 1
            
            # Send batch
            if batch_count >= batch_size:
                batch.send()
                total_loaded += batch_count
                print(f"   Loaded {total_loaded:,} sessions...")
                batch = table.batch()
                batch_count = 0
        
        # Send remaining
        if batch_count > 0:
            batch.send()
            total_loaded += batch_count
    
    print(f"\n Total sessions loaded: {total_loaded:,}")
    return total_loaded


def verify_data(table):
    """Verify data was loaded correctly"""
    
    print("\n--- Verifying Data ---")
    
    # Count rows (scan first 100)
    count = 0
    sample_row = None
    
    for key, data in table.scan(limit=100):
        count += 1
        if sample_row is None:
            sample_row = (key, data)
    
    print(f"Scanned first 100 rows, found: {count}")
    
    if sample_row:
        print(f"\nSample row:")
        print(f"   Row Key: {sample_row[0].decode()}")
        print(f"   Columns:")
        for col, val in list(sample_row[1].items())[:8]:
            print(f"      {col.decode()}: {val.decode()[:50]}...")
    
    return count


def main():
    """Main function"""
    
    print("=" * 60)
    print("   HBase Data Loader for E-commerce Sessions")
    print("=" * 60)
    
    # Connect
    connection = connect_to_hbase()
    if connection is None:
        return
    
    # Create table
    table = create_sessions_table(connection)
    if table is None:
        connection.close()
        return
    
    # Load data
    data_dir = r"D:\Patrick\AUCA\SEM3\bigdatanalytics\ecommerce_project\raw_data\session"
    total = load_sessions(connection, table, data_dir)
    
    # Verify
    verify_data(table)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f" Table created: {TABLE_NAME}")
    print(f" Sessions loaded: {total:,}")
    print(f" Column families: session_info, device, geo, activity")
    print("\nRow Key Design: user_id + '_' + timestamp")
    print("Example: user_000042_2025-03-12T14:37:22")
    
    # Close
    connection.close()
    print("\n HBase loading complete!")


if __name__ == "__main__":
    main()