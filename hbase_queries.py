import happybase
import json

# --- HBase Connection ---
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
TABLE_NAME = 'sessions'


def connect_to_hbase():
    """Connect to HBase via Thrift"""
    try:
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
        connection.open()
        print(" Connected to HBase successfully!\n")
        return connection
    except Exception as e:
        print(f" Failed to connect to HBase: {e}")
        return None


# ============================================================
# QUERY 1: Get All Sessions for a Specific User
# Business Question: What is the browsing history of a user?
# ============================================================
def get_user_sessions(table, user_id, limit=10):
    """
    Retrieve all sessions for a specific user.
    
    This demonstrates HBase's strength: efficient prefix scans.
    Since row key = user_id + "_" + timestamp, all sessions for
    a user are stored together and can be retrieved quickly.
    """
    
    print("=" * 60)
    print(f"QUERY 1: Get Sessions for User '{user_id}'")
    print("=" * 60)
    print("Business Question: What is the browsing history of this user?\n")
    
    # Create row prefix for scanning
    row_prefix = f"{user_id}_".encode()
    
    sessions = []
    count = 0
    
    for key, data in table.scan(row_prefix=row_prefix, limit=limit):
        count += 1
        session = {
            'row_key': key.decode(),
            'session_id': data.get(b'session_info:session_id', b'').decode(),
            'start_time': data.get(b'session_info:start_time', b'').decode(),
            'duration': data.get(b'session_info:duration', b'').decode(),
            'conversion_status': data.get(b'session_info:conversion_status', b'').decode(),
            'device_type': data.get(b'device:type', b'').decode(),
            'browser': data.get(b'device:browser', b'').decode(),
            'city': data.get(b'geo:city', b'').decode(),
            'state': data.get(b'geo:state', b'').decode(),
            'page_views': data.get(b'activity:page_views_count', b'').decode()
        }
        sessions.append(session)
    
    # Display results
    print(f"Found {count} session(s) for user {user_id}:\n")
    print("-" * 90)
    print(f"{'Session ID':<20} {'Start Time':<22} {'Duration':<10} {'Status':<12} {'Device':<10} {'City':<15}")
    print("-" * 90)
    
    for s in sessions:
        print(f"{s['session_id']:<20} {s['start_time']:<22} {s['duration']:<10} {s['conversion_status']:<12} {s['device_type']:<10} {s['city'][:14]:<15}")
    
    print()
    return sessions


# ============================================================
# QUERY 2: Get Sessions by Conversion Status
# Business Question: Which sessions resulted in purchases?
# ============================================================
def get_converted_sessions(table, limit=10):
    """
    Find sessions that resulted in conversions (purchases).
    
    Note: This requires a full table scan since we're not
    filtering by row key prefix. In production, you might
    create a secondary index or use a different row key design.
    """
    
    print("=" * 60)
    print("QUERY 2: Get Converted Sessions (Purchases)")
    print("=" * 60)
    print("Business Question: Which sessions resulted in purchases?\n")
    
    converted = []
    scanned = 0
    
    for key, data in table.scan(limit=1000):  # Scan more to find converted
        scanned += 1
        status = data.get(b'session_info:conversion_status', b'').decode()
        
        if status == 'converted':
            session = {
                'row_key': key.decode(),
                'user_id': key.decode().split('_')[0] + '_' + key.decode().split('_')[1],
                'session_id': data.get(b'session_info:session_id', b'').decode(),
                'start_time': data.get(b'session_info:start_time', b'').decode(),
                'duration': data.get(b'session_info:duration', b'').decode(),
                'device_type': data.get(b'device:type', b'').decode(),
                'referrer': data.get(b'session_info:referrer', b'').decode()
            }
            converted.append(session)
            
            if len(converted) >= limit:
                break
    
    # Display results
    print(f"Scanned {scanned} rows, found {len(converted)} converted sessions:\n")
    print("-" * 100)
    print(f"{'User ID':<15} {'Session ID':<20} {'Start Time':<22} {'Duration':<10} {'Device':<10} {'Referrer':<15}")
    print("-" * 100)
    
    for s in converted:
        print(f"{s['user_id']:<15} {s['session_id']:<20} {s['start_time']:<22} {s['duration']:<10} {s['device_type']:<10} {s['referrer']:<15}")
    
    print()
    return converted


# ============================================================
# QUERY 3: Get Session Details by Row Key
# Business Question: What exactly happened in a specific session?
# ============================================================
def get_session_details(table, row_key):
    """
    Get complete details for a specific session.
    
    This is the fastest query type in HBase - direct row key lookup.
    """
    
    print("=" * 60)
    print(f"QUERY 3: Get Session Details")
    print("=" * 60)
    print(f"Row Key: {row_key}\n")
    
    row = table.row(row_key.encode())
    
    if not row:
        print(" Session not found!")
        return None
    
    print("Session Info:")
    print("-" * 40)
    for key, value in row.items():
        family, column = key.decode().split(':')
        print(f"  {family}.{column}: {value.decode()[:60]}...")
    
    print()
    return row


# ============================================================
# QUERY 4: Count Sessions by Device Type
# Business Question: What devices do our users prefer?
# ============================================================
def count_by_device(table, sample_size=5000):
    """
    Count sessions by device type.
    
    Note: For large datasets, this would typically be done with
    a MapReduce job or Spark, not a client-side scan.
    """
    
    print("=" * 60)
    print("QUERY 4: Sessions by Device Type")
    print("=" * 60)
    print("Business Question: What devices do our users prefer?\n")
    
    device_counts = {}
    
    for key, data in table.scan(limit=sample_size, columns=[b'device:type']):
        device = data.get(b'device:type', b'unknown').decode()
        device_counts[device] = device_counts.get(device, 0) + 1
    
    total = sum(device_counts.values())
    
    print(f"Sample size: {total} sessions\n")
    print("-" * 40)
    print(f"{'Device Type':<15} {'Count':<10} {'Percentage':<10}")
    print("-" * 40)
    
    for device, count in sorted(device_counts.items(), key=lambda x: -x[1]):
        pct = (count / total) * 100
        print(f"{device:<15} {count:<10} {pct:.1f}%")
    
    print()
    return device_counts


# ============================================================
# MAIN FUNCTION
# ============================================================
def main():
    """Run all HBase queries"""
    
    print("\n" + "=" * 60)
    print("   HBASE QUERIES - E-COMMERCE SESSION ANALYTICS")
    print("=" * 60 + "\n")
    
    # Connect
    connection = connect_to_hbase()
    if connection is None:
        return
    
    table = connection.table(TABLE_NAME)
    
    # First, find a user_id that exists in the data
    print("Finding sample user IDs...\n")
    sample_users = set()
    sample_row_key = None
    
    for key, data in table.scan(limit=100):
        row_key = key.decode()
        # Extract user_id (format: user_XXXXXX_timestamp)
        parts = row_key.split('_')
        user_id = f"{parts[0]}_{parts[1]}"
        sample_users.add(user_id)
        if sample_row_key is None:
            sample_row_key = row_key
    
    sample_user = list(sample_users)[0] if sample_users else "user_000001"
    print(f"Sample users found: {list(sample_users)[:5]}")
    print(f"Using user: {sample_user}\n")
    
    # Run queries
    user_sessions = get_user_sessions(table, sample_user, limit=5)
    converted = get_converted_sessions(table, limit=5)
    
    if sample_row_key:
        details = get_session_details(table, sample_row_key)
    
    device_stats = count_by_device(table, sample_size=5000)
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f" User sessions query: Found {len(user_sessions)} sessions")
    print(f" Converted sessions query: Found {len(converted)} conversions")
    print(f" Device distribution query: {len(device_stats)} device types")
    
    # Close
    connection.close()
    print("\n All HBase queries completed!")


if __name__ == "__main__":
    main()