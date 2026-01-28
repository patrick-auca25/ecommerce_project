from pymongo import MongoClient
import happybase

# --- Configuration ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_USER = "admin"
MONGO_PASSWORD = "password123"
MONGO_DB = "ecommerce"

HBASE_HOST = "localhost"
HBASE_PORT = 9090


# ============================================================
# CONNECTION HELPERS
# ============================================================
def connect_mongodb():
    """Connect to MongoDB"""
    connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    client = MongoClient(connection_string)
    db = client[MONGO_DB]
    print(" Connected to MongoDB")
    return client, db


def connect_hbase():
    """Connect to HBase"""
    try:
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
        connection.open()
        print(" Connected to HBase")
        return connection
    except Exception as e:
        print(f" HBase connection failed: {e}")
        return None


# ============================================================
# ANALYSIS 1: CUSTOMER LIFETIME VALUE (CLV)
# ============================================================
def customer_lifetime_value(mongo_db):
    """
    Calculate Customer Lifetime Value (CLV) using MongoDB aggregation.
    
    Data Sources:
    - MongoDB: User profiles (registration date, location)
    - MongoDB: Transactions (purchase history)
    """
    
    print("\n" + "=" * 70)
    print("ANALYSIS 1: CUSTOMER LIFETIME VALUE (CLV)")
    print("=" * 70)
    print("\nData Sources: MongoDB (users, transactions)")
    print("Business Question: Who are our most valuable customers?\n")
    
    # --- Get CLV data using MongoDB aggregation ---
    print("Calculating CLV from MongoDB...")
    
    pipeline = [
        {"$group": {
            "_id": "$user_id",
            "total_spent": {"$sum": "$total"},
            "order_count": {"$sum": 1},
            "avg_order_value": {"$avg": "$total"},
            "first_purchase": {"$min": "$timestamp"},
            "last_purchase": {"$max": "$timestamp"}
        }},
        {"$addFields": {
            "clv_score": {"$round": [{"$multiply": ["$avg_order_value", "$order_count"]}, 2]},
            "customer_segment": {
                "$switch": {
                    "branches": [
                        {"case": {"$gte": [{"$multiply": ["$avg_order_value", "$order_count"]}, 1000]}, "then": "Platinum"},
                        {"case": {"$gte": [{"$multiply": ["$avg_order_value", "$order_count"]}, 500]}, "then": "Gold"},
                        {"case": {"$gte": [{"$multiply": ["$avg_order_value", "$order_count"]}, 100]}, "then": "Silver"}
                    ],
                    "default": "Bronze"
                }
            }
        }},
        {"$sort": {"clv_score": -1}},
        {"$limit": 15}
    ]
    
    top_customers = list(mongo_db.transactions.aggregate(pipeline))
    
    print("\n--- Top 15 Customers by CLV ---")
    print("-" * 85)
    print(f"{'User ID':<15} {'Orders':<8} {'Total Spent':<15} {'Avg Order':<12} {'CLV Score':<12} {'Segment':<10}")
    print("-" * 85)
    
    for c in top_customers:
        print(f"{c['_id']:<15} {c['order_count']:<8} ${c['total_spent']:>11,.2f} ${c['avg_order_value']:>9,.2f} {c['clv_score']:>10,.2f} {c['customer_segment']:<10}")
    
    # --- Customer Segment Summary ---
    print("\n--- Customer Segments Summary ---")
    
    segment_pipeline = [
        {"$group": {
            "_id": "$user_id",
            "total_spent": {"$sum": "$total"},
            "order_count": {"$sum": 1},
            "avg_order_value": {"$avg": "$total"}
        }},
        {"$addFields": {
            "clv_score": {"$multiply": ["$avg_order_value", "$order_count"]},
            "segment": {
                "$switch": {
                    "branches": [
                        {"case": {"$gte": [{"$multiply": ["$avg_order_value", "$order_count"]}, 1000]}, "then": "Platinum"},
                        {"case": {"$gte": [{"$multiply": ["$avg_order_value", "$order_count"]}, 500]}, "then": "Gold"},
                        {"case": {"$gte": [{"$multiply": ["$avg_order_value", "$order_count"]}, 100]}, "then": "Silver"}
                    ],
                    "default": "Bronze"
                }
            }
        }},
        {"$group": {
            "_id": "$segment",
            "customer_count": {"$sum": 1},
            "total_revenue": {"$sum": "$total_spent"},
            "avg_clv": {"$avg": "$clv_score"}
        }},
        {"$sort": {"total_revenue": -1}}
    ]
    
    segments = list(mongo_db.transactions.aggregate(segment_pipeline))
    
    print("-" * 65)
    print(f"{'Segment':<12} {'Customers':<12} {'Total Revenue':<18} {'Avg CLV':<15}")
    print("-" * 65)
    
    for s in segments:
        print(f"{s['_id']:<12} {s['customer_count']:<12} ${s['total_revenue']:>14,.2f} ${s['avg_clv']:>11,.2f}")
    
    return top_customers


# ============================================================
# ANALYSIS 2: FUNNEL CONVERSION ANALYSIS
# ============================================================
def funnel_conversion_analysis(hbase_conn):
    """
    Analyze the conversion funnel from browsing to purchase.
    
    Data Sources:
    - HBase: Session data (browsing behavior, conversion status)
    """
    
    print("\n" + "=" * 70)
    print("ANALYSIS 2: FUNNEL CONVERSION ANALYSIS")
    print("=" * 70)
    print("\nData Sources: HBase (sessions)")
    print("Business Question: Where do we lose customers in the buying journey?\n")
    
    if not hbase_conn:
        print(" HBase not available, skipping funnel analysis")
        return None
    
    print("Fetching session data from HBase...")
    
    total_sessions = 0
    sessions_with_views = 0
    sessions_with_cart = 0
    converted_sessions = 0
    
    device_stats = {}
    referrer_stats = {}
    status_stats = {}
    
    table = hbase_conn.table('sessions')
    
    for key, data in table.scan(limit=50000):
        total_sessions += 1
        
        status = data.get(b'session_info:conversion_status', b'').decode()
        device = data.get(b'device:type', b'').decode()
        referrer = data.get(b'session_info:referrer', b'').decode()
        page_views = int(data.get(b'activity:page_views_count', b'0').decode() or 0)
        
        if page_views > 1:
            sessions_with_views += 1
        
        if status in ['abandoned', 'converted']:
            sessions_with_cart += 1
        
        if status == 'converted':
            converted_sessions += 1
        
        # Device stats
        if device not in device_stats:
            device_stats[device] = {'total': 0, 'converted': 0}
        device_stats[device]['total'] += 1
        if status == 'converted':
            device_stats[device]['converted'] += 1
        
        # Referrer stats
        if referrer not in referrer_stats:
            referrer_stats[referrer] = {'total': 0, 'converted': 0}
        referrer_stats[referrer]['total'] += 1
        if status == 'converted':
            referrer_stats[referrer]['converted'] += 1
        
        # Status stats
        if status not in status_stats:
            status_stats[status] = 0
        status_stats[status] += 1
    
    print(f"   Analyzed {total_sessions:,} sessions")
    
    # Display Funnel
    print(f"""
    ┌─────────────────────────────────────────────────────────────┐
    │                    CONVERSION FUNNEL                        │
    ├─────────────────────────────────────────────────────────────┤
    │  Stage 1: Sessions Started      │ {total_sessions:>10,} │ 100.0%   │
    │  Stage 2: Products Viewed       │ {sessions_with_views:>10,} │ {sessions_with_views/total_sessions*100:>5.1f}%   │
    │  Stage 3: Added to Cart         │ {sessions_with_cart:>10,} │ {sessions_with_cart/total_sessions*100:>5.1f}%   │
    │  Stage 4: Completed Purchase    │ {converted_sessions:>10,} │ {converted_sessions/total_sessions*100:>5.1f}%   │
    └─────────────────────────────────────────────────────────────┘
    """)
    
    # Conversion by Device
    print("--- Conversion Rate by Device Type ---")
    print("-" * 50)
    print(f"{'Device':<12} {'Sessions':<12} {'Conversions':<12} {'Rate':<10}")
    print("-" * 50)
    
    for device, stats in sorted(device_stats.items(), key=lambda x: -x[1]['converted']/max(x[1]['total'],1)):
        rate = (stats['converted'] / stats['total'] * 100) if stats['total'] > 0 else 0
        print(f"{device:<12} {stats['total']:<12} {stats['converted']:<12} {rate:.2f}%")
    
    # Conversion by Referrer
    print("\n--- Conversion Rate by Referrer Source ---")
    print("-" * 55)
    print(f"{'Referrer':<15} {'Sessions':<12} {'Conversions':<12} {'Rate':<10}")
    print("-" * 55)
    
    for ref, stats in sorted(referrer_stats.items(), key=lambda x: -x[1]['converted']/max(x[1]['total'],1)):
        rate = (stats['converted'] / stats['total'] * 100) if stats['total'] > 0 else 0
        print(f"{ref:<15} {stats['total']:<12} {stats['converted']:<12} {rate:.2f}%")
    
    return {'total': total_sessions, 'converted': converted_sessions}


# ============================================================
# ANALYSIS 3: INTEGRATED BUSINESS DASHBOARD
# ============================================================
def integrated_dashboard(mongo_db, hbase_conn):
    """
    Create an integrated business dashboard combining all data sources.
    """
    
    print("\n" + "=" * 70)
    print("ANALYSIS 3: INTEGRATED BUSINESS DASHBOARD")
    print("=" * 70)
    print("\nData Sources: MongoDB + HBase")
    print("Business Question: What's the overall health of our business?\n")
    
    # MongoDB Metrics
    print("Fetching MongoDB metrics...")
    
    revenue_pipeline = [
        {"$group": {
            "_id": None,
            "total_revenue": {"$sum": "$total"},
            "total_transactions": {"$sum": 1},
            "avg_order_value": {"$avg": "$total"},
            "total_discount": {"$sum": "$discount"}
        }}
    ]
    revenue_data = list(mongo_db.transactions.aggregate(revenue_pipeline))[0]
    
    unique_customers = len(mongo_db.transactions.distinct("user_id"))
    total_users = mongo_db.users.count_documents({})
    total_products = mongo_db.products.count_documents({})
    active_products = mongo_db.products.count_documents({"is_active": True})
    
    # HBase Metrics
    print("Fetching HBase metrics...")
    
    session_count = 0
    converted_count = 0
    total_duration = 0
    
    if hbase_conn:
        table = hbase_conn.table('sessions')
        for key, data in table.scan(limit=100000):
            session_count += 1
            if data.get(b'session_info:conversion_status', b'').decode() == 'converted':
                converted_count += 1
            total_duration += int(data.get(b'session_info:duration', b'0').decode() or 0)
    
    avg_duration = total_duration / session_count if session_count > 0 else 0
    conv_rate = (converted_count / session_count * 100) if session_count > 0 else 0
    
    # Display Dashboard
    print(f"""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                    E-COMMERCE BUSINESS DASHBOARD                     ║
    ╠══════════════════════════════════════════════════════════════════════╣
    ║                                                                      ║
    ║   REVENUE METRICS (from MongoDB)                                   ║
    ║  ─────────────────────────────────────────────────────────────────   ║
    ║  Total Revenue:          ${revenue_data['total_revenue']:>15,.2f}                      ║
    ║  Total Transactions:      {revenue_data['total_transactions']:>15,}                      ║
    ║  Average Order Value:    ${revenue_data['avg_order_value']:>15,.2f}                      ║
    ║  Total Discounts Given:  ${revenue_data['total_discount']:>15,.2f}                      ║
    ║                                                                      ║
    ║   USER METRICS (from MongoDB)                                      ║
    ║  ─────────────────────────────────────────────────────────────────   ║
    ║  Total Registered Users:  {total_users:>15,}                      ║
    ║  Unique Customers:        {unique_customers:>15,}                      ║
    ║                                                                      ║
    ║   PRODUCT METRICS (from MongoDB)                                   ║
    ║  ─────────────────────────────────────────────────────────────────   ║
    ║  Total Products:          {total_products:>15,}                      ║
    ║  Active Products:         {active_products:>15,}                      ║
    ║                                                                      ║
    ║   ENGAGEMENT METRICS (from HBase)                                  ║
    ║  ─────────────────────────────────────────────────────────────────   ║
    ║  Total Sessions:          {session_count:>15,}                      ║
    ║  Converted Sessions:      {converted_count:>15,}                      ║
    ║  Conversion Rate:         {conv_rate:>14.2f}%                      ║
    ║  Avg Session Duration:    {avg_duration:>12.0f} sec                      ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    return revenue_data


# ============================================================
# MAIN FUNCTION
# ============================================================
def main():
    """Run all integrated analytics"""
    
    print("\n" + "=" * 70)
    print("   INTEGRATED ANALYTICS: MongoDB + HBase")
    print("=" * 70 + "\n")
    
    # Connect to systems
    print("Connecting to data systems...\n")
    
    mongo_client, mongo_db = connect_mongodb()
    hbase_conn = connect_hbase()
    
    print()
    
    # Run Analyses
    clv_results = customer_lifetime_value(mongo_db)
    funnel_results = funnel_conversion_analysis(hbase_conn)
    dashboard = integrated_dashboard(mongo_db, hbase_conn)
    
    # Summary
    print("\n" + "=" * 70)
    print("INTEGRATED ANALYTICS COMPLETE")
    print("=" * 70)
    print(" Customer Lifetime Value analysis completed")
    print(" Funnel Conversion analysis completed")
    print(" Integrated Dashboard generated")
    print("\nThis demonstrates how MongoDB and HBase work together")
    print("to provide comprehensive business insights.")
    
    # Cleanup
    mongo_client.close()
    if hbase_conn:
        hbase_conn.close()
    
    print("\n All connections closed")


if __name__ == "__main__":
    main()