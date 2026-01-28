import json
from pymongo import MongoClient
from datetime import datetime
import os

# --- MongoDB Connection ---
# Using your docker-compose credentials
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_USER = "admin"
MONGO_PASSWORD = "password123"
DATABASE_NAME = "ecommerce"

def connect_to_mongodb():
    """Connect to MongoDB with authentication"""
    connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    client = MongoClient(connection_string)
    
    # Test connection
    try:
        client.admin.command('ping')
        print(" Connected to MongoDB successfully!")
    except Exception as e:
        print(f" Failed to connect to MongoDB: {e}")
        return None, None
    
    db = client[DATABASE_NAME]
    return client, db

def load_json_file(filepath):
    """Load a JSON file and return its contents"""
    print(f"Loading {filepath}...")
    with open(filepath, 'r') as f:
        data = json.load(f)
    print(f"   Loaded {len(data):,} records")
    return data

def load_categories(db, filepath):
    """Load categories into MongoDB"""
    print("\n--- Loading Categories ---")
    data = load_json_file(filepath)
    
    # Drop existing collection
    db.categories.drop()
    
    # Insert data
    result = db.categories.insert_many(data)
    print(f" Inserted {len(result.inserted_ids):,} categories")
    
    # Create index
    db.categories.create_index("category_id", unique=True)
    print("   Created index on category_id")

def load_products(db, filepath):
    """Load products into MongoDB"""
    print("\n--- Loading Products ---")
    data = load_json_file(filepath)
    
    # Drop existing collection
    db.products.drop()
    
    # Insert data
    result = db.products.insert_many(data)
    print(f" Inserted {len(result.inserted_ids):,} products")
    
    # Create indexes
    db.products.create_index("product_id", unique=True)
    db.products.create_index("category_id")
    db.products.create_index("is_active")
    db.products.create_index("base_price")
    print("   Created indexes on product_id, category_id, is_active, base_price")

def load_users(db, filepath):
    """Load users into MongoDB"""
    print("\n--- Loading Users ---")
    data = load_json_file(filepath)
    
    # Drop existing collection
    db.users.drop()
    
    # Insert data
    result = db.users.insert_many(data)
    print(f" Inserted {len(result.inserted_ids):,} users")
    
    # Create indexes
    db.users.create_index("user_id", unique=True)
    db.users.create_index("geo_data.state")
    db.users.create_index("registration_date")
    print("   Created indexes on user_id, geo_data.state, registration_date")

def load_transactions(db, filepath):
    """Load transactions into MongoDB"""
    print("\n--- Loading Transactions ---")
    data = load_json_file(filepath)
    
    # Drop existing collection
    db.transactions.drop()
    
    # Insert in batches for large datasets
    batch_size = 10000
    total_inserted = 0
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        result = db.transactions.insert_many(batch)
        total_inserted += len(result.inserted_ids)
        print(f"   Inserted batch {i//batch_size + 1}: {total_inserted:,} total")
    
    print(f" Inserted {total_inserted:,} transactions")
    
    # Create indexes
    db.transactions.create_index("transaction_id", unique=True)
    db.transactions.create_index("user_id")
    db.transactions.create_index("session_id")
    db.transactions.create_index("timestamp")
    db.transactions.create_index("status")
    db.transactions.create_index("items.product_id")
    print("   Created indexes on transaction_id, user_id, session_id, timestamp, status, items.product_id")

def main():
    """Main function to load all data"""
    print("=" * 50)
    print("MongoDB Data Loader for E-commerce Project")
    print("=" * 50)
    
    # Connect to MongoDB
    client, db = connect_to_mongodb()
    if db is None:
        return
    
    # Directory containing JSON files 
    data_dir = "raw_data"  
    
    files = {
        "categories": os.path.join(data_dir, "categories.json"),
        "products": os.path.join(data_dir, "products.json"),
        "users": os.path.join(data_dir, "users.json"),
        "transactions": os.path.join(data_dir, "transactions.json")
    }
    
    # Check if files exist
    print("\nChecking files...")
    for name, filepath in files.items():
        if os.path.exists(filepath):
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"    {name}: {filepath} ({size_mb:.2f} MB)")
        else:
            print(f"    {name}: {filepath} NOT FOUND")
            return
    
    # Load data into MongoDB
    load_categories(db, files["categories"])
    load_products(db, files["products"])
    load_users(db, files["users"])
    load_transactions(db, files["transactions"])
    
    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Database: {DATABASE_NAME}")
    print(f"Collections:")
    for collection_name in db.list_collection_names():
        count = db[collection_name].count_documents({})
        print(f"   - {collection_name}: {count:,} documents")
    
    print("\n All data loaded successfully!")
    print("You can view your data at: http://localhost:8081")
    
    # Close connection
    client.close()

if __name__ == "__main__":
    main()