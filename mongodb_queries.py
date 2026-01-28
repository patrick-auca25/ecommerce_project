# mongodb_queries.py
# MongoDB Aggregation Pipelines for E-commerce Analytics

from pymongo import MongoClient
import json

# --- MongoDB Connection ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_USER = "admin"
MONGO_PASSWORD = "password123"
DATABASE_NAME = "ecommerce"

def connect_to_mongodb():
    """Connect to MongoDB with authentication"""
    connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    client = MongoClient(connection_string)
    db = client[DATABASE_NAME]
    print(" Connected to MongoDB\n")
    return client, db


# ============================================================
# AGGREGATION 1: Top-Selling Products
# ============================================================
def top_selling_products(db, limit=10):
    """
    Find the top-selling products by total quantity sold.
    
    Pipeline steps:
    1. $unwind - Flatten the items array (each item becomes a document)
    2. $group - Group by product_id, sum quantities and revenue
    3. $sort - Sort by quantity descending
    4. $limit - Get top N products
    5. $lookup - Join with products collection to get product names
    """
    
    print("=" * 60)
    print("AGGREGATION 1: Top-Selling Products")
    print("=" * 60)
    print("Business Question: What are our best-selling products?\n")
    
    pipeline = [
        # Step 1: Unwind the items array
        {"$unwind": "$items"},
        
        # Step 2: Group by product_id and calculate totals
        {"$group": {
            "_id": "$items.product_id",
            "total_quantity_sold": {"$sum": "$items.quantity"},
            "total_revenue": {"$sum": "$items.subtotal"},
            "number_of_orders": {"$sum": 1}
        }},
        
        # Step 3: Sort by quantity sold (descending)
        {"$sort": {"total_quantity_sold": -1}},
        
        # Step 4: Limit to top N
        {"$limit": limit},
        
        # Step 5: Join with products collection to get product details
        {"$lookup": {
            "from": "products",
            "localField": "_id",
            "foreignField": "product_id",
            "as": "product_info"
        }},
        
        # Step 6: Unwind product_info (converts array to object)
        {"$unwind": "$product_info"},
        
        # Step 7: Project final fields
        {"$project": {
            "_id": 0,
            "product_id": "$_id",
            "product_name": "$product_info.name",
            "category_id": "$product_info.category_id",
            "total_quantity_sold": 1,
            "total_revenue": {"$round": ["$total_revenue", 2]},
            "number_of_orders": 1
        }}
    ]
    
    results = list(db.transactions.aggregate(pipeline))
    
    print(f"Top {limit} Selling Products:")
    print("-" * 60)
    print(f"{'Rank':<5} {'Product Name':<30} {'Qty Sold':<10} {'Revenue':<12}")
    print("-" * 60)
    
    for i, product in enumerate(results, 1):
        name = product['product_name'][:28] if len(product['product_name']) > 28 else product['product_name']
        print(f"{i:<5} {name:<30} {product['total_quantity_sold']:<10} ${product['total_revenue']:,.2f}")
    
    print()
    return results


# ============================================================
# AGGREGATION 2: Revenue by Category
# Business Question: Which product categories generate the most revenue?
# ============================================================
def revenue_by_category(db):
    """
    Calculate total revenue for each product category.
    
    Pipeline steps:
    1. $unwind - Flatten items array
    2. $lookup - Join with products to get category_id
    3. $unwind - Flatten the lookup result
    4. $lookup - Join with categories to get category name
    5. $unwind - Flatten categories
    6. $group - Group by category and sum revenue
    7. $sort - Sort by revenue descending
    """
    
    print("=" * 60)
    print("AGGREGATION 2: Revenue by Category")
    print("=" * 60)
    print("Business Question: Which categories generate the most revenue?\n")
    
    pipeline = [
        # Step 1: Unwind items array
        {"$unwind": "$items"},
        
        # Step 2: Lookup product details
        {"$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "product"
        }},
        
        # Step 3: Unwind product
        {"$unwind": "$product"},
        
        # Step 4: Lookup category details
        {"$lookup": {
            "from": "categories",
            "localField": "product.category_id",
            "foreignField": "category_id",
            "as": "category"
        }},
        
        # Step 5: Unwind category
        {"$unwind": "$category"},
        
        # Step 6: Group by category
        {"$group": {
            "_id": {
                "category_id": "$category.category_id",
                "category_name": "$category.name"
            },
            "total_revenue": {"$sum": "$items.subtotal"},
            "total_items_sold": {"$sum": "$items.quantity"},
            "number_of_transactions": {"$sum": 1}
        }},
        
        # Step 7: Sort by revenue descending
        {"$sort": {"total_revenue": -1}},
        
        # Step 8: Project final fields
        {"$project": {
            "_id": 0,
            "category_id": "$_id.category_id",
            "category_name": "$_id.category_name",
            "total_revenue": {"$round": ["$total_revenue", 2]},
            "total_items_sold": 1,
            "number_of_transactions": 1
        }}
    ]
    
    results = list(db.transactions.aggregate(pipeline))
    
    print(f"Revenue by Category (All {len(results)} categories):")
    print("-" * 70)
    print(f"{'Rank':<5} {'Category':<25} {'Revenue':<15} {'Items Sold':<12} {'Orders':<10}")
    print("-" * 70)
    
    for i, cat in enumerate(results, 1):
        name = cat['category_name'][:23] if len(cat['category_name']) > 23 else cat['category_name']
        print(f"{i:<5} {name:<25} ${cat['total_revenue']:>12,.2f} {cat['total_items_sold']:<12} {cat['number_of_transactions']:<10}")
    
    # Calculate total
    total_revenue = sum(cat['total_revenue'] for cat in results)
    print("-" * 70)
    print(f"{'TOTAL':<31} ${total_revenue:>12,.2f}")
    print()
    
    return results


# ============================================================
# AGGREGATION 3: User Segmentation by Purchasing Frequency
# Business Question: How do we categorize customers by their buying behavior?
# ============================================================
def user_segmentation_by_frequency(db):
    """
    Segment users by how frequently they purchase.
    
    Segments:
    - One-time buyers: 1 purchase
    - Occasional buyers: 2-5 purchases
    - Regular buyers: 6-15 purchases
    - Loyal customers: 16+ purchases
    
    Pipeline steps:
    1. $group - Group by user_id, count transactions, sum spending
    2. $bucket - Categorize into frequency segments
    3. $project - Format output
    """
    
    print("=" * 60)
    print("AGGREGATION 3: User Segmentation by Purchasing Frequency")
    print("=" * 60)
    print("Business Question: How do we categorize customers by buying behavior?\n")
    
    pipeline = [
        # Step 1: Group by user to get purchase stats
        {"$group": {
            "_id": "$user_id",
            "purchase_count": {"$sum": 1},
            "total_spent": {"$sum": "$total"},
            "avg_order_value": {"$avg": "$total"}
        }},
        
        # Step 2: Bucket into segments
        {"$bucket": {
            "groupBy": "$purchase_count",
            "boundaries": [1, 2, 6, 16, 1000],  # 1, 2-5, 6-15, 16+
            "default": "Other",
            "output": {
                "customer_count": {"$sum": 1},
                "total_revenue": {"$sum": "$total_spent"},
                "avg_purchases_per_customer": {"$avg": "$purchase_count"},
                "avg_customer_value": {"$avg": "$total_spent"}
            }
        }},
        
        # Step 3: Add segment labels
        {"$project": {
            "_id": 0,
            "segment": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$_id", 1]}, "then": "One-time Buyers (1)"},
                        {"case": {"$eq": ["$_id", 2]}, "then": "Occasional (2-5)"},
                        {"case": {"$eq": ["$_id", 6]}, "then": "Regular (6-15)"},
                        {"case": {"$eq": ["$_id", 16]}, "then": "Loyal (16+)"}
                    ],
                    "default": "Other"
                }
            },
            "customer_count": 1,
            "total_revenue": {"$round": ["$total_revenue", 2]},
            "avg_purchases_per_customer": {"$round": ["$avg_purchases_per_customer", 1]},
            "avg_customer_value": {"$round": ["$avg_customer_value", 2]}
        }}
    ]
    
    results = list(db.transactions.aggregate(pipeline))
    
    print("Customer Segments by Purchase Frequency:")
    print("-" * 75)
    print(f"{'Segment':<22} {'Customers':<12} {'Revenue':<15} {'Avg Purchases':<15} {'Avg Value':<12}")
    print("-" * 75)
    
    total_customers = 0
    total_revenue = 0
    
    for seg in results:
        total_customers += seg['customer_count']
        total_revenue += seg['total_revenue']
        print(f"{seg['segment']:<22} {seg['customer_count']:<12} ${seg['total_revenue']:>12,.2f} {seg['avg_purchases_per_customer']:<15} ${seg['avg_customer_value']:>9,.2f}")
    
    print("-" * 75)
    print(f"{'TOTAL':<22} {total_customers:<12} ${total_revenue:>12,.2f}")
    print()
    
    return results


# ============================================================
# MAIN FUNCTION
# ============================================================
def main():
    """Run all aggregation queries"""
    
    print("\n" + "=" * 60)
    print("   MONGODB AGGREGATION QUERIES - E-COMMERCE ANALYTICS")
    print("=" * 60 + "\n")
    
    # Connect
    client, db = connect_to_mongodb()
    
    # Run aggregations
    top_products = top_selling_products(db, limit=10)
    category_revenue = revenue_by_category(db)
    user_segments = user_segmentation_by_frequency(db)
    
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f" Top-selling products query completed ({len(top_products)} results)")
    print(f" Revenue by category query completed ({len(category_revenue)} results)")
    print(f" User segmentation by frequency completed ({len(user_segments)} results)")
   
    
    # Close connection
    client.close()
    print("\n All queries completed successfully!")


if __name__ == "__main__":
    main()