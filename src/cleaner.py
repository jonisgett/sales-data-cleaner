import pandas as pd

#import data from a csv file into a list of dictionaries
def import_csv(filepath):
    df = pd.read_csv(filepath)
    df = df.fillna("")
    return df.to_dict("records")
#import data from a json file into a list of dictionaries
def import_json(filepath):
    df = pd.read_json(filepath)
    df = df.fillna("")
    return df.to_dict("records")
#import data from an xlsx file into a list of dictionaries
def import_excel(filepath):
    df = pd.read_excel(filepath)
    df = df.fillna("")
    return df.to_dict("records")
#paths
filepath_csv = "data/raw/sales_q1.csv"
filepath_json = "data/raw/sales_q2.json"
filepath_xl = "data/raw/sales_q3.xlsx"

#call functions to get our list of dictionaries 
csv_data = import_csv(filepath_csv)
json_data = import_json(filepath_json)
xls_data = import_excel(filepath_xl)

#dictionary for creating standard column names
standardize_column_names = {"order_id": ["Order_ID", "Order ID", "order_id"], "customer_name": ["CustomerName", "Customer_Name", "customer name" ], "product": ["PRODUCT", "Product", "product"],
                            "quantity": ["Quantity", "qty", "quantity"], "unit_price": ["Price", "Unit Price", "price", "unit_price"], "order_date": ["Date", "Purchase Date", "order_date"]}

#reverse the dictionary for easy lookup - because our standardized_column_names dictionary could have infinitely many values (dirty_keys)
reverse_lookup = {dirty_key: standard_key
                   for standard_key, dirty_keys in standardize_column_names.items() 
                   for dirty_key in dirty_keys}
#get our files with cleaned headers - dictionary key values
cleaned_csv = [{reverse_lookup.get(key, key): value for key, value in record.items()}
                for record in csv_data]
cleaned_json = [{reverse_lookup.get(key, key): value for key, value in record.items()}
                 for record in json_data]
cleaned_xls = [{reverse_lookup.get(key, key): value for key, value in record.items()}
                for record in xls_data]
#we can go ahead and make one large list of dictionaries at this point since we have all the keys cleaned.
combined_file_init_clean = cleaned_csv + cleaned_json + cleaned_xls
#remove records(dictionaires that don't have order_id or product.  Then clean the rest of the data by fixing customer_name)

#function for standardizing dates to YYYY-MM-DD
def standardize_date(date_value):
    if not date_value:
        return None
    
    try:
        dt = pd.to_datetime(date_value)
        return dt.strftime('%Y-%m-%d')
    except:
        return None

combined_file_final_clean = []
for record in combined_file_init_clean:
    #If there is no order_id, product, or price we will not bring the record into the cleaned list
    if not record.get("order_id") or not record.get("product") or not record.get("unit_price"):
        continue
    #If price is not valid we will not bring the record into the cleaned list
    try:
        price = float(record.get("unit_price"))
        if price <= 0:
            continue
    except (ValueError, TypeError):
        continue
    #1st let's deal with all the possible Customer errors.  
    #We don't even have the data in the record (no key, value pair in the dictionary), or the value is falsy, then clean.
    record["customer_name"] = str((record.get("customer_name", ""))).strip().title() or "Unknown Customer"
    #Deal with similar issues for quantity..key doesn't exit or the value is falsy, then clean
    try:
        record["quantity"] = int(record.get("quantity") or 0)
    except (ValueError, TypeError):
        record["quantity"] = 0
    #Deal with the price
    record["unit_price"] = price
    record["order_date"] = standardize_date(record.get("order_date"))
    #Add the record
    combined_file_final_clean.append(record)

print(f"Total records in raw data: {len(combined_file_init_clean)}")
print(f"Total records after cleaning: {len(combined_file_final_clean)}")
print(f"Records removed: {len(combined_file_init_clean) - len(combined_file_final_clean)}")

print("\n" + "="*50)
print("First 3 cleaned records:")
print("="*50)
for record in combined_file_final_clean[:3]:
    print(record)

output_df = pd.DataFrame(combined_file_final_clean)
output_df.to_csv('data/processed/cleaned_sales.csv', index=False)
print("\n✅ Cleaned data exported to: data/processed/cleaned_sales.csv")

# get unique products from each of the files
csv_products = {record["product"] for record in cleaned_csv if record.get("product")}
json_products = {record["product"] for record in cleaned_json if record.get("product")}
xls_products = {record["product"] for record in cleaned_xls if record.get("product")}

# All unique products would be the union of all 3 sets
all_products = csv_products | json_products | xls_products

# The products that are in each of the 3 files (intersection)
common_products = csv_products & json_products & xls_products

# The products that are "unique" to each of the 3 files
unique_csv_products = csv_products - json_products - xls_products
unique_json_products = json_products - csv_products - xls_products
unique_xls_products = xls_products - csv_products - json_products

# Unique Customers accross the entire cleaned joined file
all_customers = {record["customer_name"] for record in combined_file_final_clean if record.get("customer_name")}

# Print set operation results
print("\n" + "="*60)
print("SET OPERATIONS - Product Analysis")
print("="*60)
print(f"Total unique products across all files: {len(all_products)}")
print(f"All products: {sorted(all_products)}\n")

print(f"Products in ALL three files: {len(common_products)}")
print(f"Common products: {sorted(common_products)}\n")

print(f"Products ONLY in CSV: {len(unique_csv_products)}")
print(f"CSV-only products: {sorted(unique_csv_products)}\n")

print(f"Products ONLY in JSON: {len(unique_json_products)}")
print(f"JSON-only products: {sorted(unique_json_products)}\n")

print(f"Products ONLY in Excel: {len(unique_xls_products)}")
print(f"Excel-only products: {sorted(unique_xls_products)}\n")

print(f"Total unique customers: {len(all_customers)}")
print(f"Customer list: {sorted(all_customers)}")

product_stats = []
for product in all_products:
    total_quantity = sum([record["quantity"] for record in combined_file_final_clean if record.get("quantity") and record["product"] == product])
    total_revenue = sum([(record["quantity"] * record["unit_price"]) for record in combined_file_final_clean if record.get("quantity") and record.get("unit_price") and record["product"] == product])
    product_stats.append((product, total_quantity, total_revenue))

# Sort by revenue (highest first)
product_stats.sort(key=lambda x: x[2], reverse=True)

print("\n" + "="*60)
print("PRODUCT REVENUE STATISTICS")
print("="*60)
print(f"{'Product':<15} | {'Quantity':>8} | {'Revenue':>12}")
print("-" * 60)
for product, qty, revenue in product_stats:
    print(f"{product:<15} | {qty:>8} | ${revenue:>11,.2f}")

    # ============================================
# TOP CUSTOMERS
# ============================================

customer_stats = []
for customer in all_customers:
    order_count = sum(1 for record in combined_file_final_clean if record["customer_name"] == customer)
    total_spent = sum(record["quantity"] * record["unit_price"] 
                     for record in combined_file_final_clean 
                     if record["customer_name"] == customer)
    
    customer_stats.append((customer, order_count, total_spent))

# Sort by total spent (highest first) and take top 5
customer_stats.sort(key=lambda x: x[2], reverse=True)
top_5_customers = customer_stats[:5]

print("\n" + "="*60)
print("TOP 5 CUSTOMERS")
print("="*60)
print(f"{'Customer':<20} | {'Orders':>7} | {'Total Spent':>12}")
print("-" * 60)
for customer, orders, spent in top_5_customers:
    print(f"{customer:<20} | {orders:>7} | ${spent:>11,.2f}")





        

        
