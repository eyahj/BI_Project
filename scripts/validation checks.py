import pandas as pd

# Load cleaned data from CSV files
customers = pd.read_csv('data/processed/customers_cleaned.csv')
products = pd.read_csv('data/processed/products_cleaned.csv')
time = pd.read_csv('data/processed/time_cleaned.csv')
shipping = pd.read_csv('data/processed/shipping_cleaned.csv')
sales = pd.read_csv('data/processed/sales_cleaned.csv')

# ---- 1. Check Foreign Key Integrity ----
print("\nChecking Foreign Key Integrity...")

# Ensure all foreign keys in sales exist in respective dimension tables
missing_customers = sales[~sales['Customer_ID'].isin(customers['Customer_ID'])]
missing_products = sales[~sales['Product_ID'].isin(products['Product_ID'])]
missing_time = sales[~sales['TimeID'].isin(time['TimeID'])]
missing_shipping = sales[~sales['Order_ID'].isin(shipping['Order_ID'])]

# Print issues if any
if not missing_customers.empty:
    print(f"❌ {len(missing_customers)} orphaned CustomerID(s) found in Sales.")
if not missing_products.empty:
    print(f"❌ {len(missing_products)} orphaned ProductID(s) found in Sales.")
if not missing_time.empty:
    print(f"❌ {len(missing_time)} orphaned TimeID(s) found in Sales.")
if not missing_shipping.empty:
    print(f"❌ {len(missing_shipping)} orphaned ShippingID(s) found in Sales.")

# ---- 2. Check Uniqueness Constraints ----
print("\nChecking Uniqueness Constraints...")

# Ensure primary keys in dimension tables are unique
def check_uniqueness(df, key, name):
    if df[key].duplicated().sum() > 0:
        print(f"❌ Duplicate {key} values found in {name}.")
    else:
        print(f"✅ {name} primary key is unique.")

check_uniqueness(customers, 'Customer_ID', 'Customers')
check_uniqueness(products, 'Product_ID', 'Products')
check_uniqueness(time, 'TimeID', 'Time')
check_uniqueness(shipping, 'Order_ID', 'Shipping')

# ---- 3. Check Data Type Consistency ----
print("\nChecking Data Type Consistency...")

# Ensure numerical fields are numeric
num_cols = ['Sales', 'Profit', 'Quantity', 'Discount']
for col in num_cols:
    if not pd.api.types.is_numeric_dtype(sales[col]):
        print(f"❌ {col} column in Sales contains non-numeric values.")

# Ensure Date fields are properly formatted
try:
    time['OrderDate'] = pd.to_datetime(time['OrderDate'])
    print("✅ OrderDate in Time table is in correct datetime format.")
except Exception as e:
    print(f"❌ OrderDate in Time table has incorrect format: {e}")

try:
    shipping['Ship_Date'] = pd.to_datetime(shipping['Ship_Date'])
    print("✅ Ship_Date in Shipping table is in correct datetime format.")
except Exception as e:
    print(f"❌ Ship_Date in Shipping table has incorrect format: {e}")

# ---- 4. Check Data Consistency ----
print("\nChecking Data Consistency...")

# Validate Region Mapping
if 'Grouped_Region' in customers.columns:
    if customers['Grouped_Region'].isna().sum() > 0:
        print(f"❌ Some customers have missing Grouped_Region values.")
    else:
        print("✅ All customers have a valid Grouped_Region assigned.")

# Validate Logical Consistency (Profit ≤ Sales)
invalid_profit = sales[sales['Profit'] > sales['Sales']]
if not invalid_profit.empty:
    print(f"❌ {len(invalid_profit)} records have Profit greater than Sales.")

print("\nValidation Completed.")

# identify duplicate values in customers and shipping dimensions and delete them while keeping first occurences
duplicate_customers = customers[customers.duplicated(subset=['Customer_ID'], keep=False)]
print(duplicate_customers)
customers = customers.drop_duplicates(subset=['Customer_ID'], keep='first')

duplicate_orders = shipping[shipping.duplicated(subset=['Order_ID'], keep=False)]
print(duplicate_orders)
shipping = shipping.drop_duplicates(subset=['Order_ID'], keep='first')

print(f"Remaining rows in Customers: {customers.shape[0]}")
print(f"Remaining rows in Shipping: {shipping.shape[0]}")

# checking for missing values or Nulls
print("\nChecking for missing values...")
print(customers.isnull().sum())  # Check missing values in Customers
print(products.isnull().sum())   # Check missing values in Products
print(time.isnull().sum())       # Check missing values in Time
print(shipping.isnull().sum())   # Check missing values in Shipping
print(sales.isnull().sum())      # Check missing values in Sales

# checking for foreign key uniqueness after removing duplicates
missing_customers = sales[~sales['Customer_ID'].isin(customers['Customer_ID'])]
missing_orders = sales[~sales['Order_ID'].isin(shipping['Order_ID'])]

if not missing_customers.empty:
    print(f"❌ {len(missing_customers)} orphaned Customer_ID(s) remain in Sales after cleaning.")
if not missing_orders.empty:
    print(f"❌ {len(missing_orders)} orphaned Order_ID(s) remain in Sales after cleaning.")

# Checking date ranges 
print(f"Time Range in Time Table: {time['OrderDate'].min()} to {time['OrderDate'].max()}")
print(f"Shipping Date Range: {shipping['Ship_Date'].min()} to {shipping['Ship_Date'].max()}")

# Check that sales fact only contains unique values
duplicate_sales = sales[sales.duplicated(subset=['Order_ID', 'Product_ID', 'Customer_ID', 'TimeID'], keep=False)]

if not duplicate_sales.empty:
    print(f"❌ {len(duplicate_sales)} duplicate records found in Sales based on (Order_ID, Product_ID, Customer_ID, TimeID).")
else:
    print("✅ Sales fact table contains only unique records based on all foreign keys.")

# Remove duplicates found
sales = sales.drop_duplicates(subset=['Order_ID', 'Product_ID', 'Customer_ID', 'TimeID'], keep='first')

# Verify after cleaning
print(f"Remaining rows in Sales after deduplication: {sales.shape[0]}")

if sales.duplicated(subset=['Order_ID', 'Product_ID', 'Customer_ID', 'TimeID']).sum() == 0:
    print("✅ Sales table is now completely unique.")
else:
    print("❌ Some duplicates still exist.")

# Save the cleaned data back to CSV files
customers.to_csv('data/processed/customers_cleaned.csv', index=False)
products.to_csv('data/processed/products_cleaned.csv', index=False)
time.to_csv('data/processed/time_cleaned.csv', index=False)
shipping.to_csv('data/processed/shipping_cleaned.csv', index=False)
sales.to_csv('data/processed/sales_cleaned.csv', index=False)

print("\n✅ All cleaned data has been successfully saved to the processed folder.")
