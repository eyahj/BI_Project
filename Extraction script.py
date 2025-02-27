import pandas as pd
from sqlalchemy import create_engine
# Extract data from the staging tables in MySQL

# Database connection parameters
user = 'root'  # Replace with your MySQL username
password = '0000'  # Replace with your MySQL password
host = '127.0.0.1'  # Change if your MySQL server is hosted elsewhere
database = 'bidb'  # Replace with your database name

# Create a connection to the MySQL database
engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

# Define a function to extract data from a table
def extract_data(table_name):
    query = f'SELECT * FROM {table_name}'
    return pd.read_sql(query, engine)

# Extract data from staging tables
customer_data = extract_data('Staging_Customer_Dim')
product_data = extract_data('Staging_Product_Dim')
shipping_data = extract_data('Staging_Shipping_Dim')
time_data = extract_data('Staging_Time_Dim')
sales_data = extract_data('Staging_Sales_Fact')

# Print the extracted data (optional)
print("Customer Data:")
print(customer_data.head())  # Display the first few rows

print("\nProduct Data:")
print(product_data.head())

print("\nShipping Data:")
print(shipping_data.head())

print("\nTime Data:")
print(time_data.head())

print("\nSales Data:")
print(sales_data.head())
