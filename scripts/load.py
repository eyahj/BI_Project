import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables (optional)
load_dotenv()
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '0000')
DB_NAME = os.getenv('DB_NAME', 'transformed_data')

# Mapping for column names
column_mappings = {
    'Customer_Dim': {
        'Customer_ID': 'Customer_ID',
        'Customer_Name': 'Customer_Name',
        'Segment': 'Segment',
        'City': 'City',
        'State': 'State',
        'Country': 'Country',
        'Region': 'Region',
        'Grouped_Region': 'Grouped_Region'
    },
    'Product_Dim': {
        'Product_ID': 'Product_ID',
        'Product_Name': 'Product_Name',
        'Category': 'Category',
        'Sub_Category': 'Sub_Category'
    },
    'Time_Dim': {
        'TimeID': 'TimeID',
        'OrderDate': 'OrderDate',
        'OrderYear': 'OrderYear',
        'OrderMonth': 'OrderMonth',
        'DayOfWeek': 'DayOfWeek',
        'OrderQuarter': 'OrderQuarter'
    },
    'Shipping_Dim': {
        'Order_ID': 'Order_ID',
        'Ship_Date': 'Ship_Date',
        'Ship_Mode': 'Ship_Mode',
        'Delivery_Days': 'Delivery_Days',
        'Shipping_Cost': 'Shipping_Cost',
        'Shipping_Cost_Category': 'Shipping_Cost_Category',
        'Ship_Year': 'Ship_Year',
        'Ship_Month':'Ship_Month',
        'Ship_Day': 'Ship_Day'
    },
    'Sales_Fact': {
        'Order_ID': 'Order_ID',
        'Product_ID': 'Product_ID',
        'Customer_ID': 'Customer_ID',
        'TimeID': 'TimeID',
        'Sales': 'Sales',
        'Profit': 'Profit',
        'Quantity': 'Quantity',
        'Discount': 'Discount'
    }
}

def create_connection():
    """Create a MySQL database connection."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            print("✅ Connected to MySQL database")
            return connection
    except Error as e:
        print(f"❌ Error: {e}")
        return None

def load_data(connection, table_name, csv_file):
    """Load data from CSV into MySQL table."""
    try:
        cursor = connection.cursor()
        df = pd.read_csv(csv_file)

        # Rename columns based on mappings
        if table_name in column_mappings:
            df.rename(columns=column_mappings[table_name], inplace=True)

        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in df.to_numpy()]

        # Generate column names and placeholders
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))

        # Insert query with IGNORE to handle duplicates
        insert_sql = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute query
        cursor.executemany(insert_sql, data)
        connection.commit()

        print(f"✅ Data loaded successfully into {table_name}")
    except Error as e:
        print(f"❌ Error loading data into {table_name}: {e}")
    finally:
        cursor.close()

def main():
    """Main function to connect and load data."""
    connection = create_connection()
    if connection:
        try:
            load_data(connection, 'Customer_Dim', 'data/processed/customers_cleaned.csv')
            load_data(connection, 'Product_Dim', 'data/processed/products_cleaned.csv')
            load_data(connection, 'Time_Dim', 'data/processed/time_cleaned.csv')
            load_data(connection, 'Shipping_Dim', 'data/processed/shipping_cleaned.csv')
            load_data(connection, 'Sales_Fact', 'data/processed/sales_cleaned.csv')
        finally:
            connection.close()
            print("✅ Database connection closed")

if __name__ == "__main__":
    main()
