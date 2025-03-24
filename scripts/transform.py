import pandas as pd
from sqlalchemy import create_engine 

def clean_data():
    # Database connection parameters
    user = 'root'  # Replace with your MySQL username
    password = '0000'  # Replace with your MySQL password
    host = '127.0.0.1'  # Change if your MySQL server is hosted elsewhere
    database = 'bidb'  # Replace with your database name

    # Create a connection to the MySQL database
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

    # Load raw data from the MySQL staging area
    products = pd.read_sql('SELECT * FROM Staging_Product_Dim', engine)
    sales = pd.read_sql('SELECT * FROM Staging_Sales_Fact', engine)
    time = pd.read_sql('SELECT * FROM Staging_Time_Dim', engine)
    customers = pd.read_sql('SELECT * FROM Staging_Customer_Dim', engine)
    shipping = pd.read_sql('SELECT * FROM Staging_Shipping_Dim', engine)
    # Function to check and handle missing data
    def handle_missing_data(df, name):
        print(f"Missing values in {name}:")
        print(df.isnull().sum())
        
        # Fill missing values based on column type
        for col in df.columns:
            if df[col].dtype == 'object':  
                mode_value = df[col].mode()[0]  
                df[col] = df[col].fillna(mode_value)
            elif df[col].dtype in ['int64', 'float64']:  
                if df[col].skew() > 1:  
                    median_value = df[col].median()
                    df[col] = df[col].fillna(median_value)
                else:  
                    mean_value = df[col].mean()
                    df[col] = df[col].fillna(mean_value)
        
        print(f"Missing values in {name} after handling:")
        print(df.isnull().sum())
        return df

    # ---- Data Cleaning and transformation ----

    # 1--- cleaning and transforming customers data

    # Remove duplicates
    customers = customers.drop_duplicates()
    # Standardize text
    customers['Customer_Name'] = customers['Customer_Name'].str.title()

    # Handle missing values
    customers = handle_missing_data(customers, 'customers')

    # Trim whitespace
    customers['City'] = customers['City'].str.strip()
    customers['State'] = customers['State'].str.strip()
    customers['Country'] = customers['Country'].str.strip()
    customers['Region'] = customers['Region'].str.strip()

    # Validate 'Segment' column
    valid_segments = ['Consumer', 'Corporate', 'Home Office']
    customers['Segment'] = customers['Segment'].apply(lambda x: x if x in valid_segments else 'Unknown')

    # Standardize 'Country' and 'Region' values
    customers['Country'] = customers['Country'].str.upper()
    customers['Region'] = customers['Region'].str.upper()

    # ---- Grouping Regions ----
    region_mapping = {
        'CANADA': 'NORTH AMERICA',
        'CARIBBEAN': 'CARIBBEAN',
        'CENTRAL AMERICA': 'CENTRAL AMERICA',
        'SOUTH AMERICA': 'SOUTH AMERICA',
        'CENTRAL AFRICA': 'AFRICA',
        'EASTERN AFRICA': 'AFRICA',
        'NORTH AFRICA': 'AFRICA',
        'SOUTHERN AFRICA': 'AFRICA',
        'WESTERN AFRICA': 'AFRICA',
        'CENTRAL ASIA': 'ASIA',
        'EASTERN ASIA': 'ASIA',
        'SOUTHEASTERN ASIA': 'ASIA',
        'SOUTHERN ASIA': 'ASIA',
        'WESTERN ASIA': 'ASIA',
        'EASTERN EUROPE': 'EUROPE',
        'NORTHERN EUROPE': 'EUROPE',
        'SOUTHERN EUROPE': 'EUROPE',
        'WESTERN EUROPE': 'EUROPE',
        'OCEANIA': 'OCEANIA',
        'CENTRAL US': 'NORTH AMERICA',
        'EASTERN US': 'NORTH AMERICA',
        'SOUTHERN US': 'NORTH AMERICA',
        'WESTERN US': 'NORTH AMERICA'
    }

    # Map the regions to the new grouped regions
    customers['Grouped_Region'] = customers['Region'].map(region_mapping).fillna('UNKNOWN')

    # Print the transformed DataFrame
    print(customers.head())


    # Optionally, save the cleaned data
    customers.to_csv('data/processed/customers_cleaned.csv', index=False)

# 2--- cleaning and transforming shipping data
    # Remove duplicates
    shipping = shipping.drop_duplicates()
    # Convert 'Ship_Date' to datetime
    shipping['Ship_Date'] = pd.to_datetime(shipping['Ship_Date'], format='%d-%m-%Y', errors='coerce')

        # Handle missing values
    shipping = handle_missing_data(shipping, 'shipping')

        # Validate 'Ship Mode' column
    valid_ship_modes = ['First Class', 'Second Class', 'Standard Class', 'Same Day']
    shipping['Ship_Mode'] = shipping['Ship_Mode'].apply(lambda x: x if x in valid_ship_modes else 'Unknown')

        # Validate 'Delivery Days' (ensure non-negative)
    shipping['Delivery_Days'] = pd.to_numeric(shipping['Delivery_Days'], errors='coerce')
    shipping['Delivery_Days'] = shipping['Delivery_Days'].clip(lower=0)

        # Validate 'Shipping Cost' (ensure non-negative)
    shipping['Shipping_Cost'] = pd.to_numeric(shipping['Shipping_Cost'], errors='coerce')
    shipping['Shipping_Cost'] = shipping['Shipping_Cost'].clip(lower=0)
    shipping['Shipping_Cost_Category'] = pd.qcut(shipping['Shipping_Cost'], q=3, labels=['Low', 'Medium', 'High'])
    #extract the shipping year month and day from the shipping date
    shipping['Ship_Year'] = shipping['Ship_Date'].dt.year
    shipping['Ship_Month'] = shipping['Ship_Date'].dt.month
    shipping['Ship_Day'] = shipping['Ship_Date'].dt.day
    # save the cleaned data
    shipping.to_csv('data/processed/shipping_cleaned.csv', index=False)

# 3--- Cleaning and Transforming the Time Dimension

    # Handle missing values
    time = handle_missing_data(time, 'time')

    # Convert 'OrderDate' to datetime format
    time['OrderDate'] = pd.to_datetime(time['OrderDate'], errors='coerce')

    # Ensure 'OrderYear' is correct and matches 'OrderDate'
    time['OrderYear'] = time['OrderDate'].dt.year

    # Handle duplicates: Drop duplicate TimeID or OrderDate
    time.drop_duplicates(subset=['TimeID', 'OrderDate'], keep='first', inplace=True)

    # Save the cleaned data
    time.to_csv('data/processed/time_cleaned.csv', index=False)

# 4--- Cleaning and transforming products data
# Remove duplicates
    products = products.drop_duplicates()

# Handle missing values
    products = handle_missing_data(products, 'products')

# Standardize text
    products['Product_Name'] = products['Product_Name'].str.strip().str.title()
    products['Category'] = products['Category'].str.strip().str.title()
    products['Sub_Category'] = products['Sub_Category'].str.strip().str.title()

# Ensure only valid categories are used
    valid_categories = ["Technology", "Furniture", "Office Supplies"]
    products['Category'] = products['Category'].apply(lambda x: x if x in valid_categories else "Miscellaneous")

# Optionally, drop rows with invalid categories instead of assigning "Miscellaneous"
    products = products[products['Category'].isin(valid_categories)]

# Print unique values to confirm correctness
    print("Unique Categories After Validation:", products['Category'].unique())

# Print unique subcategories to check values
    print("Unique Subcategories:", products['Sub_Category'].unique())
# Save the cleaned data
    products.to_csv('data/processed/products_cleaned.csv', index=False)

# 4--- Cleaning and transforming sales data
    # Handle duplicates
    sales.drop_duplicates(inplace=True)
    # Handle missing values
    sales = handle_missing_data(sales, 'sales')

    # Validate numerical columns
    sales['Sales'] = pd.to_numeric(sales['Sales'], errors='coerce')
    sales['Profit'] = pd.to_numeric(sales['Profit'], errors='coerce')
    sales['Quantity'] = pd.to_numeric(sales['Quantity'], errors='coerce')
    sales['Discount'] = pd.to_numeric(sales['Discount'], errors='coerce')

    # Ensure non-negative values
    sales['Sales'] = sales['Sales'].clip(lower=0)
    sales['Quantity'] = sales['Quantity'].clip(lower=0)
    sales['Discount'] = sales['Discount'].clip(lower=0)

    # Validate logical consistency (Profit <= Sales)
    invalid_profit = sales[sales['Profit'] > sales['Sales']]
    if not invalid_profit.empty:
        print("Invalid Profit values found (Profit > Sales):", invalid_profit)
# Save the cleaned data
    sales.to_csv('data/processed/sales_cleaned.csv', index=False)
# Call the function to execute the cleaning process


clean_data() 

