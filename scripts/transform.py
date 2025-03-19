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

    # ---- Data Cleaning ----

    # 1--- cleaning customers data
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

# 2--- cleaning shipping data
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
# Call the function to execute the cleaning process
clean_data() 