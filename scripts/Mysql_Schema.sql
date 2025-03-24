/*Staging Area*/
-- Drop tables in reverse order to avoid foreign key dependency issues
DROP TABLE IF EXISTS Staging_Sales_Fact;
DROP TABLE IF EXISTS Staging_Time_Dim;
DROP TABLE IF EXISTS Staging_Shipping_Dim;
DROP TABLE IF EXISTS Staging_Product_Dim;
DROP TABLE IF EXISTS Staging_Customer_Dim;

-- Create dimension tables first
CREATE TABLE Staging_Customer_Dim (
    Customer_ID VARCHAR(100) ,
    Customer_Name VARCHAR(255),
    Segment VARCHAR(50),
    City VARCHAR(100),
    State VARCHAR(100),
    Country VARCHAR(100),
    Region VARCHAR(50)
);
SELECT * FROM Staging_Customer_Dim;

CREATE TABLE Staging_Product_Dim (
    Product_ID VARCHAR(100) ,
    Product_Name VARCHAR(255),
    Category VARCHAR(100),
    Sub_Category VARCHAR(100)
);
SELECT * FROM Staging_Product_Dim;

CREATE TABLE Staging_Shipping_Dim (
    Order_ID VARCHAR(100),
    Ship_Date DATE,
    Ship_Mode VARCHAR(50),
    Delivery_Days INT,
    Shipping_Cost DECIMAL(10, 2)
);
SELECT * FROM Staging_Shipping_Dim;

CREATE TABLE Staging_Time_Dim (
	TimeID INT ,
    OrderDate DATE ,
    OrderYear INT,
    OrderMonth VARCHAR(100),
    DayOfWeek VARCHAR(100),
    OrderQuarter VARCHAR(100)
    
);
SELECT * FROM Staging_Time_Dim;

-- Now create the fact table after dimension tables exist
CREATE TABLE Staging_Sales_Fact (
    Order_ID VARCHAR(100),
    Product_ID VARCHAR(100),
    Customer_ID VARCHAR(100),
    TimeID INT,
    Sales DECIMAL(10, 2),
    Profit DECIMAL(10, 2),
    Quantity INT,
    Discount DECIMAL(5, 2)
);
SELECT * FROM Staging_Sales_Fact;

/*****************************************************************************************************************************************************************/
/* this is the schema after transformation*/

-- Drop tables in reverse order to avoid foreign key dependency issues
DROP TABLE IF EXISTS Sales_Fact;
DROP TABLE IF EXISTS Time_Dim;
DROP TABLE IF EXISTS Shipping_Dim;
DROP TABLE IF EXISTS Product_Dim;
DROP TABLE IF EXISTS Customer_Dim;

-- Create dimension tables first
CREATE TABLE Customer_Dim (
    Customer_ID VARCHAR(100) PRIMARY KEY,
    Customer_Name VARCHAR(255),
    Segment VARCHAR(50),
    City VARCHAR(100),
    State VARCHAR(100),
    Country VARCHAR(100),
    Region VARCHAR(50),
    Grouped_Region VARCHAR(100)
);
SELECT * FROM Customer_Dim;

CREATE TABLE Product_Dim (
    Product_ID VARCHAR(100) PRIMARY KEY,
    Product_Name VARCHAR(255),
    Category VARCHAR(100),
    Sub_Category VARCHAR(100)
);
SELECT * FROM Product_Dim;
SELECT count(*), Category FROM Product_Dim GROUP BY (Category) ;



CREATE TABLE Shipping_Dim (
    Order_ID VARCHAR(100) PRIMARY KEY,
    Ship_Date DATE,
    Ship_Mode VARCHAR(50),
    Delivery_Days INT,
    Shipping_Cost DECIMAL(10, 2),
    Shipping_Cost_Category VARCHAR(100),
    Ship_Year INT,
    Ship_Month VARCHAR(50),
    Ship_Day INT
    
);
SELECT * FROM Shipping_Dim;

CREATE TABLE Time_Dim (
	TimeID INT PRIMARY KEY,
    OrderDate DATE ,
    OrderYear INT,
    OrderMonth VARCHAR(100),
    DayOfWeek VARCHAR(100),
    OrderQuarter VARCHAR(100)
    
);
SELECT * FROM Time_Dim;

-- Now create the fact table after dimension tables exist
CREATE TABLE Sales_Fact (
    Order_ID VARCHAR(100),
    Product_ID VARCHAR(100),
    Customer_ID VARCHAR(100),
    TimeID INT,
    Sales DECIMAL(10, 2),
    Profit DECIMAL(10, 2),
    Quantity INT,
    Discount DECIMAL(5, 2),
	FOREIGN KEY (Customer_ID) REFERENCES Customer_Dim(Customer_ID) ON DELETE CASCADE,
    FOREIGN KEY (Product_ID) REFERENCES Product_Dim(Product_ID) ON DELETE CASCADE,
    FOREIGN KEY (TimeID) REFERENCES Time_Dim(TimeID) ON DELETE CASCADE,
    FOREIGN KEY (Order_ID) REFERENCES Shipping_Dim(Order_ID) ON DELETE CASCADE

);
SELECT * FROM Sales_Fact;

SET GLOBAL wait_timeout = 600;
SET GLOBAL interactive_timeout = 600;
SET GLOBAL net_read_timeout = 300;
SET GLOBAL net_write_timeout = 300;






