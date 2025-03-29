# Superstore Dataset Analysis and ETL with Python & Talend

## Project Overview
This project focuses on analyzing the Superstore dataset to extract meaningful insights that can drive business decisions. The project involves an ETL (Extract, Transform, Load) pipeline implemented in 2 methods : once using Python and once using Talend, to gain skills in both sowtwares (note that the same transformations are done for both processes). The final output is a structured database that enables efficient analysis and reporting.

## Key Objectives

### Data Cleaning and Transformation
- Remove inconsistencies, missing values, and redundant data.
- Standardize data formats for better accuracy and consistency.

### ETL Process
- **Python Implementation:** Initial data extraction, transformation, and loading were performed using Python.
- **Talend Implementation:** The same transformations were replicated in Talend, with an additional document outlining the process.

### Data Warehouse Development
- Design and implement a structured schema in MySQL for optimized querying and reporting.

### Data Analysis and Visualization
- Prepare the cleaned dataset for future reporting and dashboarding.

## Project Features

### ETL Pipeline Implementations

#### Python-Based ETL
- Extract data from the staging area that contains raw data from multiple sources.
- Clean and transform the dataset using Pandas and NumPy.
- Load the data into a MySQL database.

#### Talend-Based ETL
- Implemented the same transformations using Talend components.
- Documented the ETL process with screenshots and explanations.

### Database Schema Design
Designed as a star schema with:

- **Fact Table:** Sales  
- **Dimension Tables:**  
  - Customers  
  - Products  
  - Regions  
  - Time  


## Technologies Used
- **Python:** Used for the first ETL implementation (Pandas, NumPy, SQLAlchemy).
- **Talend:** Used for the second ETL implementation.
- **MySQL:** Relational database for structured data storage.
- **CSV & Excel:** Input data formats.

## Key Files in Repository

### Python ETL Scripts
- `extract_data.py`: Extract data from CSV/Excel files.
- `transform_data.py`: Clean and transform the dataset.
- `load_data.py`: Load cleaned data into MySQL.
- `validation_checks.py`: validation checks to ensure consistency.
  
### Talend ETL Process
- **ETL with Talend:** Contains screenshots and descriptions of the Talend workflows.

### Data Files
- raw data files before transformations.
- processed data after transformations.

## Conclusion
This project showcases two different ETL approaches—Python and Talend—applied to the same dataset. It highlights the strengths of both tools in data transformation and loading while ensuring a structured and efficient data pipeline for further analysis.


