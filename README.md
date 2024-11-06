# Database-Normalization

**Description:** Orchestrated the parsing and normalization of a complex, large-scale dataset consisting of over 600,000 rows of messy, unstructured data. This project aimed to clean, organize, and structure the dataset by applying efficient data processing and normalization techniques, using Python, SQL, and custom scripting to create a normalized relational database.
## Details:
**Data Parsing:** Utilized Python scripting to parse raw data from a large, disorganized file into manageable pieces. Cleaned and transformed the data using various built-in Python libraries like re (regular expressions) and datetime for efficient data extraction and conversions.

**Normalization:** Constructed a normalized relational database by creating six distinct tables, ensuring data integrity and eliminating redundancy. Applied normalization techniques, specifically breaking down the data into logical, non-redundant segments (up to 3NF), and establishing primary and foreign key relationships between tables.

**Efficient Data Insertion:** Avoided the overhead of CSV files and Pandas DataFrame manipulation. Instead, opted for SQL’s executemany() function to optimize data insertion, dramatically increasing insertion speed for large datasets. Converted datetime fields to standardized formats during the insertion process to ensure consistency across the dataset.

**SQL Optimization:** Wrote optimized SQL queries for the creation of tables, schema design, and inserting the parsed data. Employed bulk data loading techniques to insert over 600,000 rows into the database efficiently, reducing processing time.

**Technologies Used:** Python, SQL, executemany(), datetime, Regular Expressions (re), MySQL/PostgreSQL

**Outcome:** Successfully normalized and loaded large-scale data into a fully structured database with minimal memory consumption and fast processing speeds. The project demonstrated my ability to handle complex data transformation tasks, optimize for performance, and structure data for future use in analytics.
