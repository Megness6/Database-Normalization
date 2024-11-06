### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error
import datetime


def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION

    conn_norm = create_connection(normalized_database_filename)

    regions = set()
    with open(data_filename) as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            line = line.split('\t')
            
            region = line[4]
            regions.add(region)
            sorted_regions = sorted(list(regions))

    create_region_query = "create table if not exists Region (RegionID integer primary key not null, Region text not null);"
    create_table(conn_norm, create_region_query)

    def insert_region(conn_norm, values):
        sql_statement = "insert into Region(Region) values(?);"
        cur = conn_norm.cursor()
        cur.executemany(sql_statement, [(v,) for v in values])
        return cur.lastrowid
    
    with conn_norm:
        insert_region(conn_norm, sorted_regions)

    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION

    conn_norm = create_connection(normalized_database_filename)

    sql_query = "select RegionID, Region from Region;"
    region_data = execute_sql_statement(sql_query, conn_norm)

    region_to_regionid_dict = {}
    keys = []
    values = []
    for each_ele in region_data:
        keys = each_ele[1]
        values = each_ele[0]
        region_to_regionid_dict[keys] = values

    return region_to_regionid_dict    

    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    
    conn_norm = create_connection(normalized_database_filename)

    countries = []
    regions = []
    with open(data_filename) as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            line = line.split('\t')
            
            country = line[3]
            region = line[4]
            countries.append(country)
            regions.append(region)
            
            country_region = sorted(set(zip(countries, regions)))

        region_data = step2_create_region_to_regionid_dictionary(normalized_database_filename)
        
        region_id = []
        country_regionid = []
        for i in country_region:
            if i[1] in region_data.keys():
                region_id = region_data[i[1]]
                country_regionid.append((i[0], region_id))

    create_country_query = "create table if not exists Country (CountryID integer primary key not null, Country text not null, RegionID integer not null, foreign key(RegionID) references Region(RegionID));"
    create_table(conn_norm, create_country_query)

    def insert_country(conn_norm, values):
        sql_statement = "insert into Country(Country, RegionID) values(?, ?);"
        cur = conn_norm.cursor()
        cur.executemany(sql_statement, values)
        return cur.lastrowid
    
    with conn_norm:
        insert_country(conn_norm, country_regionid)
         
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    
    conn_norm = create_connection(normalized_database_filename)

    sql_query = "select CountryID, Country from Country;"
    region_data = execute_sql_statement(sql_query, conn_norm)

    country_to_countryid_dict = {}
    keys = []
    values = []
    for each_ele in region_data:
        keys = each_ele[1]
        values = each_ele[0]
        country_to_countryid_dict[keys] = values

    return country_to_countryid_dict  

    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    
    conn_norm = create_connection(normalized_database_filename)

    first_names = []
    last_names = []
    addresses = []
    cities = []
    countries = []
    with open(data_filename) as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            line = line.split('\t')
            
            name = line[0]
            first_name = name.strip().split(' ')[0]
            last_name = ' '.join(name.strip().split(' ')[1:])
            address = line[1]
            city = line[2]
            country = line[3]
            first_names.append(first_name)
            last_names.append(last_name)
            addresses.append(address)
            cities.append(city)
            countries.append(country)
            
            table_data = sorted(set(zip(first_names, last_names, addresses, cities, countries)))

        country_data = step4_create_country_to_countryid_dictionary(normalized_database_filename)
        
        country_id = []
        customer_table_output = []
        for i in table_data:
            if i[4] in country_data.keys():
                country_id = country_data[i[4]]
                customer_table_output.append((i[0], i[1], i[2], i[3], country_id))

    create_customer_query = "create table if not exists Customer (CustomerID integer primary key not null, FirstName text not null, LastName text not null, Address text not null, City text not null, CountryID integer not null, foreign key(CountryID) references Country(CountryID));"
    create_table(conn_norm, create_customer_query)

    def insert_customer(conn_norm, values):
        sql_statement = "insert into Customer(FirstName, LastName, Address, City, CountryID) values(?, ?, ?, ?, ?);"
        cur = conn_norm.cursor()
        cur.executemany(sql_statement, values)
        return cur.lastrowid
    
    with conn_norm:
        insert_customer(conn_norm, customer_table_output)

    ### END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    
    conn_norm = create_connection(normalized_database_filename)

    sql_query = "select FirstName, LastName, CustomerID from Customer;"
    customer_data = execute_sql_statement(sql_query, conn_norm)

    customer_to_customerid_dict = {}
    keys = []
    values = []
    for each_ele in customer_data:
        keys = each_ele[0] + ' ' + each_ele[1]
        values = each_ele[2]
        customer_to_customerid_dict[keys] = values

    return customer_to_customerid_dict

    ### END SOLUTION
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    
    conn_norm = create_connection(normalized_database_filename)

    prod_categories = []
    prod_description = []
    
    with open(data_filename) as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            line = line.split('\t')
            
            prod_categories = line[6].split(';')
            prod_description = line[7].split(';')
            
        prodcat_data = sorted(set(zip(prod_categories, prod_description)))

    create_prodcust_query = "create table if not exists ProductCategory (ProductCategoryID integer primary key not null, ProductCategory text not null, ProductCategoryDescription text not null);"
    create_table(conn_norm, create_prodcust_query)

    def insert_productcategory(conn_norm, values):
        sql_statement = "insert into ProductCategory(ProductCategory, ProductCategoryDescription) values(?, ?);"
        cur = conn_norm.cursor()
        cur.executemany(sql_statement, values)
        return cur.lastrowid
    
    with conn_norm:
        insert_productcategory(conn_norm, prodcat_data)
   
    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn_norm = create_connection(normalized_database_filename)

    sql_query = "select ProductCategoryID, ProductCategory from ProductCategory;"
    prodcat_data = execute_sql_statement(sql_query, conn_norm)

    prodcat_to_prodcatid_dict = {}
    keys = []
    values = []
    for each_ele in prodcat_data:
        keys = each_ele[1]
        values = each_ele[0]
        prodcat_to_prodcatid_dict[keys] = values

    return prodcat_to_prodcatid_dict


    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    
    conn_norm = create_connection(normalized_database_filename)

    prod_cat = []
    prod_names = []
    prod_unitprices = []
    
    with open(data_filename) as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            line = line.split('\t')
            
            prod_names = line[5].split(';')
            prod_unitprices = line[8].split(';')
            prod_categories = line[6].split(';')
            
            prod_data = sorted(set(zip(prod_names, prod_unitprices, prod_categories)))
        prodcat_data = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
#         print(prod_data)
        
        prodcat_id = []
        prod_table_output = []
        for i in prod_data:
            if i[2] in prodcat_data.keys():
                prodcat_id = prodcat_data[i[2]]
                prod_table_output.append((i[0], i[1], prodcat_id))

    create_product_query = "create table if not exists Product (ProductID integer primary key not null, ProductName text not null, ProductUnitPrice real not null, ProductCategoryID integer not null, foreign key(ProductCategoryID) references ProductCategory(ProductCategoryID));"
    create_table(conn_norm, create_product_query)

    def insert_product(conn_norm, values):
        sql_statement = "insert into Product(ProductName,ProductUnitPrice,ProductCategoryID) values(?, ?, ?);"
        cur = conn_norm.cursor()
        cur.executemany(sql_statement, values)
        return cur.lastrowid
    
    with conn_norm:
        insert_product(conn_norm, prod_table_output)
   
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    conn_norm = create_connection(normalized_database_filename)

    sql_query = "select ProductID, ProductName from Product;"
    prod_data = execute_sql_statement(sql_query, conn_norm)

    product_to_productid_dict = {}
    keys = []
    values = []
    for each_ele in prod_data:
        keys = each_ele[1]
        values = each_ele[0]
        product_to_productid_dict[keys] = values

    return product_to_productid_dict

    ### END SOLUTION
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn_norm = create_connection(normalized_database_filename)   
    data = []

    with open(data_filename) as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            line = line.split('\t')
            
            quantities_ordered = line[9].split(';')
            order_date = line[10].split(';')
            formatted_date = []
            for i in order_date:
                i = datetime.datetime.strptime(i, '%Y%m%d').strftime('%Y-%m-%d')
                formatted_date.append(i)
            product = line[5].split(';')
            customer_name = [line[0]]*len(formatted_date)

            orddet_data = list(zip(customer_name, product, formatted_date, quantities_ordered))
            for i in orddet_data:
                data.append(i)
        orddet_data = data
        prod_data = step10_create_product_to_productid_dictionary(normalized_database_filename)
        cust_data = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
        
        orddet_table_output = []
        for i in orddet_data:
            cust_id = cust_data[i[0]]
            prod_id = prod_data[i[1]]
            orddet_table_output.append((cust_id, prod_id, i[2], int(i[3])))
        
        create_orddet_query = '''create table if not exists OrderDetail (
            OrderID integer primary key not null, 
            CustomerID inetger not null, 
            ProductID integer not null, 
            OrderDate integer not null, 
            QuantityOrdered integer not null, 
            foreign key(CustomerID) references Customer(CustomerID), 
            foreign key(ProductID) references Product(ProductID));'''
        create_table(conn_norm, create_orddet_query)

        def insert_product(conn_norm, values):
            sql_statement = "insert into OrderDetail(CustomerID, ProductID, OrderDate, QuantityOrdered) values(?, ?, ?, ?);"
            cur = conn_norm.cursor()
            cur.executemany(sql_statement, values)
            return cur.lastrowid
        
        with conn_norm:
            insert_product(conn_norm, orddet_table_output)
    ### END SOLUTION


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION

    cust_dict = step6_create_customer_to_customerid_dictionary('normalized.db')
    cust_id = cust_dict[CustomerName]
    sql_statement = f"""select FirstName || ' ' || LastName as Name, ProductName, OrderDate, ProductUnitPrice, QuantityOrdered, round(ProductUnitPrice * QuantityOrdered, 2) as Total from OrderDetail INNER JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID INNER JOIN Product ON Product.ProductID = OrderDetail.ProductID where Customer.CustomerID = {cust_id};"""
    
    ### END SOLUTION

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement
    

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION

    cust_dict = step6_create_customer_to_customerid_dictionary('normalized.db')
    cust_id = cust_dict[CustomerName]
    sql_statement = f"""select FirstName || ' ' || LastName as Name, ROUND(sum(ProductUnitPrice * QuantityOrdered), 2) as Total from OrderDetail INNER JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID INNER JOIN Product ON Product.ProductID = OrderDetail.ProductID where Customer.CustomerID = {cust_id};"""
    
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """Select FirstName || ' ' || LastName as Name, round(sum(ProductUnitPrice * QuantityOrdered), 2) as Total from OrderDetail INNER JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID  INNER JOIN Product ON Product.ProductID = OrderDetail.ProductID group by Name order by Total desc;"""
    
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """select Region, round(sum(ProductUnitPrice * QuantityOrdered), 2) as Total from Customer INNER JOIN OrderDetail ON OrderDetail.CustomerID = Customer.CustomerID  INNER JOIN Country ON Country.CountryID = Customer.CountryID INNER JOIN Region ON Region.RegionID = Country.RegionID INNER JOIN Product ON Product.ProductID = OrderDetail.ProductID group by Region order by Total desc"""
    
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """select Country, round(sum(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 2) as Total 
    from OrderDetail INNER JOIN OrderDetail ON OrderDetail.CustomerID = Customer.CustomerID  
    INNER JOIN Country ON Country.CountryID = Customer.CountryID 
    INNER JOIN Product ON Product.ProductID = OrderDetail.ProductID group by Country order by Total desc"""
    
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """
     
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """
      
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """
       
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """
      
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """
     
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement