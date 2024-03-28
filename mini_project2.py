### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

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
    with open('data.csv') as file:
        temp_list = []
        for line in file:
            line = line.strip().split('\t')
            region = line[4]
            if region != 'Region' and region not in temp_list:
                temp_list.append(region)
        region_list = []
        for i, item in enumerate(sorted(temp_list)):
            region_list.append((i+1, item))
        create_table_region_sql='''CREATE TABLE [Region]
        ([RegionID] INTEGER NOT NULL PRIMARY KEY,[Region] TEXT NOT NULL);'''
        conn = create_connection('normalized.db')
        create_table(conn, create_table_region_sql)
        with conn:
            sql_degrees = '''INSERT INTO Region (RegionId,Region) VALUES(?,?)'''
            cur = conn.cursor()
            cur.executemany(sql_degrees, region_list)  
    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection('normalized.db')
    sql_statement_region = "SELECT * from Region"
    region = execute_sql_statement(sql_statement_region, conn)
    dict1 = {k: v for v, k in region}
    return dict1
    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    region_dict = step2_create_region_to_regionid_dictionary('normalized.db')
    with open(data_filename) as file:
        list1 = []
        list2 = []
        country_list = []
        for line in file:
            line = line.strip().split('\t')
            country, region = line[3], line[4]
            if country != 'Country' and country not in list1:
                list1.append(country)
                list2.append(region_dict[region])
        list3 = list(zip(list1, list2))
        country_list = [(i+1, item[0], item[1]) for i, item in enumerate(sorted(list3))]
        create_table_country_sql='''CREATE TABLE [Country]
    ([CountryID] INTEGER NOT NULL PRIMARY KEY,[Country] TEXT NOT NULL,[RegionID] INTEGER NOT NULL, 
    FOREIGN KEY(RegionID) REFERENCES Region(RegionID));'''
        conn = create_connection('normalized.db')
        create_table(conn, create_table_country_sql)
        with conn:
            sql_country = '''INSERT INTO Country (CountryID,Country,RegionID) VALUES(?,?,?)'''
            cur = conn.cursor()
            cur.executemany(sql_country,country_list)
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection('normalized.db')
    sql_statement_region = "SELECT CountryID,Country from Country"
    country = execute_sql_statement(sql_statement_region, conn)
    my_dict = {k: v for v, k in country}
    return my_dict
    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    country_dict = step4_create_country_to_countryid_dictionary('normalized.db')
    with open(data_filename) as file:
        list1 = []
        customer_list = []
        for line in file:
            line = line.strip().split('\t')
            name, address, city, country = line[0], line[1], line[2], line[3]
            if name != 'Name' and address != 'Address' and city != 'City' and country != 'Country':
                first_name = name.split(' ')[0]
                last_name = name.split(' ')[1]+' '+name.split(' ')[2] if len(name.split()) > 2 else name.split(' ')[1]
                if (first_name, last_name, address, city, country_dict[country]) not in list1:
                    list1.append((first_name, last_name, address, city, country_dict[country]))
        #return list1
        customer_list = [(i+1, item[0], item[1], item[2], item[3], item[4]) for i, item in enumerate(sorted(list1))]
        #return customer_list
        create_table_customer_sql='''CREATE TABLE [Customer]
    ([CustomerID] INTEGER NOT NULL PRIMARY KEY,[FirstName] TEXT NOT NULL,[LastName] TEXT NOT NULL,[Address] TEXT NOT NULL,
    [City] TEXT NOT NULL, [CountryID] INTEGER NOT NULL, 
    FOREIGN KEY(CountryID) REFERENCES Country(CountryID));'''
        conn = create_connection('normalized.db')
        create_table(conn, create_table_customer_sql)
        with conn:
            sql_customer = '''INSERT INTO Customer (CustomerID,FirstName,LastName,Address,City,CountryID) VALUES(?,?,?,?,?,?)'''
            cur = conn.cursor()
            cur.executemany(sql_customer,customer_list)
    ### END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql_statement_customer = "SELECT CustomerID,FirstName,LastName from Customer;"
    customer = execute_sql_statement(sql_statement_customer, conn)
    my_dict = {(f"{firstName} {lastName}"): customerID for customerID, firstName, lastName in customer}
    sorted_dict = dict(sorted(my_dict.items(), key=lambda x: x[0]))
    return sorted_dict
    ### END SOLUTION
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    with open(data_filename) as file:
        list1=[]
        list2=[]
        next(file)
        for line in file:
            line = line.strip().split('\t')
            distinct_categories = line[6].strip().split(';')
            distinct_categories1 = line[7].strip().split(';')
            for i in range(len(distinct_categories1)):
                if distinct_categories1[i] not in list2:
                    list2.append(distinct_categories1[i])
            for i in range(len(distinct_categories)):
                if distinct_categories[i] not in list1:
                    list1.append(distinct_categories[i])
        #print(list1)
        #print(list2)
        product_list = [(i+1, item[0], item[1]) for i, item in enumerate(sorted(set(list(zip(list1, list2)))))]
        create_table_product_sql='''CREATE TABLE [ProductCategory]
        ([ProductCategoryID] INTEGER NOT NULL PRIMARY KEY,[ProductCategory] TEXT NOT NULL,
        [ProductCategoryDescription] TEXT NOT NULL);'''
        conn = create_connection('normalized.db')
        create_table(conn, create_table_product_sql)
        with conn:
            sql_product = '''INSERT INTO ProductCategory (ProductCategoryID,ProductCategory,ProductCategoryDescription) VALUES (?,?,?)'''
            cur = conn.cursor()
            cur.executemany(sql_product,product_list)
    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql_statement_product = "SELECT ProductCategoryID,ProductCategory from ProductCategory;"
    product_category = execute_sql_statement(sql_statement_product, conn)
    my_dict = {k: v for v, k in product_category}
    return my_dict
    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    product_category_id = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    with open(data_filename) as file:
        next(file)
        list1=[]
        list2=[]
        for line in file:
            line = line.strip().split('\t')
            distinct_categories = line[5].strip().split(';')
            distinct_categories2 = line[8].strip().split(';')
            distinct_categories1 = line[6].strip().split(';')
            category_ids = [product_category_id[category] for category in distinct_categories1]
            for i in range(len(distinct_categories)):
                if distinct_categories[i] not in list1:
                    list1.append(distinct_categories[i])
            for i in range(len(distinct_categories2)):
                if distinct_categories2[i] not in list2:
                    list2.append(distinct_categories2[i]) 
        product_list = [(i+1, item[0], item[1], item[2]) for i, item in enumerate(sorted(set(list(zip(distinct_categories, distinct_categories2,category_ids)))))]
        create_table_price_sql='''CREATE TABLE [Product]
        ([ProductID] INTEGER NOT NULL PRIMARY KEY,[ProductName] TEXT NOT NULL,
        [ProductUnitPrice] REAL NOT NULL,[ProductCategoryID] INTEGER NOT NULL,
        FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID));'''
        conn = create_connection('normalized.db')
        create_table(conn, create_table_price_sql)
        with conn:
            sql_price = '''INSERT INTO Product (ProductID,ProductName,ProductUnitPrice,ProductCategoryID) VALUES (?,?,?,?)'''
            cur = conn.cursor()
            cur.executemany(sql_price,product_list)
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    sql_statement_product = "SELECT ProductID,ProductName from Product;"
    product = execute_sql_statement(sql_statement_product, conn)
    my_dict = {k: v for v, k in product}
    return my_dict
    ### END SOLUTION
        
import datetime
def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    order_detail_product_id = step10_create_product_to_productid_dictionary(normalized_database_filename)
    order_detail_customer_id = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    with open(data_filename) as file:
        next(file)
        product_list=[]
        for line in file:
            line = line.strip().split('\t')
            name = line[0]
            first_name = name.split(' ')[0]
            last_name = name.split(' ')[1]+' '+name.split(' ')[2] if len(name.split()) > 2 else name.split(' ')[1]
            customer_id = order_detail_customer_id[first_name + ' ' + last_name]
            distinct_categories = line[5].strip().split(';')
            category_ids = []
            for i in distinct_categories:
                category_id = order_detail_product_id[i]
                category_ids.append(category_id)
            ordered_dates = line[10].strip().split(';')
            date_objs=[]
            for ordered_date in ordered_dates:
                date_objs.append(datetime.datetime.strptime(ordered_date, '%Y%m%d'))
            date_strs = [date_obj.strftime('%Y-%m-%d') for date_obj in date_objs] 
            quantity_ordered=line[9].strip().split(';')
            quantities=[]
            for i in quantity_ordered:
                quantities.append(i)
            for category_id, date_str, quantity in zip(category_ids, date_strs, quantities):
                product_list.append((len(product_list) + 1, customer_id, category_id, date_str, (int(quantity))))
        create_table_orderdetail_sql = """ CREATE TABLE [OrderDetail] (
        [OrderID] INTEGER NOT NULL PRIMARY KEY,
        [CustomerID] INTEGER NOT NULL,
        [ProductID] INTEGER NOT NULL,
        [OrderDate] INTEGER NOT NULL,
        [QuantityOrdered] INTEGER NOT NULL,
        FOREIGN KEY(CustomerID)REFERENCES Customer(CustomerID),
        FOREIGN KEY(ProductID)REFERENCES Product(ProductID)); """
        conn = create_connection(normalized_database_filename)
        create_table(conn,create_table_orderdetail_sql) 
        with conn:
            sql = ''' INSERT INTO OrderDetail(OrderID,CustomerID,ProductID,OrderDate,QuantityOrdered)VALUES(?,?,?,?,?) '''
            cur = conn.cursor()
            cur.executemany(sql,product_list)
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
    customer_to_customerid_dict= step6_create_customer_to_customerid_dictionary('normalized.db')
    sql_statement = f"""
    SELECT (c.FirstName || ' ' || c.LastName) Name, p.ProductName, od.OrderDate, p.ProductUnitPrice, od.QuantityOrdered, ROUND(p.ProductUnitPrice * od.QuantityOrdered, 2) AS Total 
    FROM OrderDetail od 
    INNER JOIN Customer c ON od.CustomerID = c.CustomerID 
    INNER JOIN Product p ON od.ProductID = p.ProductID 
    WHERE od.CustomerID = {customer_to_customerid_dict[CustomerName]};"""
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
    customer_to_customerid_dict= step6_create_customer_to_customerid_dictionary('normalized.db')
    sql_statement = f"""SELECT (c.FirstName || ' ' || c.LastName) AS Name, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total 
    FROM OrderDetail od 
    INNER JOIN Customer c ON od.CustomerID = c.CustomerID 
    INNER JOIN Product p ON od.ProductID = p.ProductID 
    WHERE od.CustomerID = {customer_to_customerid_dict[CustomerName]}
    GROUP BY c.FirstName, c.LastName;"""
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
    sql_statement = """SELECT (c.FirstName || ' ' || c.LastName) AS Name, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total 
    FROM OrderDetail od 
    INNER JOIN Customer c ON od.CustomerID = c.CustomerID 
    INNER JOIN Product p ON od.ProductID = p.ProductID 
    GROUP BY c.FirstName, c.LastName
    ORDER BY -Total;"""
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

    sql_statement = """SELECT r.Region, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total 
    FROM OrderDetail od 
    INNER JOIN Customer c ON od.CustomerID = c.CustomerID 
    INNER JOIN Product p ON od.ProductID = p.ProductID 
	INNER JOIN Country co ON c.CountryID = co.CountryID
	INNER JOIN Region r ON co.RegionID = r.RegionID
    GROUP BY r.Region
	ORDER BY -Total;
    """
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

    sql_statement = """SELECT co.Country, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered)) AS CountryTotal 
    FROM OrderDetail od 
    INNER JOIN Customer c ON od.CustomerID = c.CustomerID 
    INNER JOIN Product p ON od.ProductID = p.ProductID 
	INNER JOIN Country co ON c.CountryID = co.CountryID
    GROUP BY co.Country
	ORDER BY -CountryTotal;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """SELECT r.Region,co.Country, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered)) AS CountryTotal, 
    RANK() OVER (PARTITION BY r.Region ORDER BY SUM(p.ProductUnitPrice * od.QuantityOrdered) DESC) AS CountryRegionalRank
    FROM OrderDetail od 
    INNER JOIN Customer c ON od.CustomerID = c.CustomerID 
    INNER JOIN Product p ON od.ProductID = p.ProductID 
    INNER JOIN Country co ON c.CountryID = co.CountryID
    INNER JOIN Region r ON co.RegionID = r.RegionID 
    GROUP BY r.Region,co.Country
    ORDER BY Region ASC, -CountryTotal;
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

    sql_statement = """SELECT subquery.Region, subquery.Country, subquery.CountryTotal, subquery.CountryRegionalRank
    FROM (
    SELECT r.Region, co.Country, ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered)) AS CountryTotal, 
        RANK() OVER (PARTITION BY r.Region ORDER BY SUM(p.ProductUnitPrice * od.QuantityOrdered) DESC) AS CountryRegionalRank
    FROM OrderDetail od 
    INNER JOIN Customer c ON od.CustomerID = c.CustomerID 
    INNER JOIN Product p ON od.ProductID = p.ProductID 
    INNER JOIN Country co ON c.CountryID = co.CountryID
    INNER JOIN Region r ON co.RegionID = r.RegionID 
    GROUP BY r.Region, co.Country
    ) subquery
    WHERE subquery.CountryRegionalRank = 1
    ORDER BY subquery.Region ASC, -subquery.CountryTotal;  
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

    sql_statement = """WITH quarter_sales AS (
    SELECT 
        CAST(strftime('%Y', od.OrderDate)AS INTEGER) AS Year,
        CAST(strftime('%m', od.OrderDate) AS INTEGER) AS Month,
        'Q'||((CAST(strftime('%m', OrderDate) AS INTEGER)-1) / 3+1) AS Quarter,
        od.CustomerID,
        ROUND(SUM(ProductUnitPrice * od.QuantityOrdered)) AS Total
        FROM OrderDetail od
        INNER JOIN Product p ON od.ProductID = p.ProductID 
        GROUP  BY Year, Quarter, od.CustomerID
        )
        SELECT Quarter,Year,CustomerID, Total
        FROM quarter_sales
        ORDER BY Year, Quarter, CustomerID;
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

    sql_statement = """WITH quarter_sales AS (
    SELECT 
        CAST(strftime('%Y', od.OrderDate) AS INTEGER) AS Year,
        CAST(strftime('%m', od.OrderDate) AS INTEGER) AS Month,
        'Q'||((CAST(strftime('%m', OrderDate) AS INTEGER)-1) / 3+1) AS Quarter,
        od.CustomerID,
        ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered)) AS Total
        FROM OrderDetail od
        INNER JOIN Product p ON od.ProductID = p.ProductID 
        GROUP BY Year, Quarter, od.CustomerID
        ),
        customer_quarter_sales AS (
        SELECT Quarter,Year,CustomerID,Total,
        RANK() OVER (PARTITION BY Year, Quarter ORDER BY Total DESC) AS CustomerRank
        FROM quarter_sales
        )
        SELECT Quarter,Year,CustomerID,Total,CustomerRank
        FROM customer_quarter_sales
        WHERE CustomerRank <= 5
        ORDER BY Year, Quarter, Total DESC;
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

    sql_statement = """WITH monthly_sales AS (
    SELECT 
        CASE
            WHEN strftime('%m', OrderDate) = '01' THEN 'January'
            WHEN strftime('%m', OrderDate) = '02' THEN 'February'
            WHEN strftime('%m', OrderDate) = '03' THEN 'March'
            WHEN strftime('%m', OrderDate) = '04' THEN 'April'
            WHEN strftime('%m', OrderDate) = '05' THEN 'May'
            WHEN strftime('%m', OrderDate) = '06' THEN 'June'
            WHEN strftime('%m', OrderDate) = '07' THEN 'July'
            WHEN strftime('%m', OrderDate) = '08' THEN 'August'
            WHEN strftime('%m', OrderDate) = '09' THEN 'September'
            WHEN strftime('%m', OrderDate) = '10' THEN 'October'
            WHEN strftime('%m', OrderDate) = '11' THEN 'November'
            WHEN strftime('%m', OrderDate) = '12' THEN 'December'
        END AS Month,
        SUM(ROUND(ProductUnitPrice * QuantityOrdered)) AS Total,
        RANK() OVER (ORDER BY SUM(ProductUnitPrice * QuantityOrdered) DESC) AS TotalRank
        FROM OrderDetail od
        INNER JOIN Product p ON od.ProductID = p.ProductID 
        GROUP BY Month
        )
        SELECT Month, Total, TotalRank
        FROM monthly_sales
        ORDER BY TotalRank ASC;
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

    sql_statement = """WITH OrderDates AS (
        SELECT CustomerID,OrderDate,
        LAG(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PreviousOrderDate
        FROM OrderDetail
        ),
        MaxDays AS (
        SELECT OD.CustomerID,C.FirstName,C.LastName,CO.Country,OD.OrderDate,OD.PreviousOrderDate,
        MAX(julianday(OD.OrderDate) - julianday(OD.PreviousOrderDate)) AS MaxDaysWithoutOrder
        FROM OrderDates OD
        INNER JOIN Customer C ON OD.CustomerID = C.CustomerID
        INNER JOIN Country CO ON C.CountryID = CO.CountryID
        GROUP BY OD.CustomerID
        ORDER BY MaxDaysWithoutOrder DESC
        )
        SELECT CustomerID,FirstName,LastName,Country,OrderDate,PreviousOrderDate,MaxDaysWithoutOrder
        FROM MaxDays
        ORDER BY MaxDaysWithoutOrder DESC;
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement