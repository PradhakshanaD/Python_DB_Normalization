# Mini-Project: Normalized Database Creation

## Description:
This mini-project involves parsing a large, messy data file and creating a normalized database from it. The data file contains information about customers and their product orders, with each row representing a customer and their ordered products separated by semicolons. The database creation process includes creating six tables: Region, Country, Customer, ProductCategory, Product, and OrderDetail.

## Objectives:
Parse a large 'messy' file

Create a normalized database from a large 'messy' file

Optimize code to load 600,000+ rows into a database quickly

Practice SQL queries

## Project Steps:
The project is divided into several steps, each focusing on creating and populating a specific table in the normalized database. Here's a brief overview of the steps:

Create the Region Table

Create a dictionary to map Region to RegionID

Create the Country Table

Create a dictionary to map Country to CountryID

Create the Customer Table

Create a dictionary to map Name (FirstName LastName) to CustomerID

Create the Product Category Table

Create a dictionary to map productcategory to productcategoryid

Create the Product Table

Create a dictionary to map product to productid

Create the OrderDetail Table

## Project files:
data.csv: Input data file containing customer information and product orders.

Output files for each table: These files contain the data inserted into each table after parsing the input file.

instructions.txt: Instructions file providing step-by-step guidelines for completing the project.

mini_project2.py: Python source file containing the code to parse the data, create the database tables, and load data into the tables.

mini_project2_template: Template provided as part of the assignment.

## Disclaimer:
This project was completed without utilizing Pandas functions. This project was completed with adherence to the specified guidelines and instructions. For any inquiries or further assistance, please refer to the instructions file  It was conducted as part of a Master's program, focusing on database normalization, SQL query practice, and efficient data handling techniques. For any further assistance, please refer to the instructions file.
