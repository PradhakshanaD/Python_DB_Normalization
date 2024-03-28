# Mini-Project: Normalized Database Creation

## Description:

This mini-project involves parsing a large, messy data file and creating a normalized database from it. The data file contains information about customers and their product orders, with each row representing a customer and their ordered products separated by 
semicolons. The database creation process includes creating six tables: Region, Country, Customer, ProductCategory, Product, and OrderDetail.

## Objectives:

Parse a large 'messy' file

Create a normalized database from a large 'messy' file

Optimize code to load 600,000+ rows into a database quickly

Practice SQL queries

## Data Description:

The data is stored in a CSV file with 11 columns separated by tabs.

Each row represents a customer with details such as name, address, city, country, region, and their product orders.


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

## Disclaimer:

This project was completed without utilizing Pandas functions, adhering to the guidelines provided. It was conducted as part of a Master's program, focusing on database normalization, SQL query practice, and efficient data handling techniques.



