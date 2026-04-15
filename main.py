# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Return the first and last names and the job titles for all employees in Boston.
df_boston = """ 
SELECT firstName, jobTitle
    FROM employees
    JOIN offices
    USING(officeCode)
    WHERE city = 'Boston'
; """

# STEP 2
# Return all offices that have zero employees
df_zero_emp = """ 
SELECT officeCode, city, COUNT(*) as num_employees
    FROM employees
    JOIN offices
    USING(officeCode)
    GROUP BY officeCode
    HAVING num_employees = 0
; """

# STEP 3
# Replace None with your code
""" 
Return:
    employees' first name and last name
    the city and state of the office that they work out of (if they have one).
Include: all employees
    order them by their first name, 
    then their last name.
"""
df_employee = """ 
SELECT firstName, lastName, city, state
    FROM employees
    JOIN offices
    USING(officeCode)
    ORDER BY firstName, lastName
; """

# STEP 4
""" 
Return all:
    the customer's contact information (first name, last name, and phone number) 
    their sales rep's employee number for any customer who has not placed an order.
Sort the results alphabetically based on the contact's last name
"""
df_contacts = """ 
SELECT contactFirstName, contactLastName, phone, salesRepEmployeeNumber
    FROM customers
    LEFT JOIN orders
    USING(customerNumber)
    WHERE orderNumber IS NULL
    ORDER BY contactLastName
; """

# STEP 5
""" 
Return all:
    the customer contacts (first and last names) 
    details for each of the customers' payment 
    amounts and dates of payment.
Results to be sorted in descending order by the payment amount
; """
df_payment = """ 
SELECT contactFirstName, contactLastName,
    CAST(amount AS FLOAT) AS amount_float,paymentDate
    FROM customers
    JOIN payments
    USING(customerNumber)
    ORDER BY amount_float DESC
; """

# STEP 6
"""
Return the employee:
    number, first name, last name, and number of customers for employees 
    whose customers have an average credit limit over 90k.
Sort by number of customers from high to low.
; """
df_credit = """ 
SELECT firstName,lastName,
    COUNT(*) AS num_customers,
    AVG(CAST(creditLimit AS FLOAT)) AS avg_credit_limit
    FROM employees AS e
    JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY 1,2
    HAVING avg_credit_limit > 90000
    ORDER BY num_customers DESC
; """

# STEP 7
"""
Return the:
    product name and count the number of orders for each 
    product as a column named 'numorders'.
Also return:
    a new column, 'totalunits', that sums up the total quantity of product 
    sold (use the 'quantityOrdered column')
"""
df_product_sold = """ 
SELECT productName,
    COUNT(*) AS numorders,
    SUM(quantityOrdered) AS totalunits
    FROM products
    JOIN orderDetails
    USING(productCode)
    GROUP BY 1
    ORDER BY totalunits DESC
; """

# STEP 8
"""
Return:
    the product name, code, 
    and the total number of customers who have ordered each product, 
    aliased as 'numpurchasers'
Sort the results by the highest number of purchasers.
"""
df_total_customers = """ 
SELECT p.`productName`, p.`productCode`,
    COUNT(DISTINCT c.`customerName`) AS numpurchasers
    FROM products AS p
    JOIN orderdetails AS od
        ON p.`productCode` = od.`productCode`
    JOIN orders AS o
        ON od.`orderNumber` = o.`orderNumber`
    JOIN customers AS c
        ON o.`customerNumber` = c.`customerNumber`
    GROUP BY 1
    ORDER BY numpurchasers DESC
; """

# STEP 9
""" 
Find out number of customers per office.
Return:
    the count as a column named 'n_customers'.
    the office code and city.
"""
df_customers = """ 
SELECT o.`officeCode`, o.city,
    COUNT(c.`customerNumber`) AS n_customers
    FROM offices AS o
    JOIN employees AS e
      ON o.`officeCode` = e.`officeCode`
    JOIN customers AS c
      ON e.`employeeNumber` = c.`salesRepEmployeeNumber`
    GROUP BY 1
; """

# STEP 10
"""
Using a subquery, select:
    the employee number, 
    first name, last name, 
    city of the office, and 
    the office code for employees who sold 
    products that have been ordered by fewer than 20 customers.
"""
df_under_20 = """ 
SELECT employeeNumber, firstName, lastName, city, officeCode
    FROM (
        SELECT 
            *,
            COUNT(DISTINCT c.`customerName`) AS numpurchasers
            FROM products AS p
            JOIN orderdetails AS od
                ON p.`productCode` = od.`productCode`
            JOIN orders AS o
                ON od.`orderNumber` = o.`orderNumber`
            JOIN customers AS c
                ON o.`customerNumber` = c.`customerNumber`
            JOIN employees AS e
                ON c.`salesRepEmployeeNumber` = e.`employeeNumber`
            JOIN offices AS off
                ON e.`officeCode` = off.`officeCode`
            GROUP BY employeeNumber
            HAVING numpurchasers < 20
        )
    ORDER BY lastName
    ; """

conn.close()