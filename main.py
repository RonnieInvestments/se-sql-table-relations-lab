# STEP 0

# Import SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("data.sqlite")

pd.read_sql("""SELECT * FROM sqlite_master""", conn)


# STEP 1
# Return the first and last names and the job titles for all employees in Boston.
q_boston = """ 
SELECT 
    firstName,
    jobTitle
FROM employees
    JOIN offices
    USING(officeCode)
WHERE city = 'Boston'
; """

df_boston = pd.read_sql(q_boston, conn)

# STEP 2
# Return all offices that have zero employees
q_zero_emp = """ 
SELECT officeCode, city, COUNT(*) as num_employees
FROM employees
    JOIN offices
    USING(officeCode)
GROUP BY officeCode
HAVING num_employees = 0
; """

df_zero_emp = pd.read_sql(q_zero_emp, conn)

# STEP 3
""" 
Return the employees' first name and last 
name along with the city and state of the 
office that they work out of (if they have one).
Include all employees and order them by their first name, 
then their last name.
 """

q_employee = """ 
SELECT firstName, lastName, city, state
FROM employees
    JOIN offices
    USING(officeCode)
ORDER BY firstName, lastName
; """

df_employee = pd.read_sql(q_employee, conn)

# STEP 4
""" Return all of the customer's contact information 
(first name, last name, and phone number) as 
well as their sales rep's employee number for any customer 
who has not placed an order.
Sort the results alphabetically based on the contact's last name
"""

q_contacts = """ 
SELECT 
    contactFirstName, 
    contactLastName, 
    phone, 
    salesRepEmployeeNumber
FROM customers
    LEFT JOIN orders
    USING(customerNumber)
WHERE orderNumber IS NULL
ORDER BY contactLastName
; """

df_contacts = pd.read_sql(q_contacts, conn)

# STEP 5
""" 
Return all the customer contacts (first and last names) 
along with details for each of the customers' payment 
amounts and dates of payment.
Results to be sorted in descending order by the payment amount
; """

q_payment = """ 
SELECT 
    contactFirstName, 
    contactLastName,
    CAST(amount AS FLOAT) AS amount_float,
    paymentDate
FROM customers
    JOIN payments
    USING(customerNumber)
ORDER BY amount_float DESC
; """

df_payment = pd.read_sql(q_payment, conn)

# STEP 6
"""
Return the employee number, first name, last name, and number of customers for employees 
whose customers have an average credit limit over 90k.
Sort by number of customers from high to low.
; """
q_credit = """ 
SELECT 
    firstName,
    lastName,
    COUNT(*) AS num_customers,
    AVG(CAST(creditLimit AS FLOAT)) AS avg_credit_limit
FROM employees AS e
    JOIN customers AS c
    ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY 1,2
HAVING avg_credit_limit > 90000
ORDER BY num_customers DESC
; """

df_credit = pd.read_sql(q_credit, conn)

# STEP 7
"""
Return the product name and count the number of orders for each 
product as a column named 'numorders'.
Also return a new column, 'totalunits', that sums up the total quantity of product 
sold (use the 'quantityOrdered column')
"""
q_product_sold = """ 
SELECT
    productName,
    COUNT(*) AS numorders,
    SUM(quantityOrdered) AS totalunits
FROM products
    JOIN orderDetails
    USING(productCode)
GROUP BY 1
ORDER BY totalunits DESC
; """
df_product_sold = pd.read_sql(q_product_sold, conn)

# STEP 8
"""
Return the product name, code, 
and the total number of customers who have ordered each product, 
aliased as 'numpurchasers'

Sort the results by the highest number of purchasers.
"""
q_total_customers = """ 
SELECT 
    p.`productName`,
    p.`productCode`,
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
df_total_customers = pd.read_sql(q_total_customers, conn)

# STEP 9
""" 
Find out how many customers there are per office.
Return the count as a column named 'n_customers'.
Also, return the office code and city.
"""
q_customers = """ 
SELECT 
    o.`officeCode`,
    o.city,
    COUNT(c.`customerNumber`) AS n_customers
FROM offices AS o
    JOIN employees AS e
      ON o.`officeCode` = e.`officeCode`
    JOIN customers AS c
      ON e.`employeeNumber` = c.`salesRepEmployeeNumber`
GROUP BY 1
; """
df_customers = pd.read_sql(q_customers, conn)

# STEP 10
"""
Using a subquery, select the employee number, 
first name, last name, city of the office, and the office code for employees who sold 
products that have been ordered by fewer than 20 customers.
"""

q_under_20 = """ 
SELECT 
    employeeNumber,
    firstName,
    lastName,
    city,
    officeCode
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

df_under_20 = pd.read_sql(q_under_20, conn)


#Close the connection to the database
conn.close()