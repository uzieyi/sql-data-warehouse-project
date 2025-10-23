
--PS, the script will be separated soon

-- ============Trends | Change over time=================
--Change over time (aggregated measure by date)
--Sales Performance over time

-- DAY
SELECT 
order_date,
SUM(sales_amount) AS total_sales
FROM dbo.[gold.fact_sales]
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date

-- MONTH
SELECT 
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM dbo.[gold.fact_sales]
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date)


-- MONTH BY YEAR
SELECT 
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM dbo.[gold.fact_sales]
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)


--using Datetrunc
SELECT 
DATETRUNC(MONTH, order_date) AS order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM dbo.[gold.fact_sales]
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(Month, order_date)
ORDER BY DATETRUNC(Month, order_date)


-- YEAR
SELECT 
YEAR(order_date) AS order_year,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM dbo.[gold.fact_sales]
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)


-- ==========Cumulative Analysis==========
-- Aggregate the data progressively over time

-- Calcualte the total sales per month and the running total sales over time 
SELECT 
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM
(
SELECT
DATETRUNC(month, order_date) AS order_date,
SUM(sales_amount) AS total_sales 
FROM dbo.[gold.fact_sales]
WHERE order_date IS NOT NULL 
GROUP BY DATETRUNC(month, order_date)
)t 

-- By Year

SELECT 
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
AVG(avg_price) OVER (ORDER BY order_date) AS moving_average
FROM
(
SELECT
DATETRUNC(year, order_date) AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM dbo.[gold.fact_sales]
WHERE order_date IS NOT NULL 
GROUP BY DATETRUNC(year, order_date)
)t 

--partition by year

SELECT 
order_date,
total_sales,
SUM(total_sales) OVER (PARTITION BY order_date ORDER BY order_date) AS running_total_sales
FROM
(
SELECT
DATETRUNC(month, order_date) AS order_date,
SUM(sales_amount) AS total_sales 
FROM dbo.[gold.fact_sales]
WHERE order_date IS NOT NULL 
GROUP BY DATETRUNC(month, order_date)
)t 



-- =========== Performance Analysis ================ 
-- Comparing the current value to a target value. 
-- Current [measure] - Target[measure]

/*Anaalyze the yearly pergormace of products by comparing each product's sales
to both its average sales performance and the previous year's sales */

WITH yearly_product_sales AS(
SELECT 
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM dbo.[gold.fact_sales] f
LEFT JOIN dbo.[gold.dim_products] p
ON f.product_key = p.product_key
WHERE f.order_date is NOT NULL
GROUP BY 
YEAR(f.order_date),
p.product_name
)
SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name)  > 0 THEN 'Above Avg'
	 WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
	 ELSE 'Avg'
END AS avg_change,

-- Year-over-year Analysis 
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS PY_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year),
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
	 WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
	 ELSE 'No Change'
END AS PY_change
FROM yearly_product_sales
ORDER BY product_name, order_year



-- ========== Part-to-whole =========================
/* Analyse how an individual part is performing compared to the overall, 
allowing us to understand which category has the greatest impact on the business 
([measure] /Total[measure]) * 100 by [Dimension]


--Which country contribute the most to overall sales?
*/

WITH categogry_sales AS
(
SELECT 
category,
SUM(sales_amount) AS total_sales 
FROM dbo.[gold.fact_sales] f
LEFT JOIN dbo.[gold.dim_products] p
ON f.product_key = p.product_key
GROUP BY category
) 

SELECT 
category,
total_sales,
SUM(total_sales) OVER () overall_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER())  * 100, 2), '%') AS percentage_of_total
FROM categogry_sales
ORDER BY total_sales



-- ========== Data Segmentation =========================

/*  
Group the data on a specific range
Helps to understand the coreelation between two measures
- [Measure] BY [Measure]
eg - Total Products By Sales Range or
   - Total Customers By Age
*/

-- Segment Products into cost range and 
-- count how many products faill into each segment 
WITH product_segement AS
(
SELECT 
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'BELOW 100'
	 WHEN  cost BETWEEN 100 AND 500 THEN '100 - 500'
	 WHEN cost  BETWEEN 500 AND 1000 THEN '500 - 1000'
	 ELSE 'Above 1000'
END AS cost_range
FROM dbo.[gold.dim_products]
)

SELECT 
cost_range,
COUNT(product_key) AS total_products
FROM product_segement
GROUP BY cost_range
ORDER BY total_products DESC

/*
Group customers into three segments based on their spending behaviour:
	-- VIP: Customers with at least 12 Months of history and spending more than $5,000.
	-- Regular: Custoemrs with at least 12 Months of history but spending $5,000 or less.
	-- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/
WITH customer_spending AS 
(
SELECT 
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(f.order_date) AS first_order,
MAX(f.order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM dbo.[gold.fact_sales] f 
LEFT JOIN dbo.[gold.dim_customers] c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)
SELECT 
customer_key,
total_spending,
lifespan,
CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
	 WHEN lifespan <= 12 AND total_spending <= 5000 THEN 'Regular'
	 ELSE 'NEW'
END AS customer_segment 
FROM customer_spending

-- find the total number of customers by each group

WITH customer_spending AS 
(
SELECT 
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(f.order_date) AS first_order,
MAX(f.order_date) AS last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM dbo.[gold.fact_sales] f 
LEFT JOIN dbo.[gold.dim_customers] c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)
SELECT
customer_segment,
COUNT(customer_key) AS total_customers
FROM(
	SELECT 
	customer_key,
	CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
		 WHEN lifespan <= 12 AND total_spending <= 5000 THEN 'Regular'
		 ELSE 'NEW'
	END AS customer_segment
	FROM customer_spending) t
GROUP BY customer_segment 
ORDER BY total_customers DESC

