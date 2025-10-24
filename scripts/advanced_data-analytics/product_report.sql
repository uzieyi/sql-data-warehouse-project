/*

**************************************************************************
You can find the complete data exploration and analysis project in the link below
https://github.com/uzieyi/sql-exploratory-data-analytics-project.git
***************************************************************************
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviours.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segment products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/

CREATE VIEW gold.report_products AS 

WITH base_query AS(
-- 1) Base Query: Retrieves core columns from the tables 

SELECT 
f.customer_key,
f.order_number,
f.price,
f.sales_amount,
f.quantity,
f.order_date,
p.product_id,
p.product_name,
p.cost,
p.maintenance,
p.category_id,
P.product_line,
p.subcategory,
p.start_date,
DATEDIFF(month, start_date, GETDATE()) AS product_age
FROM dbo.[gold.fact_sales] f
LEFT JOIN dbo.[gold.dim_products] p
ON f.product_key = p.product_key
),

product_aggregation AS (

-- 2) Product Aggregation: Summarise Key metrics at the product level
SELECT
product_id,
product_name,
cost,
price,
product_line,
subcategory,
start_date,
product_age,
maintenance,
COUNT(DISTINCT customer_key) AS total_custoemrs,
COUNT(DISTINCT order_number) AS total_orders,
SUM(quantity) AS total_quantity_sold,
SUM(sales_amount) AS total_sales,
CASE 
    WHEN SUM(sales_amount) >= 500000 THEN 'High-performer'
    WHEN SUM(sales_amount)  >= 200000  AND  SUM(sales_amount) < 500000 THEN 'Mid-RAGE'
    ELSE 'low-performer'
END AS product_performance,

MIN(order_date) AS first_order_date,
MAX(order_date)AS last_order_date,
DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan

FROM base_query
GROUP BY
product_id,
product_name,
cost,
price,
product_line,
subcategory,
start_date,
product_age,
maintenance
)

SELECT 
product_id,
product_name,
cost,
price,
product_line,
subcategory,
start_date,
product_age,
maintenance,
total_custoemrs,
total_orders,
total_quantity_sold,
total_sales,
product_performance,


-- calculating avg_order_revenue
CASE 
    WHEN total_orders = 0 THEN 0
    ELSE total_sales / total_orders 
END AS avg_order_revenue,

first_order_date,
last_order_date,
lifespan,
-- average Monthly revenue 

CASE 
    WHEN lifespan = 0 THEN total_sales 
    ELSE total_sales / lifespan 
END AS average_monthly_revenue,

-- calculating recency (month since last sales
DATEDIFF(month, last_order_date, GETDATE() ) AS recency

FROM product_aggregation
