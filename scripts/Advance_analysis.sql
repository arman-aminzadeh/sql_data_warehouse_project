*/
===========================================
-- sales trand for year (Over time)
=======================================
*/
SELECT 
extract (year from order_date) as years,
extract (month from order_date) as month,
sum(sales_amount),
count(distinct customer_id) as total_customers,
sum(quantity) as totsl_quantity
FROM gold.fact_sales
where order_date is not null
group by extract (year from order_date), extract(month from order_date)
order by years, month

============================================
-- Cumulative Analysis
-- Running total sales by year
-- Moving Average of sales by month
============================================
select
order_date,
total_sales,
sum(total_sales) over(partition by DATE_TRUNC('year', order_date) order by order_date) as running_total_sales,
round(avg(avg_price) over(order by order_date), 2) as moving_avg
from
(
select
DATE_TRUNC('year', order_date) AS order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by date_trunc('year', order_date)
order by order_date
)t

===============================================================
-- performance analysis
-- analyze the yearly performance of products
-- by comparing each product's sales to both its average sales
-- performance and the previous year's sales
===========================================================

with yearly_product_sales as (

select
extract(year from f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
--p.subcategory
from gold.fact_sales f
left join gold.dim_products p
on f.product_number = p.product_number
where order_date is not null
group by p.product_name, extract(year from f.order_date) 
order by order_year

)
select
order_year,
product_name,
current_sales,
round(avg(current_sales) over(partition by product_name), 0) as avg_sales,
current_sales - round(avg(current_sales) over(partition by product_name), 0) as diff_avg,
case when current_sales - round(avg(current_sales) over(partition by product_name), 0) < 0 then 'Below'
	when current_sales - round(avg(current_sales) over(partition by product_name), 0) > 0 then 'Above'
	else 'Same'
end as flag,
lag(current_sales) over(partition by product_name order by order_year) as py_year_salse
from yearly_product_sales
order by product_name, order_year

============================================================
-- Which categories contribute the most to overall sales?
==========================================================

with category_sales as (
select
category,
sum(sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_number = p.product_number
where order_date is not null
group by category
)
select 
category,
total_sales,
sum(total_sales) over() as overall_sales,
concat(round((total_sales / sum(total_sales) over())*100, 2), ' %') as percentage_of_sales
from category_sales

==================================================================================
--Data Segmentation
-- segment products into cost ranges and count how many products fall into each segment.
===============================================================================
with product_info as 
(
select
product_name,
cost,
case when cost < 500 then 'Less'
	when cost between 500 and 1300 then 'Medium'
	else 'Premium'
end as cost_range
from gold.dim_products
)
select 
count(product_name),
sum(cost) as total_cost,
cost_range
from product_info
group by cost_range


=================================================================================
--Costomer segment based on their history and spending
-- VIP: costomer with at least 12 months history and spending more than 5000.
-- Regular: costomer with 12 months history and spending 5000 or less.
-- NEW: costomer with less than 12 months history.
-- find the total customer by each cateory
=================================================================================


with customer_spending as(
select
c.customer_id,
sum(f.price) as total_spend, 
max(f.order_date) as first_order,
min(f.order_date) as last_order,
(
	EXTRACT(YEAR FROM AGE(MAX(f.order_date), MIN(f.order_date))) * 12
	+
	EXTRACT(MONTH FROM AGE(MAX(f.order_date), MIN(f.order_date)))
)as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_id = c.customer_id
group by c.customer_id

)

select 
customer_segment,
count(customer_id) as total_customer
from(
	select 
	customer_id,
	total_spend,
	lifespan,
	case when lifespan >= 12 and total_spend > 5000 then 'VIP'
		when lifespan >= 12 and total_spend < 5000 then 'Regular'
		else 'New'
	end as customer_segment
	from customer_spending
)t
group by customer_segment
order by total_customer DESC

----------------------------------------------------------
----------------------------------------------------------

'
====================
Customer Report
================
purpose:
	- this report consolidate key customer metrics and behaviors
Highlights:
	1- Gather essential fields such as name, ages, and transactions details.
	2- segment customer into categories and range group 
	3- aggregate customer-level metrics:
	- total orders
	- total sales
	- total quantity
	- total products
	- lifespan
	4- Calculate vsluble KPIs:
	- recency (months since last order)
	-average ordervalue
	-average monthly spent
=========================================================================
'
create view gold.report_customers as
with base_query as (
	select
	f.order_number,
	f.product_number,
	f.order_date,
	f.sales_amount,
	f.quantity,
	c.customer_id,
	c.customer_number,
	concat(c.first_name, ' ', c.last_name) as customer_name,
	extract('year' from AGE(CURRENT_DATE, c.birthdate)) as age
	from gold.fact_sales f
	left join gold.dim_customers c
	on f.customer_id = c.customer_id
	where order_date is not null
),
customer_aggregation as(

	select
		customer_id,
		customer_number,
		customer_name,
		age,
		count(distinct order_number) as total_orders,
		sum(sales_amount) as total_sales,
		sum(quantity) as total_quantity,
		count(distinct product_number) as total_products,
		max(order_date) as last_order,
		(
			EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12
			+
			EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))
		)as lifespan
	from base_query
	group by 
		customer_id,
		customer_number,
		customer_name,
		age
)

select

customer_id,
customer_number,
customer_name,
age,
case when age < 30 then 'Young'
	when age between 30 and 55 then'Middle Age'
	else 'Senior'
end as age_category,
case when lifespan >= 12 and total_sales > 5000 then 'VIP'
		when lifespan >= 12 and total_sales < 5000 then 'Regular'
		else 'New'
	end as customer_segment,
last_order,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
case when total_sales = 0 then 0
	else total_sales/total_orders
end as avo,

case when lifespan = 0 then total_sales
	else round(total_sales/lifespan,2)
end as average_monthly_spend

from customer_aggregation
