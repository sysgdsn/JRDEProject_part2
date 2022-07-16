
# JR DE Project Part 2
## Q1
Create a table called order_products_prior by using the last SQL query you created from the previous assignment. It should be similar to below (note you need to replace the s3 bucket name “imba” to yours own bucket name):

```SQL
CREATE TABLE order_products_prior WITH (external_location = 's3://imba-jack/features/order_products_prior/', format = 'parquet')
as (SELECT a.*,
b.product_id,
b.add_to_cart_order,
b.reordered
FROM orders a
JOIN order_products b
ON a.order_id = b.order_id
WHERE a.eval_set = 'prior')
```

## Q2
Create a SQL query (user_features_1). Based on table orders, for each user, calculate the max order_number, the sum of days_since_prior_order and the average of days_since_prior_order.

```SQL
SELECT
max(order_number) AS max_order_number,
sum(days_since_prior_order) AS sum_days_since_prior_order,
avg(days_since_prior_order) AS avg_days_since_prior_order
FROM "prd"."orders"
GROUP BY user_id
ORDER BY user_id;
```

## Q3
Create a SQL query (user_features_2). Similar to above, based on table order_products_prior, for each user calculate the total number of products, total number of distinct products, and user reorder ratio(number of reordered = 1 divided by number of order_number > 1)


```SQL
SELECT 
user_id,
count(product_id) AS total_number_of_products,
count(DISTINCT product_id) AS total_number_of_distinct_products,
ROUND(CAST((COUNT(if(reordered=1,1,NULL))) AS DOUBLE) / COUNT(if(order_number>1,1,NULL)),3)  AS ratio

FROM "prd"."order_products_prior" 
GROUP BY user_id
ORDER BY user_id
```

## Q4


Create a SQL query (up_features). Based on table order_products_prior, for each user and product, calculate the total number of orders, minimum order_number, maximum order_number and average add_to_cart_order.

```SQL
SELECT 
user_id,
product_id,
SUM(order_number) AS sum_order_number,
MIN(order_number) AS min_order_number,
MAX(order_number) AS max_order_number,
AVG(add_to_cart_order) AS avg_add_to_cart_order

FROM "prd"."order_products_prior" 
GROUP BY user_id,product_id
ORDER BY user_id
```

# Q5

Create a SQL query (prd_features). Based on table order_products_prior, first write a sql query to calculate the sequence of product purchase for each user, and name it product_seq_time (For example, if a user first time purchase a product A, mark it as 1. If it’s the second time a user purchases a product A, mark it as 2). Below are some examples:

```SQL
SELECT
product_id,
COUNT(reordered) AS count_reordered,
SUM(reordered) AS sum_reordered,
COUNT(if(T1.product_seq_time = 1,1,null)) AS Count_of_Product_Seq_time_is_1,
COUNT(if(T1.product_seq_time = 2,1,null)) AS Count_of_Product_Seq_time_is_2

FROM
(SELECT 
user_id,
product_id,
order_number,
reordered,
rank() OVER(PARTITION BY user_id,product_id ORDER BY order_number) AS product_seq_time

FROM "prd"."order_products_prior"
GROUP BY user_id,product_id,order_number,reordered
ORDER BY user_id,product_id,order_number) AS T1

GROUP BY product_id
ORDER BY product_id
```