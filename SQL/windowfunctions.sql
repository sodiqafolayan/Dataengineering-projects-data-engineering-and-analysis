/*
README.md
This repo will contain my sql queries against the data linked below. This is from ...
Udacity course on sql-for-data-analysis
https://video.udacity-data.com/topher/2020/May/5eb5533b_parch-and-posey/parch-and-posey.sql
*/


/*
Still create a running total of standard_amt_usd (in the orders table) over order time, 
but this time, date truncate occurred_at by year and partition by that same year-truncated 
occurred_at variable. Your final table should have three columns: One with the amount being 
added for each row, one for the truncated date, and a final column with the running total 
within each year.
*/
SELECT standard_amt_usd, 
       DATE_TRUNC('year', occurred_at),
       SUM(standard_amt_usd) OVER (PARTITION BY DATE_TRUNC('year', occurred_at) ORDER BY occurred_at) AS running_total
FROM orders

/*
Select the id, account_id, and total variable from the orders table, then create a column called 
total_rank that ranks this total amount of paper ordered (from highest to lowest) for each account 
using a partition. Your final table should have these four columns
*/
SELECT id, 
	   account_id, 
       total,
       RANK() OVER (PARTITION BY account_id ORDER BY total DESC) total_rank
       FROM orders

/*

*/
SELECT id,
       account_id,
       DATE_TRUNC('year',occurred_at) AS year,
       DENSE_RANK() OVER account_year_window AS dense_rank,
       total_amt_usd,
       SUM(total_amt_usd) OVER account_year_window AS sum_total_amt_usd,
       COUNT(total_amt_usd) OVER account_year_window AS count_total_amt_usd,
       AVG(total_amt_usd) OVER account_year_window AS avg_total_amt_usd,
       MIN(total_amt_usd) OVER account_year_window AS min_total_amt_usd,
       MAX(total_amt_usd) OVER account_year_window AS max_total_amt_usd
FROM orders
WINDOW account_year_window AS (PARTITION BY account_id ORDER BY DATE_TRUNC('year',occurred_at))

/*
Imagine you're an analyst at Parch & Posey and you want to determine how the current order's 
total revenue ("total" meaning from sales of all types of paper) compares to the next order's 
total revenue. Modify Derek's query from the previous video in the SQL Explorer below to perform 
this analysis. You'll need to use occurred_at and total_amt_usd in the orders table along with 
LEAD to do so. In your query results, there should be four columns: occurred_at, total_amt_usd, 
lead, and lead_difference.
*/
SELECT groupedDate,
       totalOrderSum,
       LEAD(totalOrderSum) OVER (ORDER BY groupedDate) AS lead,
       LEAD(totalOrderSum) OVER (ORDER BY groupedDate) - totalOrderSum AS lead_difference
FROM 
       (SELECT occurred_at groupedDate,
       SUM(total_amt_usd) AS totalOrderSum
       FROM orders 
       GROUP BY 1) t1
