/*
README.md
This repo will contain my sql queries against the data linked below. This is from 
Udacity course on sql-for-data-analysis
https://video.udacity-data.com/topher/2020/May/5eb5533b_parch-and-posey/parch-and-posey.sql
*/


/*
Find the total amount of poster_qty paper ordered in the orders table. ...
*/
SELECT SUM(poster_qty) posterTotal
FROM orders;


/*
Find the total amount of standard_qty paper ordered in the orders table.
*/
SELECT SUM(standard_qty) standardTotal
FROM orders;


/*
Find the total dollar amount of sales using the total_amt_usd in the orders table
*/
SELECT SUM(total_amt_usd) grandAmoutUSD
FROM orders;


/*
Find the total amount spent on standard_amt_usd and gloss_amt_usd paper for each order 
in the orders table. This should give a dollar amount for each order in the table.
*/
SELECT SUM(standard_amt_usd) standardSum,
	   SUM(gloss_amt_usd) glossSum
FROM orders;

/*
Find the standard_amt_usd per unit of standard_qty paper. Your solution should use both 
an aggregation and a mathematical operator.

*/
SELECT SUM(standard_amt_usd)/SUM(standard_qty) standarUnitPrice
FROM orders;

/*
When was the earliest order ever placed? You only need to return the date.
*/
SELECT MIN(occurred_at)
FROM orders limit 5;


/*
Try performing the same query as in question 1 without using an aggregation function.
*/
SELECT occurred_at
FROM orders 
ORDER BY occurred_at
LIMIT 1;

/*

When did the most recent (latest) web_event occur?
*/
SELECT MAX(occurred_at)
FROM web_events 
LIMIT 1;

/*
Try to perform the result of the previous query without using an aggregation function.
*/
SELECT occurred_at
FROM web_events 
ORDER BY occurred_at DESC
LIMIT 1;


/*
Find the mean (AVERAGE) amount spent per order on each paper type, as well as the mean 
amount of each paper type purchased per order. Your final answer should have 6 values - 
one for each paper type for the average number of sales, as well as the average amount.
*/
SELECT AVG(standard_qty) mean_standard, 				   
       AVG(gloss_qty) mean_gloss, 
       AVG(poster_qty) mean_poster,         
       AVG(standard_amt_usd) mean_standard_usd, 
       AVG(gloss_amt_usd) mean_gloss_usd, 
       AVG(poster_amt_usd) mean_poster_usd
FROM orders;

/*
Which account (by name) placed the earliest order? Your solution should have the account 
name and the date of the order.
*/
SELECT a.name accountName, MAX(o.occurred_at) earliestOrder
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY a.name
LIMIT 1;

/*
Find the total sales in usd for each account. You should include two columns - the total sales 
for each company's orders in usd and the company name.
*/
SELECT a.name accountName, SUM(o.total_amt_usd) sumAmount
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY a.name;

/*
Via what channel did the most recent (latest) web_event occur, which account was associated with 
this web_event? Your query should return only three values - the date, channel, and account name.
*/
SELECT a.name accountName, 
	   w.channel channel, 
       w.occurred_at earliestEvent
FROM accounts a 
JOIN web_events w
ON a.id = w.account_id
ORDER BY w.occurred_at DESC
LIMIT 1;


/*
Find the total number of times each type of channel from the web_events was used. Your final table should 
have two columns - the channel and the number of times the channel was used
*/
SELECT  channel, 
        COUNT(channel) channelCount
FROM web_events
GROUP BY channel
ORDER BY channel DESC

/*
Who was the primary contact associated with the earliest web_event?
*/
SELECT a.primary_poc primaryPoc,
	   w.occurred_at earliestEvent
FROM accounts a
JOIN web_events w
ON a.id = w.account_id
ORDER BY w.occurred_at DESC
LIMIT 1;


/*
What was the smallest order placed by each account in terms of total usd. 
Provide only two columns - the account name and the total usd. Order from 
smallest dollar amounts to largest.

*/
SELECT a.name accountName,
	   MIN(total_amt_usd) smallestOrderUsd
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY 1
ORDER BY smallestOrderUsd;

/*
Find the number of sales reps in each region. Your final table should have two 
columns - the region and the number of sales_reps. Order from fewest reps to most reps.
*/
SELECT a.name, MIN(total_amt_usd) smallest_order
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY a.name
ORDER BY smallest_order;

/*
For each account, determine the average amount of each type of paper they purchased across 
their orders. Your result should have four columns - one for the account name and one for 
the average quantity purchased for each of the paper types for each account.
*/
SELECT a.name accountName,
	   AVG(o.standard_qty) avgStandardPaper,
       AVG(o.poster_qty) avgPosterPaper,
       AVG(o.gloss_qty) avgGlossPaper
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY accountName;

/*
For each account, determine the average amount spent per order on each paper type. Your 
result should have four columns - one for the account name and one for the average amount 
spent on each paper type.
*/
SELECT a.name accountName,
	   AVG(o.standard_amt_usd) avgStandardPaper,
       AVG(o.poster_amt_usd) avgPosterPaper,
       AVG(o.gloss_amt_usd) avgGlossPaper
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY accountName;

/*
Determine the number of times a particular channel was used in the web_events table for each sales 
rep. Your final table should have three columns - the name of the sales rep, the channel, and the 
number of occurrences. Order your table with the highest number of occurrences first.
*/
SELECT s.name repName,
	   w.channel channel,
       COUNT(w.channel) channelCount
FROM accounts a
JOIN web_events w
ON a.id = w.account_id
JOIN sales_reps s
ON s.id = a.sales_rep_id
GROUP BY 1, 2
ORDER BY 1


/*
Determine the number of times a particular channel was used in the web_events table for each region. 
Your final table should have three columns - the region name, the channel, and the number of occurrences. 
Order your table with the highest number of occurrences first.
*/
SELECT r.name regionName,
	   w.channel channel,
       COUNT(w.channel) channelCount
FROM accounts a
JOIN web_events w
ON a.id = w.account_id
JOIN sales_reps s
ON s.id = a.sales_rep_id
JOIN region r
ON r.id = s.region_id
GROUP BY 1, 2
ORDER BY 1

/*
Use DISTINCT to test if there are any accounts associated with more than one region.
*/
SELECT DISTINCT a.name accName, r.name regionName
FROM accounts a
JOIN sales_reps s
ON s.id = a.sales_rep_id
JOIN region r
ON r.id = s.region_id
ORDER BY 1

/*
Have any sales reps worked on more than one account?
*/
SELECT DISTINCT sales_rep_id, name
FROM accounts
ORDER BY 1;

/*
How many of the sales reps have more than 5 accounts that they manage?
*/
SELECT s.name repName, 
	   COUNT(a.name) accountCount
FROM sales_reps s
JOIN accounts a
ON s.id = a.sales_rep_id
GROUP BY repName
HAVING COUNT(a.name) > 5;

/*
How many accounts have more than 20 orders?
*/
SELECT COUNT(*)
FROM
  (SELECT a.name accountName, 
  COUNT(o.total) totalCount
  FROM accounts a
  JOIN orders o
  ON a.id = o.account_id
  GROUP BY accountName
  HAVING COUNT(o.total) > 20) AS t1

/*
Which account has the most orders?
*/
SELECT a.name accName, COUNT(*) orderCount
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY accName
ORDER BY COUNT(*) DESC
LIMIT 1;

/*
Which accounts spent more than 30,000 usd total across all orders?
*/
SELECT a.name accName, 
	   SUM(o.total_amt_usd) totalUSD
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY accName
HAVING SUM(o.total_amt_usd) > 30000
ORDER BY SUM(o.total_amt_usd) DESC;

/*
Which accounts spent less than 1,000 usd total across all orders?
*/
SELECT a.name accName, 
	   SUM(o.total_amt_usd) totalUSD
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY accName
HAVING SUM(o.total_amt_usd) < 1000
ORDER BY SUM(o.total_amt_usd) DESC;


/*
Which account has spent the most with us?
*/
SELECT a.name accName, 
	   SUM(o.total_amt_usd) totalUSD
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY accName
ORDER BY SUM(o.total_amt_usd) DESC
LIMIT 1;


/*
Which account has spent the least with us?
*/
SELECT a.name accName, 
	   SUM(o.total_amt_usd) totalUSD
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY accName
ORDER BY SUM(o.total_amt_usd)
LIMIT 1;

/*
Which accounts used facebook as a channel to contact customers more than 6 times?
*/
SELECT a.name accName, 
	   COUNT(w.channel) channelCount
FROM accounts a
JOIN web_events w
ON a.id = w.account_id
WHERE w.channel = 'facebook'
GROUP BY accName
HAVING COUNT(w.channel) > 6
ORDER BY channelcount DESC;


/*
Which account used facebook most as a channel?
*/
SELECT a.name accName, 
	   COUNT(w.channel) channelCount
FROM accounts a
JOIN web_events w
ON a.id = w.account_id
WHERE w.channel = 'facebook'
GROUP BY accName
ORDER BY channelcount DESC
LIMIT 1;

/*
Which channel was most frequently used by most accounts?
*/
SELECT channel, 
	   COUNT(channel) channelCount
FROM web_events
GROUP BY channel
ORDER BY channelCount DESC
LIMIT 1;


/*
Find the sales in terms of total dollars for all orders in each year, ordered from greatest to least. 
*/
SELECT SUM(total_amt_usd), 
	   DATE_PART('year', occurred_at) orderYear
FROM orders
GROUP BY 2
ORDER BY 1 DESC;

/*
Which month did Parch & Posey have the greatest sales in terms of total dollars? Are all months evenly 
represented by the dataset?
*/
SELECT SUM(total_amt_usd),
	   COUNT(total) monthOrderCount,
	   DATE_PART('month', occurred_at) orderMonth
FROM orders
GROUP BY 3
ORDER BY 1 DESC;


/*
Which year did Parch & Posey have the greatest sales in terms of total number of orders? Are all years 
evenly represented by the dataset?
My choice of BETWEEN 2014 AND 2016 is because there are very few sales that occurred both in 2013 and 
2017 (12, and 1 respectively) as the beginning year and end of business. So it would be unfair to 
compare those two years to other years
*/
SELECT COUNT(total) monthOrderCount,
	   DATE_PART('year', occurred_at) orderYear
FROM orders
WHERE occurred_at BETWEEN '2014' AND '2016'
GROUP BY 2
ORDER BY 1 DESC;

/*
Which month did Parch & Posey have the greatest sales in terms of total number of orders? Are all months 
evenly represented by the dataset?
*/
SELECT COUNT(total) monthOrderCount,
	   DATE_PART('month', occurred_at) orderMonth
FROM orders
WHERE occurred_at BETWEEN '2014-01-01' AND '2016-12-31'
GROUP BY 2
ORDER BY 1 DESC;


/*
In which month of which year did Walmart spend the most on gloss paper in terms of dollars?
*/
SELECT SUM(gloss_qty),
	DATE_TRUNC('month', occurred_at)
FROM orders
JOIN accounts
ON accounts.id = orders.account_id
WHERE accounts.name = 'Walmart'
GROUP BY 2
ORDER BY 1 DESC;


/*
Write a query to display for each order, the account ID, total amount of the order, and the level 
of the order - ‘Large’ or ’Small’ - depending on if the order is $3000 or more, or smaller than $3000.
*/
SELECT account_id,
	   total_amt_usd,
       CASE WHEN total_amt_usd > 3000 THEN 'Large' 				
       ELSE 'Small' END AS orderLevel
FROM orders
ORDER BY total_amt_usd DESC

/*
Write a query to display the number of orders in each of three categories, based on the total number of 
items in each order. The three categories are: 'At Least 2000', 'Between 1000 and 2000' and 'Less than 1000'
*/
SELECT CASE
	WHEN total >= 2000 THEN 'At Least 2000'
       WHEN total >= 1000 AND total < 2000 THEN 'Between 1000 and 2000' 
       ELSE 'Less than 1000' 
       END AS orderCategories,
       COUNT(*) categoryCount
FROM orders
GROUP BY 1
ORDER BY 2 DESC;


/*
We would like to understand 3 different levels of customers based on the amount associated with their purchases. 
The top level includes anyone with a Lifetime Value (total sales of all orders) greater than 200,000 usd. 
The second level is between 200,000 and 100,000 usd. The lowest level is anyone under 100,000 usd. Provide a 
table that includes the level associated with each account. You should provide the account name, the total sales 
of all orders for the customer, and the level. Order with the top spending customers listed first.
*/
SELECT a.name,
	SUM(o.total_amt_usd) totalPurchase,
       CASE WHEN SUM(o.total_amt_usd) > 200000 THEN 'Top Level'
            WHEN SUM(o.total_amt_usd) BETWEEN 100000 AND 200000 THEN 'Mid Level'
            ELSE 'Low Level' 
            END AS customerLevel
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY 1
ORDER BY 2 DESC;

/*
We would now like to perform a similar calculation to the first, but we want to obtain the total 
amount spent by customers only in 2016 and 2017. Keep the same levels as in the previous question. 
Order with the top spending customers listed first.
*/
SELECT a.name,
	SUM(o.total_amt_usd) totalPurchase,
       CASE WHEN SUM(o.total_amt_usd) > 200000 THEN 'Top Level'
            WHEN SUM(o.total_amt_usd) BETWEEN 100000 AND 200000 THEN 'Mid Level'
            ELSE 'Low Level' END AS customerLevel
FROM accounts a
JOIN orders o
ON a.id = o.account_id
WHERE DATE_PART('year', o.occurred_at) BETWEEN '2016' AND '2017'
GROUP BY 1
ORDER BY 2 DESC;
       		

/*
We would like to identify top performing sales reps, which are sales reps associated with more 
than 200 orders. Create a table with the sales rep name, the total number of orders, and a column 
with top or not depending on if they have more than 200 orders. Place the top sales people first 
in your final table.

*/
SELECT s.name repName,
	   COUNT(*) orderNumber,
       CASE WHEN COUNT(*) > 200 THEN 'top'
       ELSE 'not' END AS topOrNot
FROM sales_reps s
JOIN accounts a
ON s.id = a.sales_rep_id
JOIN orders o
ON a.id = o.account_id
GROUP BY 1
ORDER BY 2 DESC;


/*
The previous didn't account for the middle, nor the dollar amount associated with the sales. 
Management decides they want to see these characteristics represented as well. We would like 
to identify top performing sales reps, which are sales reps associated with more than 200 orders 
or more than 750000 in total sales. The middle group has any rep with more than 150 orders or 
500000 in sales. Create a table with the sales rep name, the total number of orders, total sales 
across all orders, and a column with top, middle, or low depending on this criteria. Place the top 
sales people based on dollar amount of sales first in your final table. You might see a few upset 
sales people by this criteria!
*/
SELECT s.name repName,
	COUNT(*) orderNumber,
       CASE WHEN COUNT(*) > 200 OR SUM(o.total_amt_usd) > 750000 THEN 'top'
            WHEN COUNT(*) BETWEEN 150 AND 200 OR SUM(o.total_amt_usd) BETWEEN 500000 AND 750000 THEN 'mid'
            ELSE 'low' END AS topMidLow
FROM sales_reps s
JOIN accounts a
ON s.id = a.sales_rep_id
JOIN orders o
ON a.id = o.account_id
GROUP BY 1
ORDER BY 2 DESC;
