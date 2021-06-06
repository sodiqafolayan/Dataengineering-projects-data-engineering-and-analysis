/*
README.md
This repo will contain my sql queries against the data linked below. This is from ...
Udacity course on sql-for-data-analysis
https://video.udacity-data.com/topher/2020/May/5eb5533b_parch-and-posey/parch-and-posey.sql
*/

/*
Try pulling all the data from the accounts table, and all the data from the orders table.
*/
SELECT aacounts.*, orders.*
FROM accounts
JOIN orders
ON accounts.id = orders.account_id;


/*
Try pulling standard_qty, gloss_qty, and poster_qty from the orders table, and the website and 
the primary_poc from the accounts table.
*/
SELECT o.standard_qty, o.gloss_qty, o.poster_qty, a.website, a.primary_poc
FROM orders o
JOIN accounts a
ON a.id = o.account_id;

/*
Provide a table for all web_events associated with account name of Walmart. There should be three columns. 
Be sure to include the primary_poc, time of the event, and the channel for each event. Additionally, 
you might choose to add a fourth column to assure only Walmart events were chosen
*/
SELECT a.primary_poc primaryContact, 
	w.occurred_at occurredAt,
    w.channel
FROM accounts a
JOIN web_events w
ON a.id = w.account_id
WHERE a.name = 'Walmart';

/*
Provide a table that provides the region for each sales_rep along with their associated accounts. 
Your final table should include three columns: the region name, the sales rep name, and the 
account name. Sort the accounts alphabetically (A-Z) according to account name.
*/
SELECT s.name rep_name, 
	r.name region_name, 
    a.name account_name
FROM region r
JOIN sales_reps s
ON r.id = s.region_id
JOIN accounts a
ON s.id = a.sales_rep_id;

/*
Provide the name for each region for every order, as well as the account name and the unit price they 
paid (total_amt_usd/total) for the order. Your final table should have 3 columns: region name, account name, 
and unit price. A few accounts have 0 for total, so I divided by (total + 0.01) to assure not dividing by zero.
*/
SELECT a.name account_name,
	   r.name regionName,
       (o.total_amt_usd/(o.total+0.01)) unitPrice
FROM accounts a
JOIN orders o
ON a.id = o.account_id
JOIN sales_reps s
ON s.id = a.sales_rep_id
JOIN region r
ON r.id = s.region_id; 

/*
Provide a table that provides the region for each sales_rep along with their associated accounts. This time only for 
the Midwest region. Your final table should include three columns: the region name, the sales rep name, and the 
account name. Sort the accounts alphabetically (A-Z) according to account name.
*/
SELECT s.name salesRep,
	   r.name regionName,
       a.name accountName
FROM accounts a
JOIN sales_reps s
ON s.id = a.sales_rep_id
JOIN region r
ON r.id = s.region_id
AND r.name = 'Midwest'
ORDER BY a.name;

/*
Provide a table that provides the region for each sales_rep along with their associated accounts. 
This time only for accounts where the sales rep has a first name starting with S and in the Midwest 
region. Your final table should include three columns: the region name, the sales rep name, and the 
account name. Sort the accounts alphabetically (A-Z) according to account name
*/
SELECT s.name salesRep,
	   r.name regionName,
       a.name accountName
FROM accounts a
JOIN sales_reps s
ON s.id = a.sales_rep_id
AND s.name LIKE 'S%'
JOIN region r
ON r.id = s.region_id
AND r.name = 'Midwest'
ORDER BY a.name;

/*
Provide a table that provides the region for each sales_rep along with their associated accounts. 
This time only for accounts where the sales rep has a last name starting with K and in the 
Midwest region. Your final table should include three columns: the region name, the sales rep name, 
and the account name. Sort the accounts alphabetically (A-Z) according to account name.
*/
SELECT s.name salesRep,
	   r.name regionName,
       a.name accountName
FROM accounts a
JOIN sales_reps s
ON s.id = a.sales_rep_id
AND s.name LIKE '% K%'
JOIN region r
ON r.id = s.region_id
AND r.name = 'Midwest'
ORDER BY a.name;

/*
Provide the name for each region for every order, as well as the account name and the unit price they paid 
(total_amt_usd/total) for the order. However, you should only provide the results if the standard order quantity 
exceeds 100. Your final table should have 3 columns: region name, account name, and unit price. In order 
to avoid a division by zero error, adding .01 to the denominator here is helpful total_amt_usd/(total+0.01).
*/
SELECT s.name salesRep,
	   r.name regionName,
       a.name accountName,
       o.total_amt_usd/(o.total+0.01) unitPrice
FROM orders o
JOIN accounts a
ON a.id = o.account_id
AND o.standard_qty > 100
JOIN sales_reps s
ON s.id = a.sales_rep_id
JOIN region r
ON r.id = s.region_id
ORDER BY a.name;

/*
Provide the name for each region for every order, as well as the account name and the unit price they paid 
(total_amt_usd/total) for the order. However, you should only provide the results if the standard order 
quantity exceeds 100 and the poster order quantity exceeds 50. Your final table should have 3 columns: 
region name, account name, and unit price. Sort for the smallest unit price first. In order to avoid a 
division by zero error, adding .01 to the denominator here is helpful (total_amt_usd/(total+0.01).
*/
SELECT s.name salesRep,
	   r.name regionName,
       a.name accountName,
       o.total_amt_usd/(o.total+0.01) unitPrice
FROM orders o
JOIN accounts a
ON a.id = o.account_id
AND o.standard_qty > 100
AND o.poster_qty > 50
JOIN sales_reps s
ON s.id = a.sales_rep_id
JOIN region r
ON r.id = s.region_id
ORDER BY o.total_amt_usd/(o.total+0.01);

/*
Provide the name for each region for every order, as well as the account name and the unit price they 
paid (total_amt_usd/total) for the order. However, you should only provide the results if the standard 
order quantity exceeds 100 and the poster order quantity exceeds 50. Your final table should have 3 columns: 
region name, account name, and unit price. Sort for the largest unit price first. In order to avoid a 
division by zero error, adding .01 to the denominator here is helpful (total_amt_usd/(total+0.01).
*/
SELECT s.name salesRep,
	   r.name regionName,
       a.name accountName,
       o.total_amt_usd/(o.total+0.01) unitPrice
FROM orders o
JOIN accounts a
ON a.id = o.account_id
AND o.standard_qty > 100
AND o.poster_qty > 50
JOIN sales_reps s
ON s.id = a.sales_rep_id
JOIN region r
ON r.id = s.region_id
ORDER BY o.total_amt_usd/(o.total+0.01) DESC;

/*
What are the different channels used by account id 1001? Your final table should have only 2 columns: 
account name and the different channels. You can try SELECT DISTINCT to narrow down the results to 
only the unique values.
*/
SELECT DISTINCT a.name accountName,
	   w.channel webChannels
FROM accounts a
JOIN web_events w
ON a.id = w.account_id
AND a.id = 1001;

/*
Find all the orders that occurred in 2015. Your final table should have 4 columns: occurred_at, account name, 
order total, and order total_amt_usd.
*/
SELECT o.occurred_at occurredAt,
	   a.name accountName,
       o.total orderTotal,
       o.total_amt_usd totalUSD
FROM accounts a
JOIN orders o
ON a.id = o.account_id
AND o.occurred_at BETWEEN '2015-01-01' AND '2015-12-31'
ORDER BY o.occurred_at DESC;
