/*
README.md
This repo will contain my sql queries against the data linked below. This is from 
Udacity course on sql-for-data-analysis
https://video.udacity-data.com/topher/2020/May/5eb5533b_parch-and-posey/parch-and-posey.sql
*/


/*
We want to find the average number of events for each day for each channel.
*/
SELECT channel,
       AVG(event_per_day)
FROM
    (SELECT channel,
           DATE_TRUNC('day', occurred_at),
           COUNT(*) event_per_day
     FROM web_events
     GROUP BY 1, 2) subQ
GROUP BY 1;

/*
Use DATE_TRUNC to pull month level information about the first
order ever place in the orders table.

Use the result of the previous query to find only the orders that
took place in the same month and year as the first order, and then
pull the average for each type of paper qty in this month

*/

SELECT DATE_TRUNC('month', MIN(occurred_at))
        FROM orders

SELECT AVG(standard_qty) standardAvg,
	   AVG(poster_qty) posterAvg,
       AVG(gloss_qty) glossAvg
FROM orders
WHERE DATE_TRUNC('month', occurred_at) = 
       (SELECT DATE_TRUNC('month', MIN(occurred_at))
        FROM orders)

/*
Provide the name of the sales_rep in each region with the largest amount of total_amt_usd sales

*/
/* STEP 1: Join all tables and display total sum sales for each rep for each region */
SELECT  s.name repName,
		r.name regionName,
        SUM(o.total_amt_usd) totalAmt
FROM region r
JOIN sales_reps s
ON r.id = s.region_id
JOIN accounts a
ON s.id = a.sales_rep_id
JOIN orders o
ON a.id = o.account_id
GROUP BY 1, 2;

/* STEP 2: Create an outer query to select only regions and Max total amount sales for each region */

SELECT regionName,
	   MAX(totalAmt) totalMax
FROM
        (SELECT  s.name repName,
                r.name regionName,
                SUM(o.total_amt_usd) totalAmt
        FROM region r
        JOIN sales_reps s
        ON r.id = s.region_id
        JOIN accounts a
        ON s.id = a.sales_rep_id
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1, 2) subQ1
GROUP BY 1
ORDER BY 2;

/* STEP 3: To link rep names, i replicated subQ1 as subQ3, selected rep name from there and joined all tables */
SELECT subQ3.repName,
	   subQ2.regionName,
       subQ2.totalMax
FROM
        (SELECT regionName,
            MAX(totalAmt) totalMax
        FROM
                (SELECT  s.name repName,
                        r.name regionName,
                        SUM(o.total_amt_usd) totalAmt
                FROM region r
                JOIN sales_reps s
                ON r.id = s.region_id
                JOIN accounts a
                ON s.id = a.sales_rep_id
                JOIN orders o
                ON a.id = o.account_id
                GROUP BY 1, 2) subQ1
        GROUP BY 1
        ORDER BY 2) subQ2
    JOIN 
        (SELECT  s.name repName,
                r.name regionName,
                SUM(o.total_amt_usd) totalAmt
        FROM region r
        JOIN sales_reps s
        ON r.id = s.region_id
        JOIN accounts a
        ON s.id = a.sales_rep_id
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1, 2) subQ3
ON subQ2.regionName = subQ3.regionName AND subQ2.totalMax = subq3.totalAmt;

/*
For the region with the largest (sum) of sales total_amt_usd, how many total (count) orders were placed?
*/
/* Option 1 */
SELECT  r.name regionName,
 		COUNT(o.total) orderCount,
        SUM(o.total_amt_usd) totalAmt
FROM region r
JOIN sales_reps s
ON r.id = s.region_id
JOIN accounts a
ON s.id = a.sales_rep_id
JOIN orders o
ON a.id = o.account_id
GROUP BY 1
ORDER BY 2 DESC;

/* Option 2: Using Sub-query */

SELECT regionName, MAX(orderCount)
FROM
    (SELECT  r.name regionName,
            COUNT(o.total) orderCount,
            SUM(o.total_amt_usd) totalAmt
    FROM region r
    JOIN sales_reps s
    ON r.id = s.region_id
    JOIN accounts a
    ON s.id = a.sales_rep_id
    JOIN orders o
    ON a.id = o.account_id
    GROUP BY 1
    ORDER BY 2 DESC) subQ1
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

/*
How many accounts had more total purchases than the account name which has bought the 
most standard_qty paper throughout their lifetime as a customer?
*/
/* Step 1: Get the account name with the most purchase for standard qty paper */
SELECT a.name accName,
 		SUM(o.standard_amt_usd) standardPurchase,
 		SUM(o.total_amt_usd) totalPurchase
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY 1
ORDER BY 2 DESC;

/* Step 2:  Count the account name having their total purchase greater that the account with the highest standard paper purchase*/
SELECT COUNT(*)
FROM 
(SELECT a.name accountName,
	   SUM(o.total) orderTotal
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY 1
HAVING SUM(o.total) > 
(SELECT totalOrder
    FROM
        (SELECT a.name accName,
                SUM(o.standard_qty) standardPurchase,
                SUM(o.total) totalOrder
        FROM accounts a
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1) subQ1)) subQ2

/*
For the customer that spent the most (in total over their lifetime as a customer) 
total_amt_usd, how many web_events did they have for each channel?
*/

SELECT subQ1.accName, 
       subQ2.webChannel,
       subQ2.channelCount
FROM
        (SELECT a.name accName,
            SUM(o.total_amt_usd) highestPurchase
        FROM accounts a
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1) subQ1
JOIN
        (SELECT a.name accountName,
            w.channel webChannel,
            COUNT(*) channelCount
        FROM accounts a
        JOIN web_events w
        ON a.id = w.account_id
        GROUP BY 1, 2) subQ2

ON subQ1.accName = subQ2.accountName

/*
What is the lifetime average amount spent in terms of total_amt_usd for the top 10 total spending accounts
*/
SELECT AVG(totalSum)
FROM
    (SELECT SUM(totalSpent) totalSum
    FROM
        (SELECT a.name accName,
                SUM(o.total_amt_usd) totalSpent
        FROM accounts a
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10) subQ1
    ) subQ2;

