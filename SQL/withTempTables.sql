/*
README.md
This repo will contain my sql queries against the data linked below. This is from 
Udacity course on sql-for-data-analysis
https://video.udacity-data.com/topher/2020/May/5eb5533b_parch-and-posey/parch-and-posey.sql
*/

/*
Provide the name of the sales_rep in each region with the largest amount of total_amt_usd sales
*/
WITH t1 AS 
    (SELECT s.name repName,
           r.name regionName,
           SUM(o.total_amt_usd) totalAmt
    FROM region r
    JOIN sales_reps s
    ON r.id = s.region_id
    JOIN accounts a
    ON s.id = a.sales_rep_id
    JOIN orders o
    ON a.id = o.account_id
    GROUP BY 1, 2
    ORDER BY 3),
    
    t2 AS
    (SELECT regionName,
	        MAX(totalAmt) maxAmtPerRegion
    FROM t1
    GROUP BY 1
    ORDER BY 2)
SELECT t1.repName, t1.regionName, t1.totalAmt
FROM t1
JOIN t2
ON t1.regionName = t2.regionName AND t1.totalAmt = t2.maxAmtPerRegion
ORDER BY 3 DESC;

/*
For the region with the largest sales total_amt_usd, how many total orders were placed?
*/
WITH t1 AS 
  (SELECT r.name regionName,
             SUM(o.total_amt_usd) regionTotalAmt
      FROM region r
          JOIN sales_reps s
          ON r.id = s.region_id
          JOIN accounts a
          ON s.id = a.sales_rep_id
          JOIN orders o
          ON a.id = o.account_id
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT 1),

   t2 AS (
   SELECT MAX(regionTotalAmt)
   FROM t1)

SELECT r.name regionName,
	   COUNT(o.total) totalOrder

FROM region r
          JOIN sales_reps s
          ON r.id = s.region_id
          JOIN accounts a
          ON s.id = a.sales_rep_id
          JOIN orders o
          ON a.id = o.account_id
GROUP BY 1
HAVING SUM(o.total_amt_usd) = (SELECT * FROM t2)
       

/*
How many accounts had more total purchases than the account name which has bought the most standard_qty 
paper throughout their lifetime as a customer?
*/
WITH t1 AS 
        (SELECT a.name accName,
                SUM(o.standard_qty) totalStandardPaper,
                SUM(o.total) totalOrder
            FROM accounts a
            JOIN orders o
            ON a.id = o.account_id
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 1),
     t2 AS
     (SELECT totalOrder
      FROM t1),

    t3 AS
    (SELECT a.name accountName,
        SUM(o.total) sumOrder
                FROM accounts a
                JOIN orders o
                ON a.id = o.account_id
    GROUP BY 1
    HAVING SUM(o.total) > (SELECT * FROM t2))

SELECT COUNT(*)
FROM t3



/*
For the customer that spent the most (in total over their lifetime as a customer) total_amt_usd, 
how many web_events did they have for each channel?
*/
WITH t1 AS 
        (SELECT a.id accID, 
                a.name accName,
                w.channel webChannel,
                SUM(o.total_amt_usd) totalUSD
        FROM web_events w
        JOIN accounts a
        ON a.id = w.account_id
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1, 2, 3
        ORDER BY 4 DESC
        LIMIT 1)
SELECT a.name accountName,
       w.channel webChannels,
       COUNT(*)
FROM web_events w
JOIN accounts a
ON a.id = w.account_id AND a.id = (SELECT accID FROM t1)
GROUP BY 1, 2

/*
What is the lifetime average amount spent in terms of total_amt_usd for the top 10 total spending accounts?
*/
WITH t1 AS
    (SELECT a.name accName,
        SUM(o.total_amt_usd) totalAmt
    FROM accounts a
    JOIN orders o
    ON a.id = o.account_id
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10)

SELECT accName,
       AVG(totalAmt)
FROM t1
GROUP BY 1
ORDER BY 2 DESC;

