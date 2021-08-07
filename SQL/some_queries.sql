/*
Provide the name of the sales_rep in each region with the largest amount of total_amt_usd sales.
*/
WITH t1 AS
	(SELECT s.name rep_name,
           s.region_id rep_region,
           SUM(o.total_amt_usd) total_rep_sales
    FROM sales_reps s
    JOIN accounts a
    ON s.id = a.sales_rep_id
    JOIN orders o
    ON a.id = o.account_id
    GROUP BY 1, 2
    ORDER BY 3 DESC),
t2 AS
  (SELECT rep_region,
         MAX(total_rep_sales) highest_sales
  FROM t1
  GROUP BY rep_region)
SELECT t1.rep_name
FROM t1
JOIN t2
ON t1.total_rep_sales = t2.highest_sales


/*
For the region with the largest (sum) of sales total_amt_usd, how many total (count) orders were placed?
*/

SELECT r.name,
	   SUM(o.total)
FROM region r
JOIN sales_reps s
ON r.id = s.region_id
JOIN accounts a
ON s.id = a.sales_rep_id
JOIN orders o
ON a.id = o.account_id
GROUP BY 1
HAVING SUM(o.total_amt_usd) =
    (SELECT MAX(region_total)
    FROM
          (SELECT s.region_id,
                 SUM(total_amt_usd) region_total
          FROM sales_reps s
          JOIN accounts a
          ON s.id = a.sales_rep_id
          JOIN orders o
          ON a.id = o.account_id
          GROUP BY 1) sub1)

