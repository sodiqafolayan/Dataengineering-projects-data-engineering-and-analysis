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

/*
How many accounts had more total purchases than the account name which has bought the
most standard_qty paper throughout their lifetime as a customer?
*/
SELECT COUNT(*) FROM
    (SELECT a.name,
            SUM(o.total) total_purchase
     FROM accounts a
     JOIN orders o
     ON a.id = o.account_id
     GROUP BY 1
     HAVING SUM(o.total) >
         (SELECT total
         FROM
               (SELECT a.name,
                    SUM(o.standard_qty) total_standard,
                    SUM(o.total) total
             FROM accounts a
             JOIN orders o
             ON a.id = o.account_id
             GROUP BY 1
             ORDER BY 2 DESC
             LIMIT 1) sub1)) sub2

/*
For the customer that spent the most (in total over their lifetime as a customer) total_amt_usd,
how many web_events did they have for each channel?
*/
SELECT channel,
COUNT(*)
FROM web_events
WHERE account_id =
    (SELECT acc_id FROM
        (SELECT a.id acc_id,
               a.name acc_name,
               SUM(o.total_amt_usd)
        FROM accounts a
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 1) sub1)
GROUP BY channel


/*
What is the lifetime average amount spent in terms of total_amt_usd for the top 10 total spending accounts?
*/
SELECT ROUND(AVG(total_sum), 2) top_ten_avg FROM
    (SELECT a.id acc_id,
           a.name acc_name,
           SUM(o.total_amt_usd) total_sum
    FROM accounts a
    JOIN orders o
    ON a.id = o.account_id
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 10) sub1

/*
What is the lifetime average amount spent in terms of total_amt_usd, including only the
companies that spent more per order, on average, than the average of all orders.
*/
SELECT ROUND(AVG(avg_spent), 2)
FROM
    (SELECT account_id,
           AVG(total_amt_usd) avg_spent
    FROM orders
    GROUP BY 1
    HAVING AVG(total_amt_usd) >
        (SELECT ROUND(AVG(total_amt_usd), 2) all_orders_avg
        FROM orders)
    ) sub2


/*
Provide the name of the sales_rep in each region with the largest amount of total_amt_usd sales.
*/
WITH t1 AS
        (SELECT s.region_id,
               a.sales_rep_id,
               SUM(o.total_amt_usd) rep_total
        FROM sales_reps s
        JOIN accounts a
        ON s.id = a.sales_rep_id
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1, 2),

    t2 AS
        (SELECT region_id,
               MAX(rep_total) max_sum_per_region
        FROM t1
        GROUP BY region_id),

    t3 AS
        (SELECT sales_rep_id
        FROM t1
        JOIN t2
        ON rep_total = max_sum_per_region)


SELECT name
FROM sales_reps s
JOIN t3
ON s.id = t3.sales_rep_id



/*
How many accounts had more total purchases than the account name which has bought the most standard_qty
paper throughout their lifetime as a customer?
*/
WITH t1 AS
        (SELECT a.name,
               SUM(o.standard_amt_usd) standard_total_usd
        FROM sales_reps s
        JOIN accounts a
        ON s.id = a.sales_rep_id
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1),
    t2 AS
        (SELECT a.name highest_standard_qty_acc,
               SUM(o.total) total_purchase
        FROM accounts a
        JOIN orders o
        ON a.id = o.account_id
        WHERE a.name = (SELECT name FROM t1)
        GROUP BY 1)

SELECT account_id,
	   SUM(total)
FROM orders
GROUP BY 1
HAVING SUM(total) > (SELECT total_purchase FROM t2)


/*
For the customer that spent the most (in total over their lifetime as a customer) total_amt_usd,
how many web_events did they have for each channel?
*/
WITH t1 AS
        (SELECT o.account_id acc_id,
               SUM(total_amt_usd) highest_spent
        FROM web_events w
        JOIN accounts a
        ON w.account_id = a.id
        JOIN orders o
        ON a.id = o.account_id
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1)

SELECT account_id,
	   channel,
       COUNT(channel)
FROM web_events
WHERE account_id = (SELECT acc_id FROM t1)
GROUP BY 1, 2
ORDER BY 3 DESC



/*
What is the lifetime average amount spent in terms of total_amt_usd for the top 10 total spending accounts?
*/
WITH t1 AS
     (SELECT account_id acc_id,
           SUM(total_amt_usd) total_spent
    FROM orders
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10)

SELECT ROUND(AVG(total_spent), 2) top_10_avg
FROM t1


/*
Use the accounts table to create first and last name columns that hold the first and last names for the primary_poc.
*/
SELECT LEFT(primary_poc, POSITION(' ' IN primary_poc) - 1) AS first,
RIGHT(primary_poc, LENGTH(primary_poc) - POSITION(' ' IN primary_poc)) AS last
FROM accounts



/*
Each company in the accounts table wants to create an email address for each primary_poc. The email address
should be the first name of the primary_poc . last name primary_poc @ company name .com.
*/

WITH t1 AS
(SELECT LOWER(LEFT(primary_poc, POSITION(' ' IN primary_poc) - 1)) AS first,
LOWER(RIGHT(primary_poc, LENGTH(primary_poc) - POSITION(' ' IN primary_poc))) AS last,
LOWER(RIGHT(website, 3)) AS domain
FROM accounts)

SELECT CONCAT(first, '.', last, '@', domain) email_add
FROM t1


/*
You may have noticed that in the previous solution some of the company names include spaces, which will certainly
not work in an email address. See if you can create an email address that will work by removing all of the spaces
in the account name,
*/

WITH t1 AS
(SELECT LOWER(LEFT(primary_poc, POSITION(' ' IN primary_poc) - 1)) AS first,
LOWER(RIGHT(primary_poc, LENGTH(primary_poc) - POSITION(' ' IN primary_poc))) AS last,
LOWER(REPLACE(name, ' ', '')) company_name,
LOWER(RIGHT(website, 3)) AS domain
FROM accounts)

SELECT CONCAT(first, '.', last, '@', company_name, '.', domain) email_add
FROM t1




/*
We would also like to create an initial password, which they will change after their first log in. The
first password will be the first letter of the primary_poc's first name (lowercase), then the last letter
of their first name (lowercase), the first letter of their last name (lowercase), the last letter of their
last name (lowercase), the number of letters in their first name, the number of letters in their last name,
and then the name of the company they are working with, all capitalized with no spaces.
*/


WITH t1 AS
    (SELECT LOWER(LEFT(primary_poc, POSITION(' ' IN primary_poc) - 1)) AS first,
    LOWER(RIGHT(primary_poc, LENGTH(primary_poc) - POSITION(' ' IN primary_poc))) AS last,
    LOWER(REPLACE(name, ' ', '')) company_name,
    LOWER(RIGHT(website, 3)) AS domain
    FROM accounts),

    t2 AS
    (SELECT LEFT(first, 1) AS first_name_first_letter,
           RIGHT(first, 1) AS first_name_last_letter,
           LEFT(last, 1) AS last_name_first_letter,
           RIGHT(last, 1) AS last_name_last_letter,
           LENGTH(first) AS first_name_lenght,
           LENGTH(last) AS last_name_lenght,
           company_name
     FROM t1)

SELECT UPPER(CONCAT(first_name_first_letter,
                    first_name_last_letter,
                    last_name_first_letter,
                    last_name_last_letter,
                    first_name_lenght,
                    last_name_lenght,
                    company_name)) AS password
FROM t2


SELECT LEFT(DATE_TRUNC('year', occurred_at), 4) AS annual_sales,
	   SUM(total_amt_usd) total_usd
FROM orders
GROUP BY 1
ORDER BY 2 DESC