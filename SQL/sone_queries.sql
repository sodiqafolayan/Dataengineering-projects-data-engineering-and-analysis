/*
Provide the name of the sales_rep in each region with the largest amount of total_amt_usd sales.
*/
SELECT rep_name
FROM
    (SELECT rep_name,
           MAX(sum_total) region_highest
    FROM
        (SELECT rep_name,
               region_ids,
               SUM(total_usd) sum_total
        FROM
            (SELECT s.id rep_id,
                   s.region_id region_ids,
                   s.name rep_name,
                   a.id account_ids,
                   SUM(o.total_amt_usd) total_usd
            FROM sales_reps s
            JOIN accounts a
            ON s.id = a.sales_rep_id
            JOIN orders o
            ON o.account_id = a.id
            GROUP BY 1, 2, 3, 4
            ORDER BY region_ids) sub
        GROUP BY 1, 2) sub2
    GROUP BY 1) sub3