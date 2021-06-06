/*
Use the TRANSFORM function and the table finances to calculate interest for all cards issued to each user.
*/

SELECT firstName,
       lastName,
       TRANSFORM(expenses, card -> ROUND((6.25/100) * card["charges"], 2)) interest
FROM finances;

/*
Write a SQL query that achieves the following:
Displays cardholder's firstName, lastName, and a new column named lateFee
Uses the EXISTS function to flag customers who have made been charged a late payment fee.
Store the results in a temporary view named q2Results
*/

CREATE OR REPLACE TEMPORARY VIEW q2Results AS 

SELECT firstName,
       lastName,
       EXISTS(expenses, charge -> to_date(charge.paymentDue) > to_date(charge.lastPayment)) lateFee
FROM finances;

SELECT * FROM q2Results;