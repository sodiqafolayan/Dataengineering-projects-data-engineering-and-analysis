/*
README.md
This repo will contain my sql queries against the data linked below. This is from ...
Udacity course on sql-for-data-analysis
https://video.udacity-data.com/topher/2020/May/5eb5533b_parch-and-posey/parch-and-posey.sql
*/


/*
In the accounts table, there is a column holding the website for each company. The last three 
digits specify what type of web address they are using. A list of extensions (and pricing) is 
provided here. Pull these extensions and provide how many of each website type exist in the 
accounts table.
*/
SELECT RIGHT(website, 3) AS extension,
	   COUNT(*) extensionCount
FROM accounts
GROUP BY 1
ORDER BY 2 DESC;


/*
There is much debate about how much the name (or even the first letter of a company name) matters. 
Use the accounts table to pull the first letter of each company name to see the distribution of 
company names that begin with each letter (or number).
*/
SELECT LEFT(UPPER(name), 1) AS firstLetter,
	   COUNT(*) firstLetterCount
FROM accounts
GROUP BY 1
ORDER BY 2 DESC;

/*
Use the accounts table and a CASE statement to create two groups: one group of company names that 
start with a number and a second group of those company names that start with a letter. What 
proportion of company names start with a letter?
*/
/* Solution 1 */
SELECT CASE
	   WHEN RIGHT(name, 1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') 
       THEN 'number'
       ELSE 'alphabet' END AS proportion,
       COUNT(*)
FROM accounts
GROUP BY 1

/* Solution 2 */
SELECT SUM(numbers) nums, SUM(alphabets) alpha
FROM
(SELECT CASE WHEN RIGHT(name, 1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') THEN 1 ELSE 0 END AS numbers,
       CASE WHEN RIGHT(name, 1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') THEN 0 ELSE 1 END AS alphabets
FROM accounts) t1

/*
Consider vowels as a, e, i, o, and u. What proportion of company names start with a vowel, and what 
percent start with anything else?
*/
SELECT SUM(aeiouCompany) aeiou, SUM(otherCompany) other
FROM
(SELECT CASE WHEN RIGHT(UPPER(name), 1) IN ('A', 'E', 'I', 'O', 'U') THEN 1 ELSE 0 END AS aeiouCompany,
        CASE WHEN RIGHT(UPPER(name), 1) IN ('A', 'E', 'I', 'O', 'U') THEN 0 ELSE 1 END AS otherCompany
FROM accounts) t1


/*
Use the accounts table to create first and last name columns that hold the first and last names 
for the primary_poc
*/
SELECT primary_poc,
       LEFT(primary_poc, (STRPOS(primary_poc, ' ') - 1)) firstName,
       RIGHT(primary_poc, LENGTH(primary_poc) - STRPOS(primary_poc, ' ')) lastName
FROM accounts


/*
Now see if you can do the same thing for every rep name in the sales_reps table. Again provide 
first and last name columns.
*/
SELECT name,
       LEFT(name, (STRPOS(name, ' ') - 1)) firstName,
       RIGHT(name, LENGTH(name) - STRPOS(name, ' ')) lastName
FROM sales_reps

/*
Each company in the accounts table wants to create an email address for each primary_poc. 
The email address should be the first name of the primary_poc . last name primary_poc @ 
company name .com.
*/
WITH t1 AS 
    (SELECT primary_poc,
        name accountName,
        LEFT(primary_poc, (STRPOS(primary_poc, ' ') - 1)) firstName,
        RIGHT(primary_poc, LENGTH(primary_poc) - STRPOS(primary_poc, ' ')) lastName
    FROM accounts)

SELECT firstName,
       lastName,
       accountName,
       firstName || '.' || lastName || '@' || accountName || '.com' emailAddress
FROM t1


/*
You may have noticed that in the previous solution some of the company names include spaces, 
which will certainly not work in an email address. See if you can create an email address 
that will work by removing all of the spaces in the account name, but otherwise your solution 
should be just as in question 1.
*/
WITH t1 AS 
    (SELECT primary_poc,
        replace(name, ' ', '') accountName,
        LEFT(primary_poc, (STRPOS(primary_poc, ' ') - 1)) firstName,
        RIGHT(primary_poc, LENGTH(primary_poc) - STRPOS(primary_poc, ' ')) lastName
    FROM accounts)

SELECT firstName,
       lastName,
       accountName,
       firstName || '.' || lastName || '@' || accountName || '.com' emailAddress
FROM t1


/*
We would also like to create an initial password, which they will change after their first log in. 
The first password will be the first letter of the primary_poc's first name (lowercase), then the 
last letter of their first name (lowercase), the first letter of their last name (lowercase), the 
last letter of their last name (lowercase), the number of letters in their first name, the number 
of letters in their last name, and then the name of the company they are working with, all 
capitalized with no spaces.

*/
WITH t1 AS 
    (SELECT LEFT(LOWER(primary_poc), 1) firtNameFirstLetter,
        LEFT(LOWER(primary_poc), (STRPOS(primary_poc, ' ') - 1)) firstName,
        RIGHT(LEFT(LOWER(primary_poc), (STRPOS(primary_poc, ' ') - 1)), 1) firstNamelastLetter,
        RIGHT(primary_poc, LENGTH(primary_poc) - STRPOS(primary_poc, ' ')) lastName,
        RIGHT(RIGHT(primary_poc, LENGTH(primary_poc) - STRPOS(primary_poc, ' ')), 1) lastNameLastLetter,
        LEFT(LOWER(RIGHT(primary_poc, LENGTH(primary_poc) - STRPOS(primary_poc, ' '))), 1) lastNameFirtsLetter,
        LENGTH(LEFT(LOWER(primary_poc), (STRPOS(primary_poc, ' ') - 1))) firstNameLenght,
        LENGTH(RIGHT(primary_poc, LENGTH(primary_poc) - STRPOS(primary_poc, ' '))) lastNameLenght,
        replace(name, ' ', '') accountName        
    FROM accounts)

SELECT LOWER(firtNameFirstLetter || firstNamelastLetter || lastNameFirtsLetter || 
            lastNameLastLetter || firstNameLenght || lastNameLenght) || UPPER(accountName) 
            AS password
FROM t1
/*
Write a query to change the date into correct SQL format. You will need to use SUBSTR and CONCAT for this
operation

initialdate        
01/31/2014 08:00:00 AM +0000
01/31/2014 08:00:00 AM +0000
01/31/2014 08:00:00 AM +0000
01/31/2014 08:00:00 AM +0000
*/

SELECT date initialDate,
	  SUBSTR(date, 7, 4) || '-' || LEFT(date, 2) || '-' || SUBSTR(date, 4, 2) AS formatedDate
FROM sf_crime_data;

/*
Once you change to date format, use CAST or :: to change to SQL date format
*/
SELECT date initialData,
	  SUBSTR(date, 7, 4) || '-' || LEFT(date, 2) || '-' || SUBSTR(date, 4, 2) AS formatedDate,
(SUBSTR(date, 7, 4) || '-' || LEFT(date, 2) || '-' || SUBSTR(date, 4, 2))::date AS castedDate
FROM sf_crime_data;

/*
INSERT INTO my_table (
col_1, col_2
)
VALUES (
'value1', 'value2'
)


UPDATE my_table
SET col_name = 'Adeola Adedokun'
WHERE age = 33
*/