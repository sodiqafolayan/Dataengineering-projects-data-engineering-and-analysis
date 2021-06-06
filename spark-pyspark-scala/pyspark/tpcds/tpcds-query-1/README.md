Query 1
Find customers who have returned items more than 20% more often than the 
average customer returns for a store in a given state for a 
given year.
Qualification Substitution Parameters:
YEAR.01=2000
STATE.01=TN
AGG_FIELD.01 = SR_RETURN_AMT

My Solution Approach
1. Identify all the tables that need to be read in
    storeReturns, store, customer and dateDimension
   
2. Pre-Filtering based on business use case
   store: Read only data where state = TN. 
          Columns reduced from 29 to 12
          Rows reduced from 1,704 rows to 49
          Reading store dataframe first gave me access to s_store_sk columns with only 
          stores from TN. This was very useful in ensuring that data read in in the
          store returns dataframe will only include stores in TN
   
   storeReturns: I converted store_sk column from store dataframe into a python list and 
          use it to filter storeReturns when reading it in to ensure that only stores located 
          in TN are read in. 
          Columns reduced from 20 to 16
          Rows reduced from 8,639,952,111 to 67,138,328
   
  customer: Read and select only required columns. I took just 25% sample
          Rows reduced from 80,000,000 to 19,996,290
   
   dateDimension: Read only data where d_year == 2000.
          Rows reduced from 73,049 to 366
   
 
   
   

