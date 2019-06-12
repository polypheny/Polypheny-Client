-- (Q6) Forecasting Revenue Change Query
-- tables: tpch_lineitem
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM
  tpch_lineitem
WHERE
  l_shipdate >= '1994-01-01'
  AND l_shipdate < '1995-01-01'
  AND l_discount >= 0.05 AND l_discount <= 0.06
  AND l_quantity < 24
/**

Data Store	Execution Time	Total Time
monetdbssd	34.19 ms	35.28 ms
voltdb	701.71 ms	704.32 ms
postgressqlhdd	1365.58 ms	1367.2 ms
myisamssd	35799.02 ms	35801.15 ms
 */