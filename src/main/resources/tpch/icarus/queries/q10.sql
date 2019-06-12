-- (Q10) Returned Item Reporting Query
-- tables: tpch_customer, tpch_orders, tpch_lineitem, tpch_nation
SELECT
  c_custkey,
  c_name,
  sum(l_extendedprice * (1 - l_discount)) AS revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM
  tpch_customer,
  tpch_orders,
  tpch_lineitem,
  tpch_nation
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= '1993-10-01'
  --TODO Interval
  AND o_orderdate < '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
ORDER BY
  revenue DESC
LIMIT 20

/**
Data Store	Execution Time	Total Time
monetdbssd	69.46 ms	70.83 ms
postgressqlhdd	2077.96 ms	2080.3 ms
voltdb	264021.89 ms	264025.95 ms
myisamssd	2175190.55 ms	2175193.82 ms (36 min!)
 */