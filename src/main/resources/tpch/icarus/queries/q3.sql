-- (Q3) Shipping Priority Query
-- tables: tpch_customer, tpch_orders, tpch_lineitem
SELECT
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)) AS revenue,
  o_orderdate,
  o_shippriority
FROM
  tpch_customer,
  tpch_orders,
  tpch_lineitem
WHERE
  c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY
  l_orderkey,
  o_orderdate,
  o_shippriority
ORDER BY
  revenue DESC,
  o_orderdate
LIMIT 10
/**
monetdbssd	64.65 ms	66.06 ms
postgressqlhdd	1182.41 ms	1184.68 ms
myisamssd	288429.14 ms	288432.14 ms
voltdb	490461.54 ms	490466.04 ms
*/