-- (Q12) Shipping Modes and Order Priority Query
-- tables: tpch_orders, tpch_lineitem
SELECT
  l_shipmode,
  sum(CASE
      WHEN o_orderpriority = '1-URGENT'
           OR o_orderpriority = '2-HIGH'
        THEN 1
      ELSE 0
      END) AS high_line_count,
  sum(CASE
      WHEN o_orderpriority <> '1-URGENT'
           AND o_orderpriority <> '2-HIGH'
        THEN 1
      ELSE 0
      END) AS low_line_count
FROM
  tpch_orders,
  tpch_lineitem
WHERE
  o_orderkey = l_orderkey
  AND (l_shipmode = 'MAIL' OR l_shipmode = 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= '1994-01-01'
  AND l_receiptdate < '1995-01-01'
GROUP BY
  l_shipmode
ORDER BY
  l_shipmode

/**

Data Store	Execution Time	Total Time
monetdbssd	177.66 ms	179.35 ms
postgressqlhdd	1764.45 ms	1768.08 ms
voltdb	193987.05 ms	193995.33 ms
myisamssd	267348.93 ms	267354.69 ms
 */