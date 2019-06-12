-- (Q1) Pricing Summary Report Query
-- tables: tpch_line item
SELECT
  l_returnflag,
  l_linestatus,
  sum(l_quantity)                                       AS sum_qty,
  sum(l_extendedprice)                                  AS sum_base_price,
  sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
  avg(l_quantity)                                       AS avg_qty,
  avg(l_extendedprice)                                  AS avg_price,
  avg(l_discount)                                       AS avg_disc,
  count(*)                                              AS count_order
FROM
  tpch_lineitem
WHERE
  -- l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
  l_shipdate <= '1998-09-02'
GROUP BY
  l_returnflag,
  l_linestatus
ORDER BY
  l_returnflag,
  l_linestatus

/**
Data Store	Execution Time	Total Time
monetdbssd	137.33 ms	138.51 ms
voltdb	8820.42 ms	8823.9 ms
postgressqlhdd	11957.69 ms	11959.68 ms
myisamssd	509401.54 ms	509404.19 ms -- 10 min(!)
 */