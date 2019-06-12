-- (Q4) Order Priority Checking Query
-- tables: tpch_orders, tpch_lineitem
/*SELECT
  o_orderpriority,
  count(*) AS order_count
FROM
  tpch_orders
WHERE
  o_orderdate >= '1993-07-01'
  -- AND o_orderdate < '1993-07-01' + INTERVAL '3' MONTH
  AND o_orderdate < '1993-10-01'
  AND EXISTS(
      SELECT *
      FROM
        tpch_lineitem
      WHERE
        l_orderkey = o_orderkey
        AND l_commitdate < l_receiptdate
  )
GROUP BY
  o_orderpriority
ORDER BY
  o_orderpriority*/
select count(*) from tpch_customer
-- disabled because of missing EXISTS($query) support