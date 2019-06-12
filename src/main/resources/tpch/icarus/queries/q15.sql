-- -- (Q15) Top Supplier Query
-- -- tables: tpch_lineitem, tpch_supplier
-- CREATE OR REPLACE VIEW revenue AS
--   SELECT
--     l_suppkey                               AS supplier_no,
--     sum(l_extendedprice * (1 - l_discount)) AS total_revenue
--   FROM
--     tpch_lineitem
--   WHERE
--     l_shipdate >= DATE '1996-01-01'
--     AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
-- GROUP BY
--   l_suppkey;
--Temporarily disabled because of view
SELECT count(*)FROM tpch_customer
/*
SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM
  tpch_supplier,
  revenue
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
    SELECT
      max(total_revenue)
    FROM
      revenue
  )
ORDER BY
  s_suppkey;*/
