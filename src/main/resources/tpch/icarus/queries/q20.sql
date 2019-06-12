-- -- (Q20) Potential Part Promotion Query
-- -- tables: tpch_supplier, tpch_nation, tpch_partsupp, tpch_lineitem, tpch_part
SELECT count(*) FROM tpch_customer
--TODO Query disabled because of missing IN($query) support
/**
 SELECT
   s_name,
   s_address
 FROM
   tpch_supplier,
   tpch_nation
 WHERE
   s_suppkey IN (
     SELECT
       ps_suppkey
     FROM
       tpch_partsupp
     WHERE
       ps_partkey IN (
         SELECT
           p_partkey
         FROM
           tpch_part
         WHERE
           p_name LIKE 'forest%'
       )
       AND ps_availqty > (
         SELECT
           0.5 * sum(l_quantity)
         FROM
           tpch_lineitem
         WHERE
           l_partkey = ps_partkey
           AND l_suppkey = ps_suppkey
           AND l_shipdate >= DATE '1994-01-01'
           AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
       )
   )
   AND s_nationkey = n_nationkey
   AND n_name = 'CANADA'
 ORDER BY
  s_name
*/