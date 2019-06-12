-- -- (Q17) Small-Quantity-Order Revenue Query
-- -- tables: tpch_lineitem, tpch_part
SELECT count(*) FROM tpch_customer;
--TODO Temporarily disabled since this query takes too long :)
/* SELECT
   sum(l_extendedprice) / 7.0  AS avg_yearly
 FROM
   tpch_lineitem,
   tpch_part
 WHERE
   p_partkey = l_partkey
   AND p_brand = 'Brand#23'
   AND p_container = 'MED BOX'
   AND l_quantity < (
     SELECT
       0.2 * avg(l_quantity)
     FROM
       tpch_lineitem
     WHERE
       l_partkey = p_partkey
   )*/

/**
myisamssd: finished with errors
monetdbssd: finished
voltdb: finished with errors
postgressqlhdd: aborted
 */