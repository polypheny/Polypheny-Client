-- (Q8) National Market Share Query
-- tables: tpch_part, tpch_supplier, tpch_lineitem, tpch_orders, tpch_customer, tpch_nation
/*SELECT
  o_year,
  sum(CASE
      WHEN nation = 'BRAZIL'
        THEN volume
      ELSE 0
      END) / sum(volume) AS mkt_share
FROM (
       SELECT
         extract(YEAR FROM o_orderdate)      AS o_year,
         o_orderdate                        AS o_year,
         l_extendedprice * (1 - l_discount) AS volume,
         n2.n_name                          AS nation
       FROM
         tpch_part,
         tpch_supplier,
         tpch_lineitem,
         tpch_orders,
         tpch_customer,
         tpch_nation n1,
         tpch_nation n2,
         tpch_region
       WHERE
         p_partkey = l_partkey
         AND s_suppkey = l_suppkey
         AND l_orderkey = o_orderkey
         AND o_custkey = c_custkey
         AND c_nationkey = n1.n_nationkey
         AND n1.n_regionkey = r_regionkey
         AND r_name = 'AMERICA'
         AND s_nationkey = n2.n_nationkey
         AND o_orderdate > '1995-01-01' AND o_orderdate < '1996-12-31'
         AND p_type = 'ECONOMY ANODIZED STEEL'
     ) AS all_nations
GROUP BY
  o_year
ORDER BY
  o_year*/
--disabling due to extract
SELECT count(*) FROM tpch_customer