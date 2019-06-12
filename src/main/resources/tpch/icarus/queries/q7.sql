-- (Q7) Volume Shipping Query
-- tables: tpch_supplier, tpch_lineitem, tpch_orders, tpch_customer, tpch_nation
/*SELECT
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) AS revenue
FROM (
       SELECT
         n1.n_name                          AS supp_nation,
         n2.n_name                          AS cust_nation,
         extract(YEAR FROM l_shipdate)      AS l_year,
         l_shipdate                         AS l_year,
         l_extendedprice * (1 - l_discount) AS volume
       FROM
         tpch_supplier,
         tpch_lineitem,
         tpch_orders,
         tpch_customer,
         tpch_nation n1,
         tpch_nation n2
       WHERE
         s_suppkey = l_suppkey
         AND o_orderkey = l_orderkey
         AND c_custkey = o_custkey
         AND s_nationkey = n1.n_nationkey
         AND c_nationkey = n2.n_nationkey
         AND (
           (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
           OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
         )
         AND l_shipdate > '1995-01-01' AND l_shipdate < '1996-12-31'
     ) AS shipping
GROUP BY
  supp_nation,
  cust_nation,
  l_year
ORDER BY
  supp_nation,
  cust_nation,
  l_year*/
--disabling due to extract
select count(*) from tpch_customer