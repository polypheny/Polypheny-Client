-- (Q22) Suppliers Who Kept Orders Waiting Query
-- tables: tpch_orders, tpch_customer
/*SELECT
  cntrycode,
  count(*)        AS numcust,
  sum(c_acctbal)  AS totacctbal
FROM (
  SELECT
    substr(c_phone, 1, 2) AS cntrycode,
    c_acctbal
  FROM
    tpch_customer
  WHERE
    substr(c_phone, 1, 2) IN
      ('13', '31', '23', '29', '30', '18', '17')
    AND c_acctbal > (
      SELECT
        avg(c_acctbal)
      FROM
        tpch_customer
      WHERE
        c_acctbal > 0.00
        AND substr(c_phone, 1, 2) IN
          ('13', '31', '23', '29', '30', '18', '17')
    )
    AND NOT exists(
      SELECT
        *
      FROM
        tpch_orders
      WHERE
        o_custkey = c_custkey
    )
  ) AS custsale
GROUP BY
  cntrycode
ORDER BY
  cntrycode*/
--Disabling because of missing EXISTS($query)
SELECT count(*) FROM tpch_customer