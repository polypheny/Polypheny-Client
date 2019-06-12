-- (Q11) Important Stock Identification Query
-- tables: tpch_partsupp, tpch_supplier, tpch_nation
SELECT
  ps_partkey,
  sum(ps_supplycost * ps_availqty) AS value
FROM
  tpch_partsupp,
  tpch_supplier,
  tpch_nation
WHERE
  ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY
  ps_partkey
HAVING
  sum(ps_supplycost * ps_availqty) > (
    SELECT sum(ps_supplycost * ps_availqty) * 0.0001
    FROM
      tpch_partsupp,
      tpch_supplier,
      tpch_nation
    WHERE
      ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY'
  )
ORDER BY
  value DESC

/**

Data Store	Execution Time	Total Time
voltdb	22.82 ms	44.01 ms
monetdbssd	40.89 ms	46.59 ms
postgressqlhdd	435.46 ms	445.86 ms
myisamssd	11600.82 ms	11617.96 ms
 */