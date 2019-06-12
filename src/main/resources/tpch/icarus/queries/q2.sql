-- (Q2) Minimum Cost Supplier Query
-- tables: tpch_part, tpch_supplier, tpch_partsupp, tpch_nation, tpch_region
SELECT
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
FROM
  tpch_part,
  tpch_supplier,
  tpch_partsupp,
  tpch_nation,
  tpch_region
WHERE
  p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
    SELECT
      min(ps_supplycost)
    FROM
      tpch_partsupp,
      tpch_supplier,
      tpch_nation,
      tpch_region
    WHERE
      p_partkey = ps_partkey
      AND s_suppkey = ps_suppkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'EUROPE'
    )
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT 100

/**

Data Store	Execution Time	Total Time
monetdbssd	31.39 ms	33.54 ms
voltdb	457.78 ms	463.96 ms
postgressqlhdd	580.54 ms	584.08 ms
myisamssd	65523.85 ms	65528.71 ms
 */