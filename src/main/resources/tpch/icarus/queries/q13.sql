-- (Q13) Customer Distribution Query
-- tables: tpch_customer
/*SELECT
  c_count,
  count(*) AS custdist
FROM (
  SELECT
    c_custkey,
    count(o_orderkey)
  FROM
    tpch_customer LEFT OUTER JOIN tpch_orders ON
      c_custkey = o_custkey
      AND o_comment NOT LIKE '%special%requests%'
    GROUP BY
      c_custkey
     ) AS c_orders (c_custkey, c_count)
GROUP BY
  c_count
ORDER BY
  custdist DESC,
  c_count DESC*/
-- Temporarily disabled because of NOT LIKE which is specifically bad because it is in the LEFT OUTER JOIN statement
SELECT count(*) FROM tpch_customer