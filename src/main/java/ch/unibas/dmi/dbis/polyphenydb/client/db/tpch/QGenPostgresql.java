package ch.unibas.dmi.dbis.polyphenydb.client.db.tpch;


import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang.ArrayUtils;


/**
 * Generates queries according to the specification for PostgreSQL
 *
 * @author manuel on 31.07.17.
 */
public class QGenPostgresql {

    public static String createQuery( double SCALE_FACTOR, int queryID ) {
        String[] queries = getQueries( SCALE_FACTOR );
        return queries[queryID - 1];
    }


    private static String[] getQueries( double SCALE_FACTOR ) {
        String[] queries = new String[22];
        queries[0] = q1();
        queries[1] = q2();
        queries[2] = q3();
        queries[3] = q4();
        queries[4] = q5();
        queries[5] = q6();
        queries[6] = q7();
        queries[7] = q8();
        queries[8] = q9();
        queries[9] = q10();
        queries[10] = q11( SCALE_FACTOR );
        queries[11] = q12();
        queries[12] = q13();
        queries[13] = q14();
        queries[14] = q15();
        queries[15] = q16();
        queries[16] = q17();
        queries[17] = q18();
        queries[18] = q19();
        queries[19] = q20();
        queries[20] = q21();
        queries[21] = q22();
        return queries;
    }


    private static String q1() {
        int delta = getRandomInt( 60, 120 );
        String query =
                "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM tpch_lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '"
                        + delta + "' DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;";
        return query;
    }


    private static String q2() {
        int size = getRandomInt( 1, 50 );
        String type = getRandomTypeSyllable3();
        String region = getRandomRegion();
        String query = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM tpch_part, tpch_supplier, tpch_partsupp, tpch_nation, tpch_region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = " + size + " AND p_type LIKE '%" + type
                + "' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '" + region
                + "' AND ps_supplycost = (SELECT min(ps_supplycost) FROM tpch_partsupp, tpch_supplier, tpch_nation, tpch_region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '" + region
                + "') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;";
        return query;
    }


    private static String q3() {
        String segment = getRandomSegment();
        int day = getRandomInt( 1, 31 );
        LocalDate date = LocalDate.of( 1995, Month.MARCH, day );
        String query = "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM tpch_customer, tpch_orders, tpch_lineitem WHERE c_mktsegment = '" + segment + "' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '" + date
                + "' AND l_shipdate > DATE '" + date + "' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;";
        return query;
    }


    private static String q4() {
        int year = getRandomInt( 1993, 1997 );
        int month;
        if ( year == 1997 ) {
            month = getRandomInt( 1, 10 );
        } else {
            month = getRandomInt( 1, 12 );
        }
        LocalDate date = LocalDate.of( year, month, 1 );
        String query = "SELECT o_orderpriority, count(*) AS order_count FROM tpch_orders WHERE o_orderdate >= DATE '" + date + "' AND o_orderdate < DATE '" + date
                + "' + INTERVAL '3' MONTH AND EXISTS (SELECT * FROM tpch_lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q5() {
        String region = getRandomRegion();
        int year = getRandomInt( 1993, 1997 );
        LocalDate date = LocalDate.of( year, Month.JANUARY, 1 );
        String query =
                "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM tpch_customer, tpch_orders, tpch_lineitem, tpch_supplier, tpch_nation, tpch_region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"
                        + region + "' AND o_orderdate >= DATE '" + date + "' AND o_orderdate < DATE '" + date + "' + INTERVAL '1' YEAR GROUP BY n_name ORDER BY revenue DESC;";
        return query;
    }


    private static String q6() {
        int year = getRandomInt( 1993, 1997 );
        LocalDate date = LocalDate.of( year, Month.JANUARY, 1 );
        double initialDiscount = getRandomDouble( 0.02, 0.09 );
        double discount = Math.round( initialDiscount * 100.0 ) / 100.0;
        int quantity = getRandomInt( 24, 25 );
        String query = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM tpch_lineitem WHERE l_shipdate >= DATE '" + date + "' AND l_shipdate < DATE '" + date + "' + INTERVAL '1' YEAR AND l_discount BETWEEN decimal '" + discount + "' - decimal '0.01' AND decimal '" + discount
                + "' + decimal '0.01' AND l_quantity < " + quantity + ";";
        return query;
    }


    private static String q7() {
        String nation1 = getRandomNation();
        String nation2 = getRandomNation();
        while ( nation1.equals( nation2 ) ) {
            nation2 = getRandomNation();
        }
        String query =
                "SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(YEAR FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount)  AS volume FROM tpch_supplier, tpch_lineitem, tpch_orders, tpch_customer, tpch_nation n1, tpch_nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = '"
                        + nation1 + "' AND n2.n_name = '" + nation2 + "') OR (n1.n_name = '" + nation2 + "' AND n2.n_name = '" + nation1
                        + "')) AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q8() {
        String nation = getRandomNation();
        String region = getRegionForNation( nation );
        String type = getRandomType();
        String query = "SELECT o_year, sum(CASE WHEN nation = '" + nation
                + "' THEN volume ELSE 0 END) / sum(volume) AS mkt_share FROM (SELECT extract(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM tpch_part, tpch_supplier, tpch_lineitem, tpch_orders, tpch_customer, tpch_nation n1, tpch_nation n2, tpch_region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = '"
                + region + "' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND p_type = '" + type + "') AS all_nations GROUP BY o_year ORDER BY o_year;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q9() {
        String color = getRandomColor();
        String query =
                "SELECT nation, o_year, sum(amount) AS sum_profit FROM (SELECT n_name AS nation, extract(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM tpch_part, tpch_supplier, tpch_lineitem, tpch_partsupp, tpch_orders, tpch_nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%"
                        + color + "%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q10() {

        int year = getRandomInt( 1993, 1995 );
        int month;
        if ( year == 1993 ) {
            month = getRandomInt( 2, 12 );
        } else if ( year == 1995 ) {
            month = 1;
        } else {
            month = getRandomInt( 1, 12 );
        }
        LocalDate date = LocalDate.of( year, month, 1 );
        String query = "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM tpch_customer, tpch_orders, tpch_lineitem, tpch_nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '" + date
                + "' AND o_orderdate < DATE '" + date + "' + INTERVAL '3' MONTH AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20;";
        return query;
    }


    private static String q11( double SCALE_FACTOR ) {
        String nation = getRandomNation();
        double fraction = 0.0001 / SCALE_FACTOR;
        String query = "SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value FROM tpch_partsupp, tpch_supplier, tpch_nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = '" + nation
                + "' GROUP BY ps_partkey HAVING sum(ps_supplycost * ps_availqty) > (SELECT sum(ps_supplycost * ps_availqty) * " + fraction + " FROM tpch_partsupp, tpch_supplier, tpch_nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = '" + nation
                + "') ORDER BY value DESC;";
        //Disabled because of icarus
        return "select count(*) from tpch_customer";
    }


    private static String q12() {
        String shipmode1 = getRandomMode();
        String shipmode2 = getRandomMode();
        while ( shipmode1.equals( shipmode2 ) ) {
            shipmode2 = getRandomMode();
        }
        int year = getRandomInt( 1993, 1997 );
        LocalDate date = LocalDate.of( year, Month.JANUARY, 1 );
        String query =
                "SELECT l_shipmode, sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM tpch_orders, tpch_lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('"
                        + shipmode1 + "', '" + shipmode2 + "') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= DATE '" + date + "' AND l_receiptdate < DATE '" + date + "' + INTERVAL '1' YEAR GROUP BY l_shipmode ORDER BY l_shipmode;";
        return query;
    }


    private static String q13() {
        String word1 = getRandomArrayEntry( new String[]{ "special", "pending", "unusual", "express" } );
        String word2 = getRandomArrayEntry( new String[]{ "packages", "requests", "accounts", "deposits" } );
        String query = "SELECT c_count, count(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) FROM tpch_customer LEFT OUTER JOIN tpch_orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%" + word1 + "%" + word2
                + "%' GROUP BY c_custkey) AS c_orders (c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q14() {
        int year = getRandomInt( 1993, 1997 );
        int month = getRandomInt( 1, 12 );
        LocalDate date = LocalDate.of( year, month, 1 );
        String query =
                "SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount))  AS promo_revenue FROM tpch_lineitem, tpch_part WHERE l_partkey = p_partkey AND l_shipdate >= DATE '" + date + "' AND l_shipdate < DATE '"
                        + date + "' + INTERVAL '1' MONTH;";
        return query;
    }


    private static String q15() {
        int year = getRandomInt( 1993, 1997 );
        int month;
        if ( year == 1997 ) {
            month = getRandomInt( 1, 10 );
        } else {
            month = getRandomInt( 1, 12 );
        }
        LocalDate date = LocalDate.of( year, month, 1 );
        // execute view
        String view = "CREATE OR REPLACE VIEW revenue AS SELECT l_suppkey sum(l_extendedprice * (1 - l_discount)) AS total_revenue FROM tpch_lineitem WHERE l_shipdate >= DATE '" + date + "' AND l_shipdate < DATE '" + date + "' + INTERVAL '3' MONTH GROUP BY l_suppkey;";
        String query = "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM tpch_supplier, revenue WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM revenue) ORDER BY s_suppkey;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q16() {
        int m = getRandomInt( 1, 5 );
        int n = getRandomInt( 1, 5 );
        String brand = "Brand#" + String.valueOf( m ) + String.valueOf( n );
        String types = getRandomType();
        String[] typesArray = types.split( "\\s+" );
        String type = typesArray[0] + " " + typesArray[1];
        int size1 = getRandomInt( 1, 50 );
        int size2 = getRandomInt( 1, 50 );
        int size3 = getRandomInt( 1, 50 );
        int size4 = getRandomInt( 1, 50 );
        int size5 = getRandomInt( 1, 50 );
        int size6 = getRandomInt( 1, 50 );
        int size7 = getRandomInt( 1, 50 );
        int size8 = getRandomInt( 1, 50 );
        String query =
                "SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey)  AS supplier_cnt FROM tpch_partsupp, tpch_part WHERE p_partkey = ps_partkey AND p_brand <> '" + brand + "' AND p_type NOT LIKE '" + type + "%' AND p_size IN (" + size1 + "," + size2 + "," + size3 + "," + size4 + "," + size5
                        + "," + size6 + "," + size7 + "," + size8 + ") AND ps_suppkey NOT IN (SELECT s_suppkey FROM tpch_supplier WHERE s_comment LIKE '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q17() {
        int m = getRandomInt( 1, 5 );
        int n = getRandomInt( 1, 5 );
        String brand = "Brand#" + String.valueOf( m ) + String.valueOf( n );
        String container = getRandomContainer();
        String query = "SELECT sum(l_extendedprice) / 7.0  AS avg_yearly FROM tpch_lineitem, tpch_part WHERE p_partkey = l_partkey AND p_brand = '" + brand + "' AND p_container = '" + container + "' AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM tpch_lineitem WHERE l_partkey = p_partkey);";
        // disabling
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q18() {
        int quantity = getRandomInt( 312, 315 );
        String query = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) FROM tpch_customer, tpch_orders, tpch_lineitem WHERE o_orderkey IN (SELECT l_orderkey FROM tpch_lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > " + quantity
                + ") AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q19() {
        int quantity1 = getRandomInt( 1, 10 );
        int quantity2 = getRandomInt( 10, 20 );
        int quantity3 = getRandomInt( 20, 30 );
        int m1 = getRandomInt( 1, 5 );
        int n1 = getRandomInt( 1, 5 );
        String brand1 = "Brand#" + String.valueOf( m1 ) + String.valueOf( n1 );
        int m2 = getRandomInt( 1, 5 );
        int n2 = getRandomInt( 1, 5 );
        String brand2 = "Brand#" + String.valueOf( m2 ) + String.valueOf( n2 );
        int m3 = getRandomInt( 1, 5 );
        int n3 = getRandomInt( 1, 5 );
        String brand3 = "Brand#" + String.valueOf( m3 ) + String.valueOf( n3 );
        String query =
                "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM tpch_lineitem, tpch_part WHERE (p_partkey = l_partkey AND p_brand = '" + brand1 + "' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= " + quantity1 + " AND l_quantity <= " + quantity1
                        + " + 10 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = '" + brand2 + "' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= " + quantity2
                        + " AND l_quantity <= " + quantity2 + " + 10 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = '" + brand3
                        + "' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= " + quantity3 + " AND l_quantity <= " + quantity3 + " + 10 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON');";
        return query;
    }


    private static String q20() {
        String color = getRandomColor();
        int year = getRandomInt( 1993, 1997 );
        LocalDate date = LocalDate.of( year, Month.JANUARY, 1 );
        String nation = getRandomNation();
        String query = "SELECT s_name, s_address FROM tpch_supplier, tpch_nation WHERE s_suppkey IN (SELECT ps_suppkey FROM tpch_partsupp WHERE ps_partkey IN (SELECT p_partkey FROM tpch_part WHERE p_name LIKE '" + color
                + "%') AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM tpch_lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= DATE '" + date + "' AND l_shipdate < DATE '" + date + "' + INTERVAL '1' YEAR ) ) AND s_nationkey = n_nationkey AND n_name = '" + nation
                + "' ORDER BY s_name;";
        // disabling
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q21() {
        String nation = getRandomNation();
        String query =
                "SELECT s_name, count(*)  AS numwait FROM tpch_supplier, tpch_lineitem l1, tpch_orders, tpch_nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND exists(SELECT * FROM tpch_lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT exists(SELECT * FROM tpch_lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = '"
                        + nation + "' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    private static String q22() {
        int[] array = getNUniqueRandomNumbers( 7 );
        int i1 = array[0];
        int i2 = array[1];
        int i3 = array[2];
        int i4 = array[3];
        int i5 = array[4];
        int i6 = array[5];
        int i7 = array[6];
        String query = "SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal FROM (SELECT substr(c_phone, 1, 2) AS cntrycode, c_acctbal FROM tpch_customer WHERE substr(c_phone, 1, 2) IN ('" + i1 + "', '" + i2 + "', '" + i3 + "', '" + i4 + "', '" + i5 + "', '" + i6 + "', '" + i7
                + "') AND c_acctbal > (SELECT avg(c_acctbal) FROM tpch_customer WHERE c_acctbal > 0.00 AND substr(c_phone, 1, 2) IN ('" + i1 + "', '" + i2 + "', '" + i3 + "', '" + i4 + "', '" + i5 + "', '" + i6 + "', '" + i7
                + "')) AND NOT exists(SELECT * FROM tpch_orders WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode;";
        // disabling because of icarus
        query = "select count(*) from tpch_customer;";
        return query;
    }


    public static int getRandomInt( int min, int max ) {
        return ThreadLocalRandom.current().nextInt( min, max + 1 );
    }


    public static double getRandomDouble( double min, double max ) {
        return ThreadLocalRandom.current().nextDouble( min, max );
    }


    public static String getRandomArrayEntry( String[] array ) {
        int arrayIndex = new Random().nextInt( array.length );
        return array[arrayIndex];
    }


    public static String getRandomType() {
        String[] syllables1 = new String[]{ "STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO" };
        int syllable1Index = new Random().nextInt( syllables1.length );
        String syllable1 = syllables1[syllable1Index];
        String[] syllables2 = new String[]{ "ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED" };
        int syllable2Index = new Random().nextInt( syllables2.length );
        String syllable2 = syllables2[syllable2Index];
        String[] syllables3 = new String[]{ "TIN", "NICKEL", "BRASS", "STEEL", "COPPER" };
        int syllable3Index = new Random().nextInt( syllables3.length );
        String syllable3 = syllables3[syllable3Index];
        return syllable1 + " " + syllable2 + " " + syllable3;
    }


    public static String getRandomTypeSyllable3() {
        String[] types = new String[]{ "TIN", "NICKEL", "BRASS", "STEEL", "COPPER" };
        int typeIndex = new Random().nextInt( types.length );
        return types[typeIndex];
    }


    public static String getRandomSegment() {
        String[] segments = new String[]{ "AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD" };
        int segmentIndex = new Random().nextInt( segments.length );
        return segments[segmentIndex];
    }


    public static String getRandomNation() {
        String[] nations = new String[]{ "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM",
                "UNITED STATES" };
        int nationIndex = new Random().nextInt( nations.length );
        return nations[nationIndex];
    }


    public static String getRandomRegion() {
        String[] regions = new String[]{ "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST" };
        int regionIndex = new Random().nextInt( regions.length );
        return regions[regionIndex];
    }


    public static String getRegionForNation( String nation ) {
        String region;
        String[] regionKey0 = new String[]{ "ALGERIA", "ETHIOPIA", "KENYA", "MOROCCO", "MOZAMBIQUE" };
        String[] regionKey1 = new String[]{ "ARGENTINA", "BRAZIL", "CANADA", "PERU", "UNITED STATES" };
        String[] regionKey2 = new String[]{ "INDIA", "INDONESIA", "JAPAN", "CHINA", "VIETNAM" };
        String[] regionKey3 = new String[]{ "FRANCE", "GERMANY", "ROMANIA", "RUSSIA", "UNITED KINGDOM" };
        //String[] regionKey4 = new String[]{"EGYPT", "IRAN", "IRAQ", "JORDAN", "SAUDI ARABIA"};
        if ( ArrayUtils.contains( regionKey0, nation ) ) {
            region = "AFRICA";
        } else if ( ArrayUtils.contains( regionKey1, nation ) ) {
            region = "AMERICA";
        } else if ( ArrayUtils.contains( regionKey2, nation ) ) {
            region = "ASIA";
        } else if ( ArrayUtils.contains( regionKey3, nation ) ) {
            region = "EUROPE";
        } else {
            region = "MIDDLE EAST";
        }
        return region;
    }


    public static String getRandomColor() {
        String[] colors = new String[]{ "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger",
                "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint",
                "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel",
                "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow" };
        int colorIndex = new Random().nextInt( colors.length );
        return colors[colorIndex];
    }


    public static String getRandomMode() {
        String[] modes = new String[]{ "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB" };
        int modeIndex = new Random().nextInt( modes.length );
        return modes[modeIndex];
    }


    public static String getRandomContainer() {
        String[] syllables1 = new String[]{ "SM", "LG", "MED", "JUMBO", "WRAP" };
        int syllable1Index = new Random().nextInt( syllables1.length );
        String syllable1 = syllables1[syllable1Index];
        String[] syllables2 = new String[]{ "CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM" };
        int syllable2Index = new Random().nextInt( syllables2.length );
        String syllable2 = syllables2[syllable2Index];
        return syllable1 + " " + syllable2;
    }


    public static int[] getNUniqueRandomNumbers( int n ) {
        int[] array = new int[n];
        ArrayList<Integer> list = new ArrayList<>();
        for ( int i = 0 + 10; i <= 24 + 10; i++ ) { // 24 countries
            list.add( new Integer( i ) );
        }
        Collections.shuffle( list );
        for ( int i = 0; i < n; i++ ) {
            array[i] = list.get( i );
        }
        return array;
    }

}
