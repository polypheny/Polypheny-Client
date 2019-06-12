package ch.unibas.dmi.dbis.polyphenydb.client.db.musqle;


import java.text.DecimalFormat;


/**
 * Generates queries according to the specification for Icarus
 */
public class QGenIcarus {

    private static final DecimalFormat decimalFormat = new DecimalFormat( "#.##" );


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
        queries[10] = q11();
        queries[11] = q12();
        queries[12] = q13();
        queries[13] = q14();
        queries[14] = q15();
        queries[15] = q16();
        queries[16] = q17();
        queries[17] = q18();
        return queries;
    }


    private static String q1() {
        return "select c_name from tpch_customer, tpch_nation where c_nationkey = n_nationkey limit all;";
    }


    private static String q2() {
        return "select c_name from tpch_customer, tpch_orders where c_custkey = o_orderkey limit all;";
    }


    private static String q3() {
        return "select c_name from tpch_orders, tpch_customer, tpch_nation where o_custkey = c_custkey and c_nationkey = n_nationkey limit all";
    }


    private static String q4() {
        return "select l_linenumber from tpch_lineitem, tpch_orders, tpch_customer, tpch_nation where l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey limit all;";
    }


    private static String q5() {
        return "select l_linenumber from tpch_part, tpch_lineitem, tpch_orders, tpch_customer, tpch_nation where p_partkey = l_partkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey limit all;";
    }


    private static String q6() {
        return "select o_orderdate from tpch_customer, tpch_nation, tpch_orders, tpch_lineitem, tpch_part, tpch_partsupp where c_nationkey = n_nationkey and c_custkey = o_custkey and l_orderkey = o_orderkey and l_partkey = p_partkey and p_partkey = ps_partkey;";
    }


    private static String q7() {
        return "select ps_availqty from tpch_customer, tpch_nation, tpch_orders, tpch_lineitem, tpch_part, tpch_partsupp, tpch_supplier where c_nationkey = n_nationkey and c_custkey = o_orderkey and l_orderkey = o_orderkey and l_partkey = p_partkey and p_partkey = ps_partkey and s_suppkey = ps_suppkey limit all;";
    }


    private static String q8() {
        return "select c_name from tpch_customer, tpch_nation, tpch_region where c_nationkey = n_nationkey and n_regionkey = r_regionkey limit all;";
    }


    private static String q9() {
        return "select p_name from tpch_part, tpch_partsupp where p_partkey = ps_partkey limit all;";
    }


    private static String q10() {
        return "select r_name from tpch_customer, tpch_nation, tpch_region, tpch_orders where c_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and c_custkey = o_custkey and c_custkey < 200 limit all;";
    }


    private static String q11() {
        return "select l_discount from tpch_lineitem, tpch_orders, tpch_customer, tpch_nation, tpch_region where l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' limit all;";
    }


    private static String q12() {
        return "select c_name from tpch_customer, tpch_orders, tpch_lineitem, tpch_nation where c_custkey = o_custkey and o_orderkey = l_orderkey and c_nationkey = n_nationkey and c_nationkey = 8 limit all;";
    }


    private static String q13() {
        return "select c_name from tpch_customer, tpch_nation, tpch_region, tpch_orders where c_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and c_custkey = o_custkey limit all;";
    }


    private static String q14() {
        return "select o_orderkey from tpch_orders, tpch_customer, tpch_nation, tpch_region where o_custkey = c_custkey and c_nationkey = n_nationkey and n_regionkey = r_regionkey and o_orderkey < 100 limit all;";
    }


    private static String q15() {
        return "select l_partkey from tpch_lineitem, tpch_orders, tpch_partsupp, tpch_part where l_orderkey = o_orderkey and l_partkey = ps_partkey and p_partkey = ps_partkey and l_orderkey = 5 limit all";
    }


    private static String q16() {
        return "select o_orderdate from tpch_partsupp, tpch_part, tpch_lineitem, tpch_orders, tpch_customer, tpch_nation where p_partkey = ps_partkey and p_partkey = l_partkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey and p_retailprice < 2000 and n_name = 'AFRICA' limit all";
    }


    private static String q17() {
        return "select n_nationkey from tpch_partsupp, tpch_supplier, tpch_nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'EUROPE' limit all;";
    }


    private static String q18() {
        return "select p_name from tpch_part, tpch_partsupp, tpch_supplier where p_partkey = ps_partkey and ps_suppkey = s_suppkey and p_partkey = 3 limit all;";
    }

}
