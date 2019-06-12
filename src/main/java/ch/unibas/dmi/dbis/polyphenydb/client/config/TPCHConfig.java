package ch.unibas.dmi.dbis.polyphenydb.client.config;


/**
 * Collection of static parameters of the TPC-H Benchmark. The default values come from the TPC-H Specification 2.17.1
 *
 * @author Manuel Huerbin
 * @tpchversion 2.17.1
 */
public class TPCHConfig {

    public static final int PART = 1; // part must be at least 1
    public static final int PART_COUNT = 1; // part must be less than or equal to part count
    /*
      Minimum Required Stream Count (5.3.4, page 95 / 96)
      SF      S(Streams)
      1       2
      10      3
      30      4
      100     5
      300     6
      1000    7
      3000    8
      10000   9
      30000   10
      100000  11
     */
    public static final int STREAMS = 2; // (S) number of query streams used in Throughput test (depends on SF (refer to page 96 of the TPCH-Specification)
}
