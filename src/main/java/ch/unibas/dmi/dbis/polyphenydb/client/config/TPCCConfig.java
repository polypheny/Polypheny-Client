package ch.unibas.dmi.dbis.polyphenydb.client.config;


/**
 * Collection of static parameters of the TPC-C Benchmark. The default values come from the TPC-C Specification 5.11
 *
 * This config is an embarrassing attempt to make the TPC-C Benchmark more configurable. Unfortunately, these 'magic numbers' are intertwined in the whole Specification such that changing numbers here almost certainly will break things because it is not always clear which methods depend on what.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class TPCCConfig {

    public static final int NUMBER_OF_ITEMS = 100000;
    public static final int STOCK_PER_WAREHOUSE = 100000;
    public static final int DISTRICTS_PER_WAREHOUSE = 10;
    public static final int CUSTOMERS_PER_DISTRICT = 3000;
    public static final int HISTORY_ROWS_PER_CUSTOMER = 1;
    public static final int ORDER_ROWS_PER_DISTRICT = 3000;
    public static final int TERMINALS_PER_DISTRICT = 1;
}
