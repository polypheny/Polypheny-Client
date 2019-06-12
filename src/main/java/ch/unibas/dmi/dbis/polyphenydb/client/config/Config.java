package ch.unibas.dmi.dbis.polyphenydb.client.config;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.AccessMethod;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.DBMSSystem;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.Scenario;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


/**
 * Stores default values for the Polypheny-Client. In the future, this could be handled by a config.json which sets default-values
 *
 * Values in this Config should only accessed by Classes in the 'job' package - neither master nor worker should rely on config-values. Instead, they should either access the job (master) or receive their info from master (workers)
 *
 * @author Silvan Heller
 */
public class Config {

    public static final int DEFAULT_WORKER_PORT = 8333;
    public static final String DEFAULT_HOST = "10.34.58.105"; // remote host: ?.?.?.?
    public static final int DEFAULT_DBMS_PORT = 5432;
    public static final String DEFAULT_DATABASE_NAME = "polypheny";
    public static final String DEFAULT_DBMS_USERNAME = "polypheny";
    public static final String DEFAULT_DBMS_PASSWORD = "polypheny";

    public static final String DEFAULT_CHRONOS_SYSTEM = "polyphenydb";


    public static final String DEFAULT_SCENARIO = Scenario.SCENARIOTPCC.toString();

    public static final int DEFAULT_WORKERS = 1;
    /**
     * If you want more than one DEFAULT_WORKER_URL, Seperate them with comma and no space after the comma. Example 'localhost,133.7.133.7'
     */
    public static final String DEFAULT_WORKER_URL = "localhost";

    public static final List<String> DEFAULT_NETDATA_MEASUREMENTS = Arrays.asList( "system.cpu", "system.io" );
    public static final String DEFAULT_ACCESS_METHOD = AccessMethod.ACCESSJDBC.toString();

    public static final String DEFAULT_DBMS_SYSTEM = DBMSSystem.SYSTEMPOSTGRESQL.toString();
    public static final int MEASUREMENT_TIME_MIN = 1;

    //TPCC
    public static final int DEFAULT_TPCC_WAREHOUSES = 1;
    public static final boolean TPCC_TERMINAL_THINK = false;
    //TPCH
    public static final double TPCH_SCALEFACTOR = 1;
    //YCSB
    public static final int DEFAULT_YCSB_THREADS = 1;
    public static final String DEFAULT_YCSB_PROPERTIES_PATH = "ycsb_postgres.properties";

    public static final boolean POPULATE_DATABASE = false;

    public static final String DEFAULT_INPUT_FOLDER = "input/";
    public static final String DEFAULT_OUTPUT_FOLDER = "output/";
    public static final boolean CREATE_SCHEMA = false;

    public static final String DEFAULT_WORKER_STORAGE_LOCATION = "storage/worker/" + new SimpleDateFormat( "MM_dd_HH_mm_ss" ).format( Date.from( Instant.now() ) );
    public static final String DEFAULT_MASTER_STORAGE_LOCATION = "storage/master/" + new SimpleDateFormat( "MM_dd_HH_mm_ss" ).format( Date.from( Instant.now() ) );

    public static final boolean EXECUTE_REFRESH_FUNCTIONS = false;
    public static final boolean EXECUTE_POWER_TEST = true;
}
