package ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc;


import ch.unibas.dmi.dbis.polyphenydb.client.chronos.ProgressListener;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.ConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.DBConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.JDBCConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.PolyphenyDbJdbcConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.DeliveryTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.NewOrderTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.OrderStatusTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.PaymentTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.StockLevelTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.polyphenydb.PolyphenyDbDeliveryTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.polyphenydb.PolyphenyDbNewOrderTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.polyphenydb.PolyphenyDbOrderStatusTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.polyphenydb.PolyphenyDbPaymentTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.polyphenydb.PolyphenyDbStockLevelTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.postgres.PostgresNewOrderTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.AccessMethod;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.job.PolyphenyJobCdl;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.tpcc.worker.TPCCWorker;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The PostgreSQL implementation of the {@link TPCCBenchmarker}. Uses the {@link JDBCConnector} to connect with databases and the {@link PostgresNewOrderTransactionExecutor} for new-order transactions. Sort-of reference implementation for new DBMS which want to be benchmarked.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class PolyphenyDbTpccBenchmarker extends TPCCBenchmarker {

    private static final int BATCH_SIZE = 1_000;
    private DBConnector connector;
    private Logger logger = LogManager.getLogger();
    private NewOrderTransactionExecutor newOrderTransactionExecutor;
    private PaymentTransactionExecutor paymentTransactionExecutor;
    private OrderStatusTransactionExecutor orderStatusTransactionExecutor;
    private StockLevelTransactionExecutor stockLevelTransactionExecutor;
    private DeliveryTransactionExecutor deliveryTransactionExecutor;


    public PolyphenyDbTpccBenchmarker( PolyphenyJobCdl job ) throws ConnectionException {
        this( job.getEvaluation().getOptions().getAccessMethod(), job.getEvaluation().getDbms().getHost(), job.getEvaluation().getDbms().getPort(), job.getEvaluation().getDbms().getDatabase(), job.getEvaluation().getDbms().getUsername(),
                job.getEvaluation().getDbms().getPassword(), job.getEvaluation().getOptions().getTpccWarehouses() );
    }


    /**
     * Creates a Benchmarker for PostgreSQL using the specified {@link AccessMethod}.
     *
     * @param accessMethod AccessMethod to be used for Connection.
     * @param dbHost IP or URL, no prefixes
     * @param port port for the DB
     * @param NUMBER_OF_CONFIGURED_WAREHOUSES how many warehouses this benchmark should use
     */
    public PolyphenyDbTpccBenchmarker( AccessMethod accessMethod, String dbHost, int port, String database, String username, String password, int NUMBER_OF_CONFIGURED_WAREHOUSES ) throws ConnectionException {
        super( NUMBER_OF_CONFIGURED_WAREHOUSES );
        if ( accessMethod.getNumber() != AccessMethod.ACCESSJDBC_VALUE ) {
            logger.error( "For Postgres-Benchmarking, only JDBC is supported" );
            throw new IllegalArgumentException();
        }
        this.connector = new PolyphenyDbJdbcConnector( dbHost, port, database, username, password, "false" );
        this.newOrderTransactionExecutor = new PolyphenyDbNewOrderTransactionExecutor( NUMBER_OF_CONFIGURED_WAREHOUSES, this.connector, this );
        this.paymentTransactionExecutor = new PolyphenyDbPaymentTransactionExecutor( this.connector, this );
        this.orderStatusTransactionExecutor = new PolyphenyDbOrderStatusTransactionExecutor( this.connector );
        this.stockLevelTransactionExecutor = new PolyphenyDbStockLevelTransactionExecutor( this.connector );
        this.deliveryTransactionExecutor = new PolyphenyDbDeliveryTransactionExecutor( this.connector );
    }


    /**
     * Creates a Benchmarker from a {@link LaunchWorkerMessage}. This is mostly called from a {@link TPCCWorker}.
     *
     * @param workerMessage {@link LaunchWorkerMessage} which will be used to determine Access Method etc.
     */
    public PolyphenyDbTpccBenchmarker( LaunchWorkerMessage workerMessage ) throws ConnectionException {
        this( workerMessage.getDbInfo().getAccessMethod(), workerMessage.getDbInfo().getDbHost(), workerMessage.getDbInfo().getDbPort(), workerMessage.getDbInfo().getDatabase(), workerMessage.getDbInfo().getUsername(), workerMessage.getDbInfo().getPassword(),
                workerMessage.getTpccWorkerMessage().getNUMBEROFCONFIGUREDWAREHOUSES() );
    }


    @Override
    public void populateDatabase( ProgressListener progressListener ) {
        createTables( progressListener );
        super.populateDatabase( progressListener );
    }


    @Override
    public OrderStatusTransactionExecutor getOrderStatusExecutor() {
        return this.orderStatusTransactionExecutor;
    }


    @Override
    public NewOrderTransactionExecutor getNewOrderExecutor() {
        return this.newOrderTransactionExecutor;
    }


    @Override
    public PaymentTransactionExecutor getPaymentExecutor() {
        return this.paymentTransactionExecutor;
    }


    @Override
    public StockLevelTransactionExecutor getStockLevelTransactionExecutor() {
        return this.stockLevelTransactionExecutor;
    }


    @Override
    public DeliveryTransactionExecutor getDeliveryTransactionExecutor() {
        return this.deliveryTransactionExecutor;
    }


    @Override
    public void createTables( ProgressListener progressListener ) {
        logger.info( "Dropping existing Tables" );
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_order_line" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        logger.info( "Dropped table" );
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_stock" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_item" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_new_order" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_order" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_history" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_customer" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_district" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        try { // TPC-C
            connector.executeStatement( "DROP TABLE tpcc_warehouse" );
        } catch ( ConnectionException e ) {
            //Ignore
        }
        logger.info( "Creating tables" );
        try { // TPC-C
            connector.executeStatement( "CREATE TABLE tpcc_warehouse ("
                    + "  W_ID       INTEGER ,"
                    + "  W_NAME     VARCHAR(10) NOT NULL,"
                    + "  W_STREET_1 VARCHAR(20) NOT NULL,"
                    + "  W_STREET_2 VARCHAR(20) NOT NULL,"
                    + "  W_CITY     VARCHAR(20) NOT NULL,"
                    + "  W_STATE    VARCHAR(2) NOT NULL,"
                    + "  W_ZIP      VARCHAR(9) NOT NULL,"
                    + "  W_TAX      DECIMAL NOT NULL,"
                    + "  W_YTD      DECIMAL NOT NULL"
                    + ")" );
            logger.info( "Created first table" );
            connector.executeStatement( "CREATE TABLE tpcc_district ("
                    + "  D_ID        INTEGER NOT NULL,"
                    + "  D_W_ID      INTEGER NOT NULL,"
                    + "  D_NAME      VARCHAR(10) NOT NULL,"
                    + "  D_STREET_1  VARCHAR(20) NOT NULL,"
                    + "  D_STREET_2  VARCHAR(20) NOT NULL,"
                    + "  D_CITY      VARCHAR(20) NOT NULL,"
                    + "  D_STATE     VARCHAR(2) NOT NULL,"
                    + "  D_ZIP       INTEGER NOT NULL,"
                    + "  D_TAX       DECIMAL NOT NULL,"
                    + "  D_YTD       DECIMAL NOT NULL,"
                    + "  D_NEXT_O_ID INTEGER NOT NULL"
                    + ")" );
            connector.executeStatement( "CREATE TABLE tpcc_customer ("
                    + "  C_ID          INTEGER NOT NULL,"
                    + "  C_D_ID        INTEGER NOT NULL,"
                    + "  C_W_ID        INTEGER NOT NULL,"
                    + "  C_FIRST       VARCHAR(16) NOT NULL,"
                    + "  C_MIDDLE      VARCHAR(2) NOT NULL,"
                    + "  C_LAST        VARCHAR(16) NOT NULL,"
                    + "  C_STREET_1    VARCHAR(20) NOT NULL,"
                    + "  C_STREET_2    VARCHAR(20) NOT NULL,"
                    + "  C_CITY        VARCHAR(20) NOT NULL,"
                    + "  C_STATE       VARCHAR(2) NOT NULL,"
                    + "  C_ZIP         INTEGER NOT NULL,"
                    + "  C_PHONE       BIGINT NOT NULL,"
                    + "  C_SINCE       TIMESTAMP NOT NULL,"
                    + "  C_CREDIT      VARCHAR(2) NOT NULL,"
                    + "  C_CREDIT_LIM  DECIMAL NOT NULL,"
                    + "  C_DISCOUNT    DECIMAL NOT NULL,"
                    + "  C_BALANCE     DECIMAL NOT NULL,"
                    + "  C_YTD_PAYMENT DECIMAL NOT NULL,"
                    + "  C_PAYMENT_CNT DECIMAL NOT NULL,"
                    + "  C_DELIVERY_CNT DECIMAL NOT NULL,"
                    + "  C_DATA        VARCHAR(500) NOT NULL"
                    + ")" );
            connector.executeStatement( "CREATE TABLE tpcc_history ("
                    + "  H_C_ID   INTEGER NOT NULL,"
                    + "  H_C_D_ID INTEGER NOT NULL,"
                    + "  H_C_W_ID INTEGER NOT NULL,"
                    + "  H_D_ID   INTEGER NOT NULL,"
                    + "  H_W_ID   INTEGER NOT NULL,"
                    + "  H_DATE   TIMESTAMP NOT NULL,"
                    + "  H_AMOUNT DECIMAL NOT NULL,"
                    + "  H_DATA   VARCHAR(24) NOT NULL"
                    + ")" );
            connector.executeStatement( "CREATE TABLE tpcc_order ("
                    + "  O_ID         INTEGER NOT NULL,"
                    + "  O_D_ID       INTEGER NOT NULL,"
                    + "  O_W_ID       INTEGER NOT NULL,"
                    + "  O_C_ID       INTEGER NOT NULL,"
                    + "  O_ENTRY_D    TIMESTAMP NOT NULL,"
                    + "  O_CARRIER_ID INTEGER,"
                    + "  O_OL_CNT     DECIMAL NOT NULL,"
                    + "  O_ALL_LOCAL  DECIMAL NOT NULL"
                    + ")" );
            connector.executeStatement( "CREATE TABLE tpcc_new_order ("
                    + "  NO_O_ID INTEGER NOT NULL,"
                    + "  NO_D_ID INTEGER NOT NULL,"
                    + "  NO_W_ID INTEGER NOT NULL"
                    + ")" );
            connector.executeStatement( "CREATE TABLE tpcc_item ("
                    + "  I_ID    INTEGER NOT NULL,"
                    + "  I_IM_ID INTEGER NOT NULL,"
                    + "  I_NAME  VARCHAR(24) NOT NULL,"
                    + "  I_PRICE DECIMAL NOT NULL,"
                    + "  I_DATA  VARCHAR(50) NOT NULL"
                    + ")" );
            connector.executeStatement( "CREATE TABLE tpcc_stock ("
                    + "  S_I_ID       INTEGER NOT NULL,"
                    + "  S_W_ID       INTEGER NOT NULL,"
                    + "  S_QUANTITY   DECIMAL NOT NULL,"
                    + "  S_DIST_01    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_02    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_03    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_04    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_05    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_06    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_07    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_08    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_09    VARCHAR(24) NOT NULL,"
                    + "  S_DIST_10    VARCHAR(24) NOT NULL,"
                    + "  S_YTD        DECIMAL NOT NULL,"
                    + "  S_ORDER_CNT  DECIMAL NOT NULL,"
                    + "  S_REMOTE_CNT DECIMAL NOT NULL,"
                    + "  S_DATA       VARCHAR(50) NOT NULL"
                    + ")" );
            connector.executeStatement( "CREATE TABLE tpcc_order_line ("
                    + "  OL_O_ID        INTEGER NOT NULL,"
                    + "  OL_D_ID        INTEGER NOT NULL,"
                    + "  OL_W_ID        INTEGER NOT NULL,"
                    + "  OL_NUMBER      INTEGER NOT NULL,"
                    + "  OL_I_ID        INTEGER NOT NULL,"
                    + "  OL_SUPPLY_W_ID INTEGER NOT NULL,"
                    + "  OL_DELIVERY_D  TIMESTAMP,"
                    + "  OL_QUANTITY    DECIMAL NOT NULL,"
                    + "  OL_AMOUNT      DECIMAL NOT NULL,"
                    + "  OL_DIST_INFO   VARCHAR(24) NOT NULL"
                    + ")" );
            connector.commitTransaction();
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
        logger.info( "Tables created" );
    }


    @Override
    protected int getBatchSize() {
        return BATCH_SIZE;
    }


    @Override
    public DBConnector getConnector() {
        return connector;
    }
}
