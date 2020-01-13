package org.polypheny.client.db.tpcc;


import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.chronos.ProgressListener;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.access.JDBCConnector;
import org.polypheny.client.db.access.JDBCPostgresConnector;
import org.polypheny.client.db.tpcc.transactions.DeliveryTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.NewOrderTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.OrderStatusTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.PaymentTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.StockLevelTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.postgres.PostgresDeliveryTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.postgres.PostgresNewOrderTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.postgres.PostgresOrderStatusTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.postgres.PostgresPaymentTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.postgres.PostgresStockLevelTransactionExecutor;
import org.polypheny.client.grpc.PolyClientGRPC.AccessMethod;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.job.PolyphenyJobCdl;
import org.polypheny.client.scenarios.tpcc.worker.TPCCWorker;


/**
 * The PostgreSQL implementation of the {@link TPCCBenchmarker}. Uses the {@link JDBCConnector} to connect with databases and the {@link PostgresNewOrderTransactionExecutor} for new-order transactions. Sort-of reference implementation for new DBMS which want to be benchmarked.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class PostgresTpccBenchmarker extends TPCCBenchmarker {

    private static final int BATCH_SIZE = 10_000;
    private DBConnector connector;
    private Logger logger = LogManager.getLogger();
    private NewOrderTransactionExecutor newOrderTransactionExecutor;
    private PaymentTransactionExecutor paymentTransactionExecutor;
    private OrderStatusTransactionExecutor orderStatusTransactionExecutor;
    private StockLevelTransactionExecutor stockLevelTransactionExecutor;
    private DeliveryTransactionExecutor deliveryTransactionExecutor;


    public PostgresTpccBenchmarker( PolyphenyJobCdl job ) throws ConnectionException {
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
    public PostgresTpccBenchmarker( AccessMethod accessMethod, String dbHost, int port, String database, String username, String password, int NUMBER_OF_CONFIGURED_WAREHOUSES ) throws ConnectionException {
        super( NUMBER_OF_CONFIGURED_WAREHOUSES );
        if ( accessMethod.getNumber() != AccessMethod.ACCESSJDBC_VALUE ) {
            logger.error( "For Postgres-Benchmarking, only JDBC is supported" );
            throw new IllegalArgumentException();
        }
        this.connector = new JDBCPostgresConnector( dbHost, port, database, username, password );
        this.newOrderTransactionExecutor = new PostgresNewOrderTransactionExecutor( NUMBER_OF_CONFIGURED_WAREHOUSES, this.connector, this );
        this.paymentTransactionExecutor = new PostgresPaymentTransactionExecutor( this.connector, this );
        this.orderStatusTransactionExecutor = new PostgresOrderStatusTransactionExecutor( this.connector );
        this.stockLevelTransactionExecutor = new PostgresStockLevelTransactionExecutor( this.connector );
        this.deliveryTransactionExecutor = new PostgresDeliveryTransactionExecutor( this.connector );
    }


    /**
     * Creates a Benchmarker from a {@link LaunchWorkerMessage}. This is mostly called from a {@link TPCCWorker}.
     *
     * @param workerMessage {@link LaunchWorkerMessage} which will be used to determine Access Method etc.
     */
    public PostgresTpccBenchmarker( LaunchWorkerMessage workerMessage ) throws ConnectionException {
        this( workerMessage.getDbInfo().getAccessMethod(), workerMessage.getDbInfo().getDbHost(), workerMessage.getDbInfo().getDbPort(), workerMessage.getDbInfo().getDatabase(), workerMessage.getDbInfo().getUsername(), workerMessage.getDbInfo().getPassword(),
                workerMessage.getTpccWorkerMessage().getNUMBEROFCONFIGUREDWAREHOUSES() );
    }


    @Override
    public void populateDatabase( ProgressListener progressListener ) {
        try {
            logger.trace( "Truncating tables" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_warehouse CASCADE" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_district CASCADE" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_customer CASCADE" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_history CASCADE" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_order CASCADE" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_new_order CASCADE" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_item CASCADE" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_stock CASCADE" );
            connector.executeStatement( "TRUNCATE TABLE tpcc_order_line CASCADE" );
            logger.debug( "Tables truncated" );
        } catch ( ConnectionException e ) {
            throw new RuntimeException( e );
        }
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
        logger.info( "Creating Tables" );
        try {
            connector.executeScript( new File( "src/main/sql/tpcc.sql" ) );
            //TODO Jar execution
            connector.commitTransaction();
        } catch ( ConnectionException e ) {
            logger.fatal( "Error while creating Tables", e );
            abort();
            throw new RuntimeException( e );
        }
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
