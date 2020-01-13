package org.polypheny.client.db.tpch;


import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.config.TPCHConfig;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.access.JDBCPostgresConnector;
import org.polypheny.client.generator.tpch.TPCHPopulationGenerators.CustomerGenerator;
import org.polypheny.client.generator.tpch.TPCHPopulationGenerators.LineItemGenerator;
import org.polypheny.client.generator.tpch.TPCHPopulationGenerators.NationGenerator;
import org.polypheny.client.generator.tpch.TPCHPopulationGenerators.OrderGenerator;
import org.polypheny.client.generator.tpch.TPCHPopulationGenerators.PartGenerator;
import org.polypheny.client.generator.tpch.TPCHPopulationGenerators.PartSupplierGenerator;
import org.polypheny.client.generator.tpch.TPCHPopulationGenerators.RegionGenerator;
import org.polypheny.client.generator.tpch.TPCHPopulationGenerators.SupplierGenerator;
import org.polypheny.client.generator.tpch.objects.Customer;
import org.polypheny.client.generator.tpch.objects.GenerateUtils;
import org.polypheny.client.generator.tpch.objects.LineItem;
import org.polypheny.client.generator.tpch.objects.Nation;
import org.polypheny.client.generator.tpch.objects.Order;
import org.polypheny.client.generator.tpch.objects.Part;
import org.polypheny.client.generator.tpch.objects.PartSupplier;
import org.polypheny.client.generator.tpch.objects.Region;
import org.polypheny.client.generator.tpch.objects.Supplier;
import org.polypheny.client.grpc.PolyClientGRPC.AccessMethod;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHTransactionType;
import org.polypheny.client.rpc.ProtoObjectFactory;
import org.polypheny.client.scenarios.tpch.worker.TPCHWorker;


/**
 * Executes Transactions using JDBC
 *
 * @author manuel on 11.04.17.
 */
public class PostgresTpchBenchmarker extends TPCHBenchmarker {

    private static final int BATCH_SIZE = 10_000;
    private final double SCALE_FACTOR;
    private DBConnector connector;
    private Logger logger = LogManager.getLogger();


    /**
     * Creates a Benchmarker for PostgreSQL using the specified {@link AccessMethod}.
     *
     * @param accessMethod AccessMethod to be used for Connection.
     * @param dbHost IP or URL, no prefixes
     * @param port port for the DB
     * @param SCALE_FACTOR the scale factor this benchmark should use
     */
    public PostgresTpchBenchmarker( AccessMethod accessMethod, String dbHost, int port, String database, String username, String password, double SCALE_FACTOR ) throws ConnectionException {
        super( SCALE_FACTOR );
        this.SCALE_FACTOR = SCALE_FACTOR;
        if ( accessMethod.getNumber() != AccessMethod.ACCESSJDBC_VALUE ) {
            logger.error( "For Postgres-Benchmarking, only JDBC is supported" );
            throw new IllegalArgumentException();
        }
        this.connector = new JDBCPostgresConnector( dbHost, port, database, username, password );
    }


    /**
     * Creates a Benchmarker from a {@link LaunchWorkerMessage}. This is mostly called from a {@link TPCHWorker}.
     *
     * @param workerMessage {@link LaunchWorkerMessage} which will be used to determine Access Method etc.
     */
    public PostgresTpchBenchmarker( LaunchWorkerMessage workerMessage ) throws ConnectionException {
        this( workerMessage.getDbInfo().getAccessMethod(), workerMessage.getDbInfo().getDbHost(), workerMessage.getDbInfo().getDbPort(), workerMessage.getDbInfo().getDatabase(), workerMessage.getDbInfo().getUsername(), workerMessage.getDbInfo().getPassword(),
                workerMessage.getTpchWorkerMessage().getSCALEFACTOR() );
    }


    @Override
    public void createTables() {
        logger.trace( "Creating Tables" );
        try {
            connector.executeScript( new File( "src/main/sql/tpch.sql" ) );
            //TODO Jar execution needs modification
            connector.commitTransaction();
        } catch ( ConnectionException e ) {
            logger.fatal( "Error while creating Tables", e );
            abort();
            throw new RuntimeException( e );
        }
    }


    @Override
    public void abort() {
        logger.trace( "Aborting TPC-H Benchmarker" );
        try {
            connector.abortTransaction();
        } catch ( ConnectionException e ) {
            logger.error( e );
            //Ignore
        }
        connector.close();
    }


    // populate table "tcph_part"
    public void writeParts( double scaleFactor, int part, int partCount ) {
        try {
            logger.trace( "Writing tpch_parts ..." );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            int cnt = 1;
            query.append( "INSERT INTO tpch_part VALUES" );
            for ( Part entity : new PartGenerator( scaleFactor, part, partCount ) ) {
                query.append( "(" );
                query.append( entity.getPartKey() );
                query.append( "," );
                query.append( "\'" + entity.getName() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getManufacturer() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getBrand() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getType() + "\'" ); // String
                query.append( "," );
                query.append( entity.getSize() );
                query.append( "," );
                query.append( "\'" + entity.getContainer() + "\'" );
                query.append( "," );
                query.append( entity.getRetailPrice() ); //.getRetailPriceInCents
                query.append( "," );
                query.append( "\'" + entity.getComment() + "\'" ); // String
                query.append( ")" );
                if ( cnt % BATCH_SIZE == 0 && cnt != scaleFactor * 200_000 ) {
                    query.append( ";" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    query = new StringBuilder();
                    query.append( "INSERT INTO tpch_part VALUES" );
                } else {
                    if ( cnt != scaleFactor * 200_000 ) {
                        query.append( "," );
                    }
                }
                cnt++;
            }
            query.append( ";" );
            connector.executeStatement( query.toString() );
            connector.commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished writing tpch_part rows. Elapsed time: {} ms", (stop - start) );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database 'tpch_part'. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating table tpch_part" );
    }


    // populate table "tcph_supplier"
    public void writeSuppliers( double scaleFactor, int part, int partCount ) {
        try {
            logger.trace( "Writing tpch_suppliers ..." );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            int cnt = 1;
            query.append( "INSERT INTO tpch_supplier VALUES" );
            for ( Supplier entity : new SupplierGenerator( scaleFactor, part, partCount ) ) {
                query.append( "(" );
                query.append( entity.getSupplierKey() );
                query.append( "," );
                query.append( "\'" + entity.getName() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getAddress() + "\'" ); // String
                query.append( "," );
                query.append( entity.getNationKey() );
                query.append( "," );
                query.append( "\'" + entity.getPhone() + "\'" ); // String
                query.append( "," );
                query.append( entity.getAccountBalance() ); //.getAccountBalanceInCents()
                query.append( "," );
                query.append( "\'" + entity.getComment() + "\'" ); // String
                query.append( ")" );
                if ( cnt % BATCH_SIZE == 0 && cnt != scaleFactor * 10_000 ) {
                    query.append( ";" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    query = new StringBuilder();
                    query.append( "INSERT INTO tpch_supplier VALUES" );
                } else {
                    if ( cnt != scaleFactor * 10_000 ) {
                        query.append( "," );
                    }
                }
                cnt++;
            }
            query.append( ";" );
            connector.executeStatement( query.toString() );
            connector.commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished writing tpch_supplier rows. Elapsed time: {} ms", (stop - start) );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database 'tpch_supplier'. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating table tpch_suppliers" );
    }


    // populate table "tcph_partsupp"
    public void writePartSuppliers( double scaleFactor, int part, int partCount ) {
        try {
            logger.trace( "Writing tpch_partsuppliers ..." );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            int cnt = 1;
            query.append( "INSERT INTO tpch_partsupp VALUES" );
            for ( PartSupplier entity : new PartSupplierGenerator( scaleFactor, part, partCount ) ) {
                query.append( "(" );
                query.append( entity.getPartKey() );
                query.append( "," );
                query.append( entity.getSupplierKey() );
                query.append( "," );
                query.append( entity.getAvailableQuantity() );
                query.append( "," );
                query.append( entity.getSupplyCost() ); //.getSupplyCostInCents()
                query.append( "," );
                query.append( "\'" + entity.getComment() + "\'" ); // String
                query.append( ")" );
                if ( cnt % BATCH_SIZE == 0 && cnt != scaleFactor * 800_000 ) {
                    query.append( ";" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    query = new StringBuilder();
                    query.append( "INSERT INTO tpch_partsupp VALUES" );
                } else {
                    if ( cnt != scaleFactor * 800_000 ) {
                        query.append( "," );
                    }
                }
                cnt++;
            }
            query.append( ";" );
            connector.executeStatement( query.toString() );
            connector.commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished writing tpch_partsupp rows. Elapsed time: {} ms", (stop - start) );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database 'tpch_partsupp'. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating table tpch_partsuppliers" );
    }


    // populate table "tcph_customer"
    public void writeCustomers( double scaleFactor, int part, int partCount ) {
        try {
            logger.trace( "Writing tpch_customers ..." );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            int cnt = 1;
            query.append( "INSERT INTO tpch_customer VALUES" );
            for ( Customer entity : new CustomerGenerator( scaleFactor, part, partCount ) ) {
                query.append( "(" );
                query.append( entity.getCustomerKey() );
                query.append( "," );
                query.append( "\'" + entity.getName() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getAddress() + "\'" ); // String
                query.append( "," );
                query.append( entity.getNationKey() );
                query.append( "," );
                query.append( "\'" + entity.getPhone() + "\'" ); // String
                query.append( "," );
                query.append( entity.getAccountBalance() );
                query.append( "," );
                query.append( "\'" + entity.getMarketSegment() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getComment() + "\'" ); // String
                query.append( ")" );
                if ( cnt % BATCH_SIZE == 0 && cnt != scaleFactor * 150_000 ) {
                    query.append( ";" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    query = new StringBuilder();
                    query.append( "INSERT INTO tpch_customer VALUES" );
                } else {
                    if ( cnt != scaleFactor * 150_000 ) {
                        query.append( "," );
                    }
                }
                cnt++;
            }
            query.append( ";" );
            connector.executeStatement( query.toString() );
            connector.commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished writing tpch_customer rows. Elapsed time: {} ms", (stop - start) );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database 'tpch_customer'. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating table tpch_customers" );
    }


    // populate table "tcph_orders"
    public void writeOrders( double scaleFactor, int part, int partCount ) {
        try {
            logger.trace( "Writing tpch_orders ..." );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            int cnt = 1;
            query.append( "INSERT INTO tpch_orders VALUES" );
            for ( Order entity : new OrderGenerator( scaleFactor, part, partCount ) ) {
                query.append( "(" );
                query.append( entity.getOrderKey() );
                query.append( "," );
                query.append( entity.getCustomerKey() );
                query.append( "," );
                query.append( "\'" + entity.getOrderStatus() + "\'" ); // char
                query.append( "," );
                query.append( entity.getTotalPrice() ); //.getTotalPriceInCents
                query.append( "," );
                String orderDate = GenerateUtils.formatDate( entity.getOrderDate() ); // Date
                query.append( "TO_DATE(\'" + orderDate + "\', \'YYYY-MM-DD\')" );
                query.append( "," );
                query.append( "\'" + entity.getOrderPriority() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getClerk() + "\'" ); // String
                query.append( "," );
                query.append( entity.getShipPriority() );
                query.append( "," );
                query.append( "\'" + entity.getComment() + "\'" ); // String
                query.append( ")" );
                if ( cnt % BATCH_SIZE == 0 && cnt != scaleFactor * 1_500_000 ) {
                    query.append( ";" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    query = new StringBuilder();
                    query.append( "INSERT INTO tpch_orders VALUES" );
                } else {
                    if ( cnt != scaleFactor * 1_500_000 ) {
                        query.append( "," );
                    }
                }
                cnt++;
            }
            query.append( ";" );
            connector.executeStatement( query.toString() );
            connector.commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished writing tpch_orders part rows. Elapsed time: {} ms", (stop - start) );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database 'tpch_orders'. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating table tpch_orders" );
    }


    // populate table "tcph_lineitem"
    public void writeLineItems( double scaleFactor, int part, int partCount ) {
        try {
            logger.trace( "Writing tpch_lineitems ..." );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            int cnt = 1;
            query.append( "INSERT INTO tpch_lineitem VALUES" );
            for ( LineItem entity : new LineItemGenerator( scaleFactor, part, partCount ) ) {
                query.append( "(" );
                query.append( entity.getOrderKey() );
                query.append( "," );
                query.append( entity.getPartKey() );
                query.append( "," );
                query.append( entity.getSupplierKey() );
                query.append( "," );
                query.append( entity.getLineNumber() );
                query.append( "," );
                query.append( entity.getQuantity() );
                query.append( "," );
                query.append( entity.getExtendedPrice() ); //.getExtendedPriceInCents()
                query.append( "," );
                query.append( entity.getDiscount() );
                query.append( "," );
                query.append( entity.getTax() ); //.getTaxPercent()
                query.append( "," );
                query.append( "\'" + entity.getReturnFlag() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getStatus() + "\'" ); // String
                query.append( "," );
                String shipDate = GenerateUtils.formatDate( entity.getShipDate() ); // Date
                query.append( "TO_DATE(\'" + shipDate + "\', \'YYYY-MM-DD\')" );
                query.append( "," );
                String commitDate = GenerateUtils.formatDate( entity.getCommitDate() ); // Date
                query.append( "TO_DATE(\'" + commitDate + "\', \'YYYY-MM-DD\')" );
                query.append( "," );
                String receiptDate = GenerateUtils.formatDate( entity.getReceiptDate() );
                query.append( "TO_DATE(\'" + receiptDate + "\', \'YYYY-MM-DD\')" ); // Date
                query.append( "," );
                query.append( "\'" + entity.getShipInstructions() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getShipMode() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getComment() + "\'" ); // String
                query.append( ")" );
                if ( cnt % BATCH_SIZE == 0 && cnt != scaleFactor * 6_001_215 ) {
                    query.append( ";" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    query = new StringBuilder();
                    query.append( "INSERT INTO tpch_lineitem VALUES" );
                } else {
                    if ( cnt != scaleFactor * 6_001_215 ) {
                        query.append( "," );
                    }
                }
                cnt++;
            }
            query.append( ";" );
            connector.executeStatement( query.toString() );
            connector.commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished writing tpch_lineitem rows. Elapsed time: {} ms", (stop - start) );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database 'tpch_lineitem'. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating table tpch_lineitems" );
    }


    // populate table "tpch_nation"
    public void writeNations() {
        try {
            logger.trace( "Writing tpch_nations ..." );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            query.append( "INSERT INTO tpch_nation VALUES" );
            for ( Nation entity : new NationGenerator() ) {
                query.append( "(" );
                query.append( entity.getNationKey() );
                query.append( "," );
                query.append( "\'" + entity.getName() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getRegionKey() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getComment() + "\'" ); // String
                query.append( ")" );
                query.append( "," );
            }
            query.deleteCharAt( query.length() - 1 ); // ugly remove of last ','
            query.append( ";" );
            connector.executeStatement( query.toString() );
            connector.commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished writing tpch_nations. Elapsed time: {} ms", (stop - start) );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database 'tpch_nation'. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating table tpch_nations" );
    }


    // populate table "tpch_region"
    public void writeRegions() {
        try {
            logger.trace( "Writing tpch_regions ..." );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            query.append( "INSERT INTO tpch_region VALUES" );
            for ( Region entity : new RegionGenerator() ) {
                query.append( "(" );
                query.append( entity.getRegionKey() );
                query.append( "," );
                query.append( "\'" + entity.getName() + "\'" ); // String
                query.append( "," );
                query.append( "\'" + entity.getComment() + "\'" ); // String
                query.append( ")" );
                query.append( "," );
            }
            query.deleteCharAt( query.length() - 1 ); // ugly remove of last ','
            query.append( ";" );
            connector.executeStatement( query.toString() );
            connector.commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished writing tpch_regions. Elapsed time: {} ms", (stop - start) );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database 'tpch_region'. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating table tpch_region" );
    }


    @Override
    public TPCHResultTuple genericQueryExecutor( int queryID ) {
        String query = new String( QGenPostgresql.createQuery( SCALE_FACTOR, queryID ) );
        //TODO Jar Execution needs modification
        return super.genericQueryExecutor( queryID, connector, query );
    }


    @Override
    public TPCHResultTuple genericRefreshFunctionExecutor( int refreshFunctionID ) {
        try {
            TPCHTransactionType transactionType = TPCHTransactionType.TPCHTRANSACTIONUNDEFINED;
            logger.trace( "Executing RF" + (refreshFunctionID - 22) + " transaction" );
            long start = System.currentTimeMillis();
            connector.startTransaction();
            StringBuilder query = new StringBuilder();
            int cnt = 1;
            switch ( refreshFunctionID ) {
                case 23:
                    transactionType = TPCHTransactionType.TPCHTRANSACTIONNEWSALES;
                    //insert into tpch_orders
                    query.append( "INSERT INTO tpch_orders VALUES" );
                    for ( Order entity : new OrderGenerator( SCALE_FACTOR / 1_000d, TPCHConfig.PART, TPCHConfig.PART_COUNT ) ) {
                        query.append( "(" );
                        query.append( entity.getOrderKey() + 8 ); // second 8 key values of a group of 32
                        query.append( "," );
                        query.append( entity.getCustomerKey() );
                        query.append( "," );
                        query.append( "\'" + entity.getOrderStatus() + "\'" );
                        query.append( "," );
                        query.append( entity.getTotalPrice() );
                        query.append( "," );
                        String orderDate = GenerateUtils.formatDate( entity.getOrderDate() );
                        query.append( "TO_DATE(\'" + orderDate + "\', \'YYYY-MM-DD\')" );
                        query.append( "," );
                        query.append( "\'" + entity.getOrderPriority() + "\'" );
                        query.append( "," );
                        query.append( "\'" + entity.getClerk() + "\'" );
                        query.append( "," );
                        query.append( entity.getShipPriority() );
                        query.append( "," );
                        query.append( "\'" + entity.getComment() + "\'" );
                        query.append( ")" );
                        if ( cnt % BATCH_SIZE == 0 && cnt != SCALE_FACTOR * 1_500_000 ) {
                            query.append( " ON CONFLICT (o_orderkey) DO UPDATE " +
                                    "SET o_custkey=excluded.o_custkey, " +
                                    "o_orderstatus=excluded.o_orderstatus, " +
                                    "o_totalprice=excluded.o_totalprice, " +
                                    "o_orderdate=excluded.o_orderdate, " +
                                    "o_orderpriority=excluded.o_orderpriority, " +
                                    "o_clerk=excluded.o_clerk, " +
                                    "o_shippriority=excluded.o_shippriority, " +
                                    "o_comment=excluded.o_comment" );
                            query.append( ";" );
                            connector.executeStatement( query.toString() );
                            connector.commitTransaction();
                            query = new StringBuilder();
                            query.append( "INSERT INTO tpch_orders VALUES" );
                        } else {
                            if ( cnt != SCALE_FACTOR * 1_500_000 ) {
                                query.append( "," );
                            }
                        }
                        cnt++;
                    }
                    query.deleteCharAt( query.length() - 1 ); // ugly remove of last ','
                    query.append( " ON CONFLICT (o_orderkey) DO UPDATE " +
                            "SET o_custkey=excluded.o_custkey, " +
                            "o_orderstatus=excluded.o_orderstatus, " +
                            "o_totalprice=excluded.o_totalprice, " +
                            "o_orderdate=excluded.o_orderdate, " +
                            "o_orderpriority=excluded.o_orderpriority, " +
                            "o_clerk=excluded.o_clerk, " +
                            "o_shippriority=excluded.o_shippriority, " +
                            "o_comment=excluded.o_comment" );
                    query.append( ";" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    query = new StringBuilder();
                    cnt = 1;
                    query.append( "INSERT INTO tpch_lineitem VALUES" );
                    for ( LineItem entity : new LineItemGenerator( SCALE_FACTOR / 1_143,
                            TPCHConfig.PART, TPCHConfig.PART_COUNT ) ) {
                        query.append( "(" );
                        query.append( entity.getOrderKey() + 8 ); // second 8 key values of group of 32
                        query.append( "," );
                        query.append( "1" );
                        query.append( "," );
                        query.append( "2" );
                        query.append( "," );
                        query.append( entity.getLineNumber() );
                        query.append( "," );
                        query.append( entity.getQuantity() );
                        query.append( "," );
                        query.append( entity.getExtendedPrice() );
                        query.append( "," );
                        query.append( entity.getDiscount() );
                        query.append( "," );
                        query.append( entity.getTax() );
                        query.append( "," );
                        query.append( "\'" + entity.getReturnFlag() + "\'" );
                        query.append( "," );
                        query.append( "\'" + entity.getStatus() + "\'" );
                        query.append( "," );
                        String shipDate = GenerateUtils.formatDate( entity.getShipDate() );
                        query.append( "TO_DATE(\'" + shipDate + "\', \'YYYY-MM-DD\')" );
                        query.append( "," );
                        String commitDate = GenerateUtils.formatDate( entity.getCommitDate() );
                        query.append( "TO_DATE(\'" + commitDate + "\', \'YYYY-MM-DD\')" );
                        query.append( "," );
                        String receiptDate = GenerateUtils.formatDate( entity.getReceiptDate() );
                        query.append( "TO_DATE(\'" + receiptDate + "\', \'YYYY-MM-DD\')" );
                        query.append( "," );
                        query.append( "\'" + entity.getShipInstructions() + "\'" );
                        query.append( "," );
                        query.append( "\'" + entity.getShipMode() + "\'" );
                        query.append( "," );
                        query.append( "\'" + entity.getComment() + "\'" );
                        query.append( ")" );
                        if ( cnt % BATCH_SIZE == 0 && cnt != SCALE_FACTOR * 6_001_215 ) {
                            query.append( " ON CONFLICT (l_orderkey, l_linenumber) DO UPDATE " +
                                    "SET l_partkey=excluded.l_partkey, " +
                                    "l_suppkey=excluded.l_suppkey, " +
                                    "l_quantity=excluded.l_quantity, " +
                                    "l_extendedprice=excluded.l_extendedprice, " +
                                    "l_discount=excluded.l_discount, " +
                                    "l_tax=excluded.l_tax, " +
                                    "l_returnflag=excluded.l_returnflag, " +
                                    "l_linestatus=excluded.l_linestatus, " +
                                    "l_shipdate=excluded.l_shipdate, " +
                                    "l_commitdate=excluded.l_commitdate, " +
                                    "l_receiptdate=excluded.l_receiptdate, " +
                                    "l_shipinstruct=excluded.l_shipinstruct, " +
                                    "l_shipmode=excluded.l_shipmode, " +
                                    "l_comment=excluded.l_comment" );
                            query.append( ";" );
                            connector.executeStatement( query.toString() );
                            connector.commitTransaction();
                            query = new StringBuilder();
                            query.append( "INSERT INTO tpch_lineitem VALUES" );
                        } else {
                            if ( cnt != SCALE_FACTOR * 6_001_215 ) {
                                query.append( "," );
                            }
                        }
                        cnt++;
                    }
                    query.deleteCharAt( query.length() - 1 ); // ugly remove of last ','
                    query.append( " ON CONFLICT (l_orderkey, l_linenumber) DO UPDATE " +
                            "SET l_partkey=excluded.l_partkey, " +
                            "l_suppkey=excluded.l_suppkey, " +
                            "l_quantity=excluded.l_quantity, " +
                            "l_extendedprice=excluded.l_extendedprice, " +
                            "l_discount=excluded.l_discount, " +
                            "l_tax=excluded.l_tax, " +
                            "l_returnflag=excluded.l_returnflag, " +
                            "l_linestatus=excluded.l_linestatus, " +
                            "l_shipdate=excluded.l_shipdate, " +
                            "l_commitdate=excluded.l_commitdate, " +
                            "l_receiptdate=excluded.l_receiptdate, " +
                            "l_shipinstruct=excluded.l_shipinstruct, " +
                            "l_shipmode=excluded.l_shipmode, " +
                            "l_comment=excluded.l_comment" );
                    query.append( ";" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    break;
                case 24:
                    transactionType = TPCHTransactionType.TPCHTRANSACTIONOLDSALES;
                    // Deleting rows in the sparsely populated second 8 key values of tpch_orders (deleting lineitems as well)
                    query.append( "DELETE FROM tpch_orders WHERE o_orderkey IN (" );
                    for ( int i = 0; i < SCALE_FACTOR * 1500; i++ ) {
                        for ( int j = 8; j < 16; j++ ) {
                            int index = i * 32 + j;
                            // no entry with ID 8
                            if ( index != 8 ) {
                                query.append( index + ", " );
                            }
                        }
                    }
                    query.delete( query.length() - 2, query.length() );
                    query.append( ");" );
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
            }
            long stop = System.currentTimeMillis();
            logger.trace( "Finished RF{}. Elapsed time: {} ms", (refreshFunctionID - 22), (stop - start) );
            return ProtoObjectFactory.TPCHResultTuple( start, (stop - start), transactionType, refreshFunctionID, false, query.toString() );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while executing RF{}. Ignoring.", (refreshFunctionID - 22) );
            logger.error( e );
            try {
                connector.abortTransaction();
            } catch ( ConnectionException e1 ) {
                //Ignore
            }
            return TPCHResultTuple.getDefaultInstance();
        }
    }
}
