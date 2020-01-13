package org.polypheny.client.db.tpch;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.config.TPCHConfig;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.access.RESTConnector;
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
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHTransactionType;
import org.polypheny.client.job.PolyphenyJobCdl;
import org.polypheny.client.rpc.ProtoObjectFactory;


/**
 * @author silvan on 25.07.17.
 */
public class IcarusTpchBenchmarker extends TPCHBenchmarker {

    private final static Logger logger = LogManager.getLogger();
    private static final int INSERT_BATCH_SIZE = 1_000;
    private static final int DELETE_BATCH_SIZE = 200;
    private final double SCALE_FACTOR;
    private DBConnector connector;


    public IcarusTpchBenchmarker( PolyphenyJobCdl cdl ) {
        this( cdl.getEvaluation().getOptions().getTpchScalefactor(), cdl.getEvaluation().getDbms().getHost(), cdl.getEvaluation().getDbms().getPort() );
    }


    public IcarusTpchBenchmarker( LaunchWorkerMessage workerMessage ) {
        this( workerMessage.getTpchWorkerMessage().getSCALEFACTOR(), workerMessage.getDbInfo().getDbHost(), workerMessage.getDbInfo().getDbPort() );
    }


    public IcarusTpchBenchmarker( double SCALE_FACTOR, String host, int port ) {
        super( SCALE_FACTOR );
        this.SCALE_FACTOR = SCALE_FACTOR;
        this.connector = new RESTConnector( host, port );
    }


    @Override
    public void populateDatabase() {
        try {
            logger.trace( "Truncating tables" );
            connector.executeStatement( "TRUNCATE TABLE tpch_part" );
            connector.executeStatement( "TRUNCATE TABLE tpch_region" );
            connector.executeStatement( "TRUNCATE TABLE tpch_nation" );
            connector.executeStatement( "TRUNCATE TABLE tpch_supplier" );
            connector.executeStatement( "TRUNCATE TABLE tpch_partsupp" );
            connector.executeStatement( "TRUNCATE TABLE tpch_customer" );
            connector.executeStatement( "TRUNCATE TABLE tpch_orders" );
            connector.executeStatement( "TRUNCATE TABLE tpch_lineitem" );
            logger.debug( "Tables truncated" );
        } catch ( ConnectionException e ) {
            throw new RuntimeException( e );
        }
        super.populateDatabase();
    }


    @Override
    public void createTables() {
        logger.warn( "Icarus does not support table creation" );
    }


    @Override
    public void abort() {
        try {
            connector.abortTransaction();
        } catch ( ConnectionException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    protected void writeParts( double scaleFactor, int part, int partCount ) throws ConnectionException {
        logger.debug( "Writing parts" );
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        long start = System.currentTimeMillis();

        for ( Part entity : new PartGenerator( scaleFactor, part, partCount ) ) {
            query.append( "INSERT INTO tpch_part (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) VALUES" );
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
            query.append( entity.getRetailPrice() );
            query.append( "," );
            query.append( "\'" + entity.getComment() + "\'" ); // String
            query.append( ");" );
            if ( cnt % INSERT_BATCH_SIZE == 0 && cnt != scaleFactor * 200_000 ) {
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }

        logger.trace( query );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.debug( "Finished writing parts. Elapsed time: {} ms", (stop - start) );
    }


    @Override
    protected void writeSuppliers( double scaleFactor, int part, int partCount ) throws ConnectionException {
        logger.debug( "Writing suppliers" );
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        long start = System.currentTimeMillis();
        int cnt = 1;
        for ( Supplier entity : new SupplierGenerator( scaleFactor, part, partCount ) ) {
            query.append( "INSERT INTO tpch_supplier (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) VALUES" );
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
            query.append( ");" );
            if ( cnt % INSERT_BATCH_SIZE == 0 && cnt != scaleFactor * 10_000 ) {
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }

        logger.trace( query );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.debug( "Finished writing suppliers. Elapsed time: {} ms", (stop - start) );
    }


    @Override
    protected void writePartSuppliers( double scaleFactor, int part, int partCount ) throws ConnectionException {
        logger.debug( "Writing part-supplier" );
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        long start = System.currentTimeMillis();
        int cnt = 1;
        for ( PartSupplier entity : new PartSupplierGenerator( scaleFactor, part, partCount ) ) {
            query.append( "INSERT INTO tpch_partsupp (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) VALUES" );
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
            query.append( ");" );
            if ( cnt % INSERT_BATCH_SIZE == 0 && cnt != scaleFactor * 800_000 ) {
                if ( cnt % 100_000 == 0 ) {
                    logger.debug( "Inserted {} items", cnt );
                }
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }

        logger.trace( query );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.debug( "Finished writing part-suppliers. Elapsed time: {} ms", (stop - start) );
    }


    @Override
    protected void writeCustomers( double scaleFactor, int part, int partCount ) throws ConnectionException {
        logger.debug( "Writing customers" );
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        long start = System.currentTimeMillis();
        int cnt = 1;
        for ( Customer entity : new CustomerGenerator( scaleFactor, part, partCount ) ) {
            query.append( "INSERT INTO tpch_customer (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) VALUES" );
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
            query.append( ");" );

            if ( cnt % INSERT_BATCH_SIZE == 0 && cnt != scaleFactor * 150_000 ) {
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }

        logger.trace( query );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.debug( "Finished writing customers. Elapsed time: {} ms", (stop - start) );
    }


    @Override
    protected void writeOrders( double scaleFactor, int part, int partCount ) throws ConnectionException {
        logger.debug( "Writing orders" );
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        long start = System.currentTimeMillis();
        int cnt = 1;
        for ( Order entity : new OrderGenerator( scaleFactor, part, partCount ) ) {
            query.append( "INSERT INTO tpch_orders (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) VALUES" );
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
            query.append( "\'" + orderDate + "\'" );
            query.append( "," );
            query.append( "\'" + entity.getOrderPriority() + "\'" ); // String
            query.append( "," );
            query.append( "\'" + entity.getClerk() + "\'" ); // String
            query.append( "," );
            query.append( entity.getShipPriority() );
            query.append( "," );
            query.append( "\'" + entity.getComment() + "\'" ); // String
            query.append( ");" );

            if ( cnt % INSERT_BATCH_SIZE == 0 && cnt != scaleFactor * 1_500_000 ) {
                if ( cnt % 100_000 == 0 ) {
                    logger.debug( "Inserted {} items", cnt );
                }
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }

        logger.trace( query );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.debug( "Finished writing orders. Elapsed time: {} ms", (stop - start) );
    }


    @Override
    protected void writeLineItems( double scaleFactor, int part, int partCount ) throws ConnectionException {
        logger.debug( "Writing line-items" );
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        long start = System.currentTimeMillis();
        int cnt = 1;

        for ( LineItem entity : new LineItemGenerator( scaleFactor, part, partCount ) ) {
            query.append( "INSERT INTO tpch_lineitem (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) VALUES" );
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
            query.append( "\'" + shipDate + "\'" );
            query.append( "," );
            String commitDate = GenerateUtils.formatDate( entity.getCommitDate() ); // Date
            query.append( "\'" + commitDate + "\'" );
            query.append( "," );
            String receiptDate = GenerateUtils.formatDate( entity.getReceiptDate() );
            query.append( "\'" + receiptDate + "\'" ); // Date
            query.append( "," );
            query.append( "\'" + entity.getShipInstructions() + "\'" ); // String
            query.append( "," );
            query.append( "\'" + entity.getShipMode() + "\'" ); // String
            query.append( "," );
            query.append( "\'" + entity.getComment() + "\'" ); // String
            query.append( ");" );
            if ( cnt % INSERT_BATCH_SIZE == 0 && cnt != scaleFactor * 6_001_215 ) {
                if ( cnt % 100_000 == 0 ) {
                    logger.debug( "Inserted {} items", cnt );
                }
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }

        logger.trace( query );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.debug( "Finished writing line-items. Elapsed time: {} ms", (stop - start) );
    }


    @Override
    protected void writeNations() throws ConnectionException {
        logger.debug( "Writing nations" );
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        long start = System.currentTimeMillis();

        for ( Nation entity : new NationGenerator() ) {
            query.append( "INSERT INTO tpch_nation (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) VALUES" );
            query.append( "(" );
            query.append( entity.getNationKey() );
            query.append( "," );
            query.append( "\'" + entity.getName() + "\'" ); // String
            query.append( "," );
            query.append( "\'" + entity.getRegionKey() + "\'" ); // String
            query.append( "," );
            query.append( "\'" + entity.getComment() + "\'" ); // String
            query.append( ");" );
        }

        logger.trace( query );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.debug( "Finished writing nations. Elapsed time: {} ms", (stop - start) );
    }


    @Override
    protected void writeRegions() throws ConnectionException {
        logger.trace( "Writing regions" );
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        long start = System.currentTimeMillis();

        for ( Region entity : new RegionGenerator() ) {
            query.append( "INSERT INTO tpch_region (R_REGIONKEY, R_NAME, R_COMMENT) VALUES" );
            query.append( "(" );
            query.append( entity.getRegionKey() );
            query.append( "," );
            query.append( "\'" + entity.getName() + "\'" ); // String
            query.append( "," );
            query.append( "\'" + entity.getComment() + "\'" ); // String
            query.append( ");" );
        }

        logger.trace( query );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.debug( "Finished writing regions. Elapsed time: {} ms", (stop - start) );
    }


    @Override
    public TPCHResultTuple genericQueryExecutor( int queryID ) {
        logger.trace( "Executing Query {}", queryID );
        String query = new String( QGenIcarus.createQuery( SCALE_FACTOR, queryID ) );
        //TODO Jar Execution needs modification
        return super.genericQueryExecutor( queryID, connector, query );
    }


    @Override
    public TPCHResultTuple genericRefreshFunctionExecutor( int refreshFunctionID ) {
        TPCHTransactionType transactionType = TPCHTransactionType.TPCHTRANSACTIONUNDEFINED;
        logger.debug( "Executing RF" + (refreshFunctionID - 22) + " transaction" );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        switch ( refreshFunctionID ) {
            case 23:
                //We don't log the query-text here since the TPCH-Interface does not mandate returning query-text for the write interface and manually doing it again here would lead to MASSIVE Code reuse
                transactionType = TPCHTransactionType.TPCHTRANSACTIONNEWSALES;
                //insert into tpch_orders
                try {
                    writeOrders( SCALE_FACTOR / 1_000d, TPCHConfig.PART, TPCHConfig.PART_COUNT );
                    writeLineItems( SCALE_FACTOR / 1_143, TPCHConfig.PART, TPCHConfig.PART_COUNT );
                } catch ( ConnectionException e ) {
                    //We expect a ConnectionException here if there are duplicate inserts, so we just log it
                    logger.trace( e );
                }
                long stop = System.currentTimeMillis();
                return ProtoObjectFactory.TPCHResultTuple( start, (stop - start), transactionType, refreshFunctionID, false, query.toString() );
            case 24:
                try {
                    transactionType = TPCHTransactionType.TPCHTRANSACTIONOLDSALES;
                    // Deleting rows in the sparsely populated second 8 key values of tpch_orders (deleting lineitems as well)
                    int counter = 0;
                    query.append( "DELETE FROM tpch_orders WHERE o_orderkey=" );
                    final String s = " OR o_orderkey=";
                    for ( int i = 0; i < SCALE_FACTOR * 1500; i++ ) {
                        for ( int j = 8; j < 16; j++ ) {
                            int index = i * 32 + j;
                            // no entry with ID 8
                            if ( index != 8 ) {
                                query.append( index + s );
                                counter++;
                                if ( counter % DELETE_BATCH_SIZE == 0 ) {
                                    if ( counter % (DELETE_BATCH_SIZE * 3) == 0 ) {
                                        logger.debug( counter );
                                    }
                                    query.delete( query.length() - s.length(), query.length() );
                                    connector.executeStatement( query.toString() );
                                    connector.commitTransaction();
                                    query = new StringBuilder();
                                    query.append( "DELETE FROM tpch_orders WHERE o_orderkey=" );
                                }
                            }
                        }
                    }
                    if ( query.toString().endsWith( s ) ) {
                        query.delete( query.length() - s.length(), query.length() );
                    }
                    connector.executeStatement( query.toString() );
                    connector.commitTransaction();
                    stop = System.currentTimeMillis();
                    logger.trace( "Finished RF{}. Elapsed time: {} ms", (refreshFunctionID - 22), (stop - start) );
                    return ProtoObjectFactory.TPCHResultTuple( start, (stop - start), transactionType, refreshFunctionID, false, query.toString() );
                } catch ( ConnectionException e ) {
                    logger.error( "ConnectionException while executing RF{}. Exiting.", (refreshFunctionID - 22) );
                    abort();
                    throw new RuntimeException( e );
                }
            default:
                logger.error( refreshFunctionID );
        }
        throw new IllegalArgumentException( "RefreshFunction " + refreshFunctionID + " is not supported" );
    }
}
