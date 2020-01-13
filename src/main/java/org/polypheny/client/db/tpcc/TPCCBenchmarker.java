package org.polypheny.client.db.tpcc;


import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.chronos.ProgressListener;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.tpcc.transactions.DeliveryTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.NewOrderTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.OrderStatusTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.PaymentTransactionExecutor;
import org.polypheny.client.db.tpcc.transactions.StockLevelTransactionExecutor;
import org.polypheny.client.generator.tpcc.TPCCPopulationGenerator;
import org.polypheny.client.generator.tpcc.objects.Customer;
import org.polypheny.client.generator.tpcc.objects.District;
import org.polypheny.client.generator.tpcc.objects.History;
import org.polypheny.client.generator.tpcc.objects.Item;
import org.polypheny.client.generator.tpcc.objects.NewOrder;
import org.polypheny.client.generator.tpcc.objects.Order;
import org.polypheny.client.generator.tpcc.objects.OrderLine;
import org.polypheny.client.generator.tpcc.objects.Stock;
import org.polypheny.client.generator.tpcc.objects.Warehouse;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;


/**
 * This is the interface that any system to be benchmarked should implement. Insert-methods return strings which should contains the insert-query. Since the data to insert is also given in a blocked fashion, we return the queries also as a whole. This is ok and not too memory-intensive since
 * generation is done in a iterative fashion and not all rows at once.
 *
 * Methods are implemented in a best-effort fashion to adhere to the SQL-standard. If your system does not support certain queries, you can override the given method.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public abstract class TPCCBenchmarker {

    private final int NUMBER_OF_CONFIGURED_WAREHOUSES;
    private Logger logger = LogManager.getLogger();


    /**
     * @param NUMBER_OF_CONFIGURED_WAREHOUSES How many warehouses the company should have. Influences the initial population and transactions.
     */
    public TPCCBenchmarker( final int NUMBER_OF_CONFIGURED_WAREHOUSES ) {
        this.NUMBER_OF_CONFIGURED_WAREHOUSES = NUMBER_OF_CONFIGURED_WAREHOUSES;
    }


    /**
     * Since {@link TPCCBenchmarker#getNewOrderExecutor()} is abstract, we expect each implementation of the {@link TPCCBenchmarker} to have their own implementation of the necessary methods.
     *
     * @param C_OL_I_ID Runtime constant chosen by the master.
     * @param queryID will not be modified and used to generate the {@link TPCCResultTuple}
     */
    public TPCCResultTuple newOrderTransaction( final int W_ID, final int queryID, final int C_OL_I_ID ) {
        try {
            return getNewOrderExecutor().newOrderTransaction( W_ID, queryID, C_OL_I_ID );
        } catch ( Exception e ) {
            abort();
            throw e;
        }
    }


    /**
     * For Parameter documentation, {@link PaymentTransactionExecutor#paymentTransaction(int, int, int, int, int)}
     */
    public TPCCResultTuple paymentTransaction( int queryID, int W_ID, int C_C_LAST, int C_C_ID ) {
        try {
            return getPaymentExecutor().paymentTransaction( queryID, W_ID, NUMBER_OF_CONFIGURED_WAREHOUSES, C_C_LAST, C_C_ID );
        } catch ( Exception e ) {
            abort();
            throw e;
        }
    }


    /**
     * {@link OrderStatusTransactionExecutor#orderStatusTransaction(int, int, int, int)}
     */
    public TPCCResultTuple orderStatusTransaction( final int queryID, final int C_C_LAST, final int C_C_ID, final int W_ID ) {
        try {
            return getOrderStatusExecutor().orderStatusTransaction( queryID, C_C_LAST, C_C_ID, W_ID );
        } catch ( Exception e ) {
            abort();
            throw e;
        }
    }


    /**
     * {@link StockLevelTransactionExecutor#stockLevelTransaction(int, int, int)}
     */
    public TPCCResultTuple stockLevelTransaction( final int queryID, final int W_ID, final int D_ID ) {
        try {
            return getStockLevelTransactionExecutor().stockLevelTransaction( W_ID, queryID, D_ID );
        } catch ( Exception e ) {
            abort();
            throw e;
        }
    }


    /**
     * {@link DeliveryTransactionExecutor#deliveryTransaction(int, int)}
     */
    public TPCCResultTuple deliveryTransaction( int queryID, int W_ID ) {
        try {
            return getDeliveryTransactionExecutor().deliveryTransaction( queryID, W_ID );
        } catch ( Exception e ) {
            abort();
            throw e;
        }
    }


    public abstract OrderStatusTransactionExecutor getOrderStatusExecutor();

    public abstract NewOrderTransactionExecutor getNewOrderExecutor();

    public abstract PaymentTransactionExecutor getPaymentExecutor();

    public abstract StockLevelTransactionExecutor getStockLevelTransactionExecutor();

    public abstract DeliveryTransactionExecutor getDeliveryTransactionExecutor();

    /**
     * Create the DB Schema according to section 1.2.
     */
    public abstract void createTables( ProgressListener progressListener );


    /**
     * Populates a Database according to Section 4. This method contains the core logic and hands off insertion to methods which can be overriden.
     */
    public void populateDatabase( ProgressListener progressListener ) {
        try {
            logger.info( "populating database" );
            Item[] items = TPCCPopulationGenerator.generateItems();
            logger.info( "Generated Items" );
            writeItems( items );
            logger.info( "Wrote Items" );
            Warehouse[] warehouses = TPCCPopulationGenerator.generateWarehouses( NUMBER_OF_CONFIGURED_WAREHOUSES );
            writeWarehouses( warehouses );
            //For each row in the WAREHOUSE table
            for ( Warehouse warehouse : warehouses ) {
                logger.info( "Writing data for warehouse {}", warehouse.getW_ID() );
                Stock[] stock = TPCCPopulationGenerator.generateStockForWarehouse( warehouse );
                writeStock( stock );
                progressListener.reportPopulationProgress( Optional.empty(), Optional.of( "Inserted Stock for warehouse " + warehouse.getW_ID() ) );
                District[] districts = TPCCPopulationGenerator.generateDistrictsForWarehouse( warehouse );
                writeDistricts( districts );
                progressListener.reportPopulationProgress( Optional.empty(), Optional.of( "Inserted Districts for warehouse " + warehouse.getW_ID() ) );
                //For each row in the DISTRICT table:
                for ( District district : districts ) {
                    logger.info( "Writing data for district {}", district.getD_ID() );
                    Timestamp T_SINCE = Timestamp.from( Instant.now() );
                    Customer[] customers = TPCCPopulationGenerator.generateCustomersForDistrict( district, T_SINCE );
                    writeCustomers( customers );
                    //For each row in the CUSTOMER table
                    List<History> histories = new ArrayList<>();
                    for ( Customer customer : customers ) {
                        histories.addAll( Arrays.asList( TPCCPopulationGenerator.generateHistoryForCustomer( customer ) ) );
                        if ( histories.size() > getBatchSize() ) {
                            writeHistory( histories );
                            histories.clear();
                        }
                    }
                    writeHistory( histories );
                    //Continue with DISTRICT-things
                    Order[] orders = TPCCPopulationGenerator.generateOrdersForDistrict( district );
                    writeOrders( orders );
                    //For each row in the ORDER table
                    List<OrderLine> orderLines = new ArrayList<>();
                    for ( Order order : orders ) {
                        orderLines.addAll( Arrays.asList( TPCCPopulationGenerator.generateOrderLineForOrder( order ) ) );
                        if ( orderLines.size() > getBatchSize() ) {
                            writeOrderLines( orderLines );
                            orderLines.clear();
                        }
                    }
                    writeOrderLines( orderLines );
                    //Continue with DISTRICT-things
                    NewOrder[] newOrders = TPCCPopulationGenerator.generateNewOrdersForDistrict( district );
                    writeNewOrders( newOrders );
                    progressListener.reportPopulationProgress( Optional.of( ((double) warehouse.getW_ID() / (double) warehouses.length) + ((double) district.getD_ID() / (double) districts.length) ), Optional.of( "Wrote data for district " + district.getD_ID() ) );
                }
                progressListener.reportPopulationProgress( Optional.of( (double) warehouse.getW_ID() / (double) warehouses.length ), Optional.of( "Wrote data for warehouse " + warehouse.getW_ID() ) );
            }
            getConnector().commitTransaction();
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating database" );
    }


    void truncate( DBConnector connector ) throws ConnectionException {
        logger.trace( "Truncating tables" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_warehouse" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_district" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_customer" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_history" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_order" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_new_order" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_item" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_stock" );
        connector.executeStatement( "TRUNCATE TABLE tpcc_order_line" );
        logger.debug( "Tables truncated" );
    }


    /**
     * BATCH_SIZE for insertion
     */
    protected abstract int getBatchSize();


    /**
     * Aborts the currently running benchmark, closing all associated resources. Must not throw exceptions.
     */
    public void abort() {
        logger.trace( "Aborting TPC-C Benchmarker" );
        try {
            getConnector().abortTransaction();
        } catch ( ConnectionException e ) {
            logger.error( e );
            //Ignore
        }
        getConnector().close();
    }


    /**
     * Underlying connector, this is used for the default-implementations of queries..
     */
    public abstract DBConnector getConnector();


    /**
     * Writes multiple {@link NewOrder} rows to the database.
     */
    public String writeNewOrders( NewOrder[] newOrders ) throws ConnectionException {
        if ( newOrders.length == 0 ) {
            logger.info( "was asked to insert 0 new orders" );
            return "";
        }
        logger.trace( "Writing {} new order rows", newOrders.length );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO tpcc_new_order VALUES" );

        for ( NewOrder newOrder : newOrders ) {
            query.append( "(" );
            query.append( newOrder.getNO_O_ID() );
            query.append( "," );
            query.append( newOrder.getNO_D_ID() );
            query.append( "," );
            query.append( newOrder.getNO_W_ID() );
            query.append( ")" );

            if ( cnt % getBatchSize() == 0 && cnt != newOrders.length ) {
                query.append( ";" );
                result.append( query.toString() );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                query = new StringBuilder();
                query.append( "INSERT INTO tpcc_new_order VALUES" );
            } else {
                if ( cnt != newOrders.length ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        result.append( query.toString() );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing new order rows. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    /**
     * Writes multiple {@link OrderLine} rows to the database
     */
    public String writeOrderLines( List<OrderLine> orderLines ) throws ConnectionException {
        if ( orderLines.size() == 0 ) {
            logger.info( "was asked to insert 0 order lines" );
            return "";
        }
        logger.trace( "Writing {} order line rows", orderLines.size() );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO tpcc_order_line VALUES" );

        for ( OrderLine orderLine : orderLines ) {
            query.append( "(" );
            query.append( orderLine.getOL_O_ID() );
            query.append( "," );
            query.append( orderLine.getOL_D_ID() );
            query.append( "," );
            query.append( orderLine.getOL_W_ID() );
            query.append( "," );
            query.append( orderLine.getOL_NUMBER() );
            query.append( "," );
            query.append( orderLine.getOL_I_ID() );
            query.append( "," );
            query.append( orderLine.getOL_SUPPLY_W_ID() );
            //Postgresql doesn't like 'null' as a format for timestamps
            if ( orderLine.getOL_DELIVERY_D() == null ) {
                query.append( "," );
                query.append( orderLine.getOL_DELIVERY_D() );
                query.append( "," );
            } else {
                query.append( ",'" );
                query.append( orderLine.getOL_DELIVERY_D() );
                query.append( "'," );
            }
            query.append( orderLine.getOL_QUANTITY() );
            query.append( "," );
            query.append( orderLine.getOL_AMOUNT() );
            query.append( ",'" );
            query.append( orderLine.getOL_DIST_INFO() );
            query.append( "')" );

            if ( cnt % getBatchSize() == 0 && cnt != orderLines.size() ) {
                query.append( ";" );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                result.append( query.toString() );
                query = new StringBuilder();
                query.append( "INSERT INTO tpcc_order_line VALUES" );
            } else {
                if ( cnt != orderLines.size() ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        result.append( query.toString() );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing order line rows. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    /**
     * Writes multiple {@link Order} rows to the database.
     */
    public String writeOrders( Order[] orders ) throws ConnectionException {
        if ( orders.length == 0 ) {
            logger.info( "was asked to insert 0 orders" );
            return "";
        }
        logger.trace( "Writing {} order rows", orders.length );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO \"tpcc_order\" VALUES" );

        for ( Order order : orders ) {
            query.append( "(" );
            query.append( order.getO_ID() );
            query.append( "," );
            query.append( order.getO_D_ID() );
            query.append( "," );
            query.append( order.getO_W_ID() );
            query.append( "," );
            query.append( order.getO_C_ID() );
            query.append( ",'" );
            query.append( order.getO_ENTRY_D() );
            query.append( "'," );
            query.append( order.getO_CARRIER_ID() );
            query.append( "," );
            query.append( order.getO_OL_CNT() );
            query.append( "," );
            query.append( order.getO_ALL_LOCAL() );
            query.append( ")" );

            if ( cnt % getBatchSize() == 0 && cnt != orders.length ) {
                query.append( ";" );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                result.append( query.toString() );
                query = new StringBuilder();
                query.append( "INSERT INTO \"tpcc_order\" VALUES" );
            } else {
                if ( cnt != orders.length ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        result.append( query.toString() );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing orders. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    /**
     * Writes multiple {@link History} rows to the Database.
     */
    public String writeHistory( List<History> histories ) throws ConnectionException {
        if ( histories.size() == 0 ) {
            logger.info( "was asked to insert 0 histories" );
            return "";
        }
        logger.trace( "Writing {} history rows", histories.size() );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO tpcc_history VALUES" );

        for ( History history : histories ) {
            query.append( "(" );
            query.append( history.getH_C_ID() );
            query.append( "," );
            query.append( history.getH_C_D_ID() );
            query.append( "," );
            query.append( history.getH_C_W_ID() );
            query.append( "," );
            query.append( history.getH_D_ID() );
            query.append( "," );
            query.append( history.getH_W_ID() );
            query.append( ",'" );
            query.append( history.getH_DATE() );
            query.append( "'," );
            query.append( history.getH_AMOUNT() );
            query.append( ",'" );
            query.append( history.getH_DATA() );
            query.append( "')" );

            if ( cnt % getBatchSize() == 0 && cnt != histories.size() ) {
                query.append( ";" );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                result.append( query.toString() );
                query = new StringBuilder();
                query.append( "INSERT INTO tpcc_history VALUES" );
            } else {
                if ( cnt != histories.size() ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        result.append( query.toString() );
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing histories. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    /**
     * Writes multiple {@link Customer}s to the Database.
     */
    protected String writeCustomers( Customer[] customers ) throws ConnectionException {
        if ( customers.length == 0 ) {
            logger.info( "was asked to insert 0 customers" );
            return "";
        }
        logger.trace( "Writing {} customers", customers.length );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO tpcc_customer VALUES" );

        for ( Customer customer : customers ) {
            query.append( "(" );
            query.append( customer.getC_ID() );
            query.append( "," );
            query.append( customer.getC_D_ID() );
            query.append( "," );
            query.append( customer.getC_W_ID() );
            query.append( ",'" );
            query.append( customer.getC_FIRST() );
            query.append( "','" );
            query.append( customer.getC_MIDDLE() );
            query.append( "','" );
            query.append( customer.getC_LAST() );
            query.append( "','" );
            query.append( customer.getC_STREET_1() );
            query.append( "','" );
            query.append( customer.getC_STREET_2() );
            query.append( "','" );
            query.append( customer.getC_CITY() );
            query.append( "','" );
            query.append( customer.getC_STATE() );
            query.append( "'," );
            query.append( customer.getC_ZIP() );
            query.append( "," );
            query.append( customer.getC_PHONE() );
            query.append( ",'" );
            query.append( customer.getC_SINCE() );
            query.append( "','" );
            query.append( customer.getC_CREDIT() );
            query.append( "'," );
            query.append( customer.getC_CREDIT_LIM() );
            query.append( "," );
            query.append( customer.getC_DISCOUNT() );
            query.append( "," );
            query.append( customer.getC_BALANCE() );
            query.append( "," );
            query.append( customer.getC_YTD_PAYMENT() );
            query.append( "," );
            query.append( customer.getC_PAYMENT_CNT() );
            query.append( "," );
            query.append( customer.getC_DELIVERY_CNT() );
            query.append( ",'" );
            query.append( customer.getC_DATA() );
            query.append( "')" );

            if ( cnt % getBatchSize() == 0 && cnt != customers.length ) {
                query.append( ";" );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                result.append( query.toString() );
                query = new StringBuilder();
                query.append( "INSERT INTO tpcc_customer VALUES" );
            } else {
                if ( cnt != customers.length ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        result.append( query.toString() );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing customers. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    /**
     * Writes multiple {@link District}s to the Database.
     */
    protected String writeDistricts( District[] districts ) throws ConnectionException {
        if ( districts.length == 0 ) {
            logger.info( "was asked to insert 0 districts" );
            return "";
        }
        logger.trace( "Writing {} districts", districts.length );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO tpcc_district VALUES" );
        for ( District district : districts ) {
            query.append( "(" );
            query.append( district.getD_ID() );
            query.append( "," );
            query.append( district.getD_W_ID() );
            query.append( ",'" );
            query.append( district.getD_NAME() );
            query.append( "','" );
            query.append( district.getD_STREET_1() );
            query.append( "','" );
            query.append( district.getD_STREET_2() );
            query.append( "','" );
            query.append( district.getD_CITY() );
            query.append( "','" );
            query.append( district.getD_STATE() );
            query.append( "'," );
            query.append( district.getD_ZIP() );
            query.append( "," );
            query.append( district.getD_TAX() );
            query.append( "," );
            query.append( district.getD_YTD() );
            query.append( "," );
            query.append( district.getD_NEXT_O_ID() );
            query.append( ")" );

            if ( cnt % getBatchSize() == 0 && cnt != districts.length ) {
                query.append( ";" );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                query = new StringBuilder();
                result.append( query.toString() );
                query.append( "INSERT INTO tpcc_district VALUES" );
            } else {
                if ( cnt != districts.length ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        result.append( query.toString() );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing districts. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    /**
     * Writes multiple {@link Stock}s to the Database.
     */
    protected String writeStock( Stock[] stocks ) throws ConnectionException {
        if ( stocks.length == 0 ) {
            logger.info( "was asked to insert 0 stocks" );
            return "";
        }
        logger.trace( "Writing {} stock rows", stocks.length );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO tpcc_stock VALUES" );
        for ( Stock stock : stocks ) {
            query.append( "(" );
            query.append( stock.getS_I_ID() );
            query.append( "," );
            query.append( stock.getS_W_ID() );
            query.append( "," );
            query.append( stock.getS_QUANTITY() );
            query.append( ",'" );
            query.append( stock.getS_DIST_01() );
            query.append( "','" );
            query.append( stock.getS_DIST_02() );
            query.append( "','" );
            query.append( stock.getS_DIST_03() );
            query.append( "','" );
            query.append( stock.getS_DIST_04() );
            query.append( "','" );
            query.append( stock.getS_DIST_05() );
            query.append( "','" );
            query.append( stock.getS_DIST_06() );
            query.append( "','" );
            query.append( stock.getS_DIST_07() );
            query.append( "','" );
            query.append( stock.getS_DIST_08() );
            query.append( "','" );
            query.append( stock.getS_DIST_09() );
            query.append( "','" );
            query.append( stock.getS_DIST_10() );
            query.append( "'," );
            query.append( stock.getS_YTD() );
            query.append( "," );
            query.append( stock.getS_ORDER_CNT() );
            query.append( "," );
            query.append( stock.getS_REMOTE_CNT() );
            query.append( ",'" );
            query.append( stock.getS_DATA() );
            query.append( "')" );

            if ( cnt % getBatchSize() == 0 && cnt != stocks.length ) {
                query.append( ";" );
                result.append( query.toString() );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                query = new StringBuilder();
                query.append( "INSERT INTO tpcc_stock VALUES" );
            } else {
                if ( cnt != stocks.length ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        result.append( query.toString() );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing stock. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    /**
     * Writes multiple {@link Warehouse}s to the Database.
     */
    protected String writeWarehouses( Warehouse[] warehouses ) throws ConnectionException {
        if ( warehouses.length == 0 ) {
            logger.info( "was asked to insert 0 warehouses" );
            return "";
        }
        logger.trace( "Writing {} warehouses", warehouses.length );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO tpcc_warehouse VALUES" );
        for ( Warehouse warehouse : warehouses ) {
            query.append( "(" );
            query.append( warehouse.getW_ID() );
            query.append( ",'" );
            query.append( warehouse.getW_NAME() );
            query.append( "','" );
            query.append( warehouse.getW_STREET_1() );
            query.append( "','" );
            query.append( warehouse.getW_STREET_2() );
            query.append( "','" );
            query.append( warehouse.getW_CITY() );
            query.append( "','" );
            query.append( warehouse.getW_STATE() );
            query.append( "','" );
            query.append( warehouse.getW_ZIP() );
            query.append( "'," );
            query.append( warehouse.getW_TAX() );
            query.append( "," );
            query.append( warehouse.getW_YTD() );
            query.append( ")" );

            if ( cnt % getBatchSize() == 0 && cnt != warehouses.length ) {
                query.append( ";" );
                result.append( query.toString() );
                logger.trace( "query: {}", query.toString() );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                query = new StringBuilder();
                query.append( "INSERT INTO tpcc_warehouse VALUES" );
            } else {
                if ( cnt != warehouses.length ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        result.append( query.toString() );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing warehouses. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    /**
     * Writes multiple {@link Item}s to the Database.
     */
    public String writeItems( Item[] items ) throws ConnectionException {
        if ( items.length == 0 ) {
            logger.info( "was asked to insert 0 items" );
            return "";
        }
        logger.trace( "Writing {} items", items.length );
        long start = System.currentTimeMillis();
        getConnector().startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        query.append( "INSERT INTO tpcc_item VALUES" );
        for ( Item item : items ) {
            query.append( "(" );
            query.append( item.getI_ID() );
            query.append( "," );
            query.append( item.getI_IM_ID() );
            query.append( ",'" );
            query.append( item.getI_NAME() );
            query.append( "'," );
            query.append( item.getI_PRICE() );
            query.append( ",'" );
            query.append( item.getI_DATA() );
            query.append( "')" );

            if ( cnt % getBatchSize() == 0 && cnt != items.length ) {
                query.append( ";" );
                result.append( query.toString() );
                getConnector().executeStatement( query.toString() );
                getConnector().commitTransaction();
                query = new StringBuilder();
                query.append( "INSERT INTO tpcc_ITEM VALUES" );
            } else {
                if ( cnt != items.length ) {
                    query.append( "," );
                }
            }
            cnt++;
        }
        query.append( ";" );
        result.append( query.toString() );
        getConnector().executeStatement( query.toString() );
        getConnector().commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing items. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }

}
