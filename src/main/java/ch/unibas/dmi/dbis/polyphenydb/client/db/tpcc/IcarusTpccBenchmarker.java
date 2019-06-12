package ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc;


import ch.unibas.dmi.dbis.polyphenydb.client.chronos.ProgressListener;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.ConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.DBConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.RESTConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.DeliveryTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.NewOrderTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.OrderStatusTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.PaymentTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.StockLevelTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.icarus.IcarusDeliveryTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.icarus.IcarusNewOrderTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.icarus.IcarusOrderStatusTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.icarus.IcarusPaymentTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.icarus.IcarusStockLevelTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Customer;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.District;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.History;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Item;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.NewOrder;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Order;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.OrderLine;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Stock;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Warehouse;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.job.PolyphenyJobCdl;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author silvan on 17.07.17.
 */
public class IcarusTpccBenchmarker extends TPCCBenchmarker {

    private final static Logger logger = LogManager.getLogger();
    private static final int BATCH_SIZE = 1_000;
    private DBConnector connector;
    private IcarusDeliveryTransactionExecutor deliveryTransactionExecutor;
    private IcarusNewOrderTransactionExecutor newOrderTransactionExecutor;
    private IcarusOrderStatusTransactionExecutor orderStatusTransactionExecutor;
    private IcarusPaymentTransactionExecutor paymentTransactionExecutor;
    private IcarusStockLevelTransactionExecutor stockLevelTransactionExecutor;


    public IcarusTpccBenchmarker( PolyphenyJobCdl cdl ) {
        this( cdl.getEvaluation().getOptions().getTpccWarehouses(), cdl.getEvaluation().getDbms().getHost(), cdl.getEvaluation().getDbms().getPort() );
    }


    private IcarusTpccBenchmarker( int NUMBER_OF_CONFIGURED_WAREHOUSES, String host, int port ) {
        super( NUMBER_OF_CONFIGURED_WAREHOUSES );
        this.connector = new RESTConnector( host, port );
        deliveryTransactionExecutor = new IcarusDeliveryTransactionExecutor( new RESTConnector( host, port ) );
        newOrderTransactionExecutor = new IcarusNewOrderTransactionExecutor( NUMBER_OF_CONFIGURED_WAREHOUSES, new RESTConnector( host, port ), this );
        orderStatusTransactionExecutor = new IcarusOrderStatusTransactionExecutor( new RESTConnector( host, port ) );
        paymentTransactionExecutor = new IcarusPaymentTransactionExecutor( new RESTConnector( host, port ), this );
        stockLevelTransactionExecutor = new IcarusStockLevelTransactionExecutor( new RESTConnector( host, port ) );
    }


    public IcarusTpccBenchmarker( LaunchWorkerMessage workerMessage ) {
        this( workerMessage.getTpccWorkerMessage().getNUMBEROFCONFIGUREDWAREHOUSES(), workerMessage.getDbInfo().getDbHost(), workerMessage.getDbInfo().getDbPort() );
    }


    @Override
    public void populateDatabase( ProgressListener progressListener ) {
        try {
            super.truncate( connector );
        } catch ( ConnectionException e ) {
            throw new RuntimeException( e );
        }
        super.populateDatabase( progressListener );
    }


    /**
     * BATCH_SIZE for insertion
     */
    @Override
    protected int getBatchSize() {
        return BATCH_SIZE;
    }


    @Override
    public OrderStatusTransactionExecutor getOrderStatusExecutor() {
        return orderStatusTransactionExecutor;
    }


    @Override
    public NewOrderTransactionExecutor getNewOrderExecutor() {
        return newOrderTransactionExecutor;
    }


    @Override
    public PaymentTransactionExecutor getPaymentExecutor() {
        return paymentTransactionExecutor;
    }


    @Override
    public StockLevelTransactionExecutor getStockLevelTransactionExecutor() {
        return stockLevelTransactionExecutor;
    }


    @Override
    public DeliveryTransactionExecutor getDeliveryTransactionExecutor() {
        return deliveryTransactionExecutor;
    }


    @Override
    public void createTables( ProgressListener progressListener ) {
        logger.error( "Icarus does not support table-creation" );
    }


    @Override
    public DBConnector getConnector() {
        return connector;
    }


    @Override
    public String writeNewOrders( NewOrder[] newOrders ) throws ConnectionException {
        if ( newOrders.length == 0 ) {
            return "";
        }
        logger.trace( "Writing {} new orders", newOrders.length );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( NewOrder newOrder : newOrders ) {
            query.append( "INSERT INTO tpcc_new_order (NO_O_ID, NO_D_ID, NO_W_ID) VALUES" );
            query.append( "(" );
            query.append( newOrder.getNO_O_ID() );
            query.append( "," );
            query.append( newOrder.getNO_D_ID() );
            query.append( "," );
            query.append( newOrder.getNO_W_ID() );
            query.append( ")" );
            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != newOrders.length ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing new orders. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    @Override
    public String writeOrderLines( List<OrderLine> orderLines ) throws ConnectionException {
        if ( orderLines.size() == 0 ) {
            return "";
        }
        logger.trace( "Writing {} orderlines", orderLines.size() );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( OrderLine orderLine : orderLines ) {
            query.append( "INSERT INTO tpcc_order_line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES" );
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

            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != orderLines.size() ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing orderlines. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    @Override
    public String writeOrders( Order[] orders ) throws ConnectionException {
        if ( orders.length == 0 ) {
            return "";
        }
        logger.trace( "Writing {} orders", orders.length );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( Order order : orders ) {
            query.append( "INSERT INTO tpcc_order (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES" );
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
            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != orders.length ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing orders. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    @Override
    public String writeHistory( List<History> histories ) throws ConnectionException {
        if ( histories.size() == 0 ) {
            return "";
        }
        logger.trace( "Writing {} histories", histories.size() );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( History history : histories ) {
            query.append( "INSERT INTO tpcc_history (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES" );
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
            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != histories.size() ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing history. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    @Override
    protected String writeCustomers( Customer[] customers ) throws ConnectionException {
        if ( customers.length == 0 ) {
            return "";
        }
        logger.trace( "Writing {} customers", customers.length );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( Customer customer : customers ) {
            query.append( "INSERT INTO tpcc_customer (C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA) VALUES" );
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
            query.append( "','" );
            query.append( customer.getC_ZIP() );
            query.append( "','" );
            query.append( customer.getC_PHONE() );
            query.append( "','" );
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
            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != customers.length ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing customers. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    @Override
    protected String writeDistricts( District[] districts ) throws ConnectionException {
        if ( districts.length == 0 ) {
            return "";
        }
        logger.trace( "Writing {} districts", districts.length );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( District district : districts ) {
            query.append( "INSERT INTO tpcc_district (D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) VALUES" );
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
            query.append( "','" );
            query.append( district.getD_ZIP() );
            query.append( "'," );
            query.append( district.getD_TAX() );
            query.append( "," );
            query.append( district.getD_YTD() );
            query.append( "," );
            query.append( district.getD_NEXT_O_ID() );
            query.append( ")" );
            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != districts.length ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing districts. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    @Override
    protected String writeStock( Stock[] stocks ) throws ConnectionException {
        if ( stocks.length == 0 ) {
            return "";
        }
        logger.trace( "Writing {} stocks", stocks.length );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( Stock stock : stocks ) {
            query.append( "INSERT INTO tpcc_stock (S_I_ID, S_W_ID, S_QUANTITY, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA) VALUES" );
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
            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != stocks.length ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing stocks. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    @Override
    protected String writeWarehouses( Warehouse[] warehouses ) throws ConnectionException {
        if ( warehouses.length == 0 ) {
            return "";
        }
        logger.trace( "Writing {} warehouses", warehouses.length );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( Warehouse warehouse : warehouses ) {
            query.append( "INSERT INTO tpcc_warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) VALUES" );
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
            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != warehouses.length ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing warehouses. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }


    @Override
    public String writeItems( Item[] items ) throws ConnectionException {
        if ( items.length == 0 ) {
            return "";
        }
        logger.trace( "Writing {} items", items.length );
        long start = System.currentTimeMillis();
        connector.startTransaction();
        StringBuilder result = new StringBuilder();
        StringBuilder query = new StringBuilder();
        int cnt = 1;
        for ( Item item : items ) {
            query.append( "INSERT INTO tpcc_item (I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA) VALUES" );
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
            query.append( ";" );

            if ( cnt % BATCH_SIZE == 0 && cnt != items.length ) {
                result.append( query.toString() );
                logger.trace( query );
                connector.executeStatement( query.toString() );
                connector.commitTransaction();
                query = new StringBuilder();
            }
            cnt++;
        }
        logger.trace( query );
        result.append( query.toString() );
        connector.executeStatement( query.toString() );
        connector.commitTransaction();
        long stop = System.currentTimeMillis();
        logger.trace( "Finished writing items. Elapsed time: {} ms", (stop - start) );
        return result.toString();
    }
}
