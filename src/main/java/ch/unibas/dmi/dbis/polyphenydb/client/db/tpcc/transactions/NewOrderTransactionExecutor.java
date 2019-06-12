package ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions;


import static ch.unibas.dmi.dbis.polyphenydb.client.generator.RandomGenerator.generateUniform;
import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.TPCCQueryTuple;
import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.TPCCResultTuple;

import ch.unibas.dmi.dbis.polyphenydb.client.db.access.DBConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.exceptions.TransactionAbortedException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.exceptions.TupleNotFoundException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.TPCCBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Customer;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.District;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Item;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.NewOrder;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Order;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.OrderLine;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Stock;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Warehouse;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.QueryType;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCTransactionType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Implementation of a new Order Transaction according to 2.4.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public abstract class NewOrderTransactionExecutor extends TransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private final int NUMBER_OF_CONFIGURED_WAREHOUSES;
    private final TPCCBenchmarker benchmarker;
    private final DBConnector connector;


    public NewOrderTransactionExecutor( int NUMBER_OF_CONFIGURED_WAREHOUSES, DBConnector connector, TPCCBenchmarker benchmarker ) {
        super( connector );
        this.connector = connector;
        this.NUMBER_OF_CONFIGURED_WAREHOUSES = NUMBER_OF_CONFIGURED_WAREHOUSES;
        this.benchmarker = benchmarker;
    }


    /**
     * Executes a new Order Transaction as specified by the TPC-C Benchmark. Contains the Transaction Profile as described in 2.4.1.5
     *
     * You are free to @Override this method if you feel your database can perform some of this logic server-side. The standard way to benchmark a new DB is to simply implement the abstract methods and leave the logic to this method. It also enables better performance comparison.
     *
     * @param queryID Unique ID of this query
     * @param C_OL_I_ID the runtime constant (determined by the server) which will be used to generate OL_I_ID
     * @return Information about how the transaction went.
     */
    public TPCCResultTuple newOrderTransaction( final int W_ID, final int queryID, final int C_OL_I_ID ) {
        queries.clear();
        logger.trace( "Starting new order transaction" );
        long start = System.currentTimeMillis();
        boolean aborted = false;

        int D_ID = generateUniform( 1, 10 );
        int C_ID = TPCCGenerator.getCID( C_OL_I_ID );
        int ol_cnt = generateUniform( 5, 15 );
        int rbk = generateUniform( 1, 100 );

        int[] OL_I_IDs = new int[ol_cnt];
        int[] OL_SUPPLY_W_IDs_ = new int[ol_cnt];
        int[] OL_QUANTITYs = new int[ol_cnt];

        int O_ALL_LOCAL = 1;

        for ( int i = 0; i < ol_cnt; i++ ) {
            int OL_I_ID = TPCCGenerator.getCOLIID( C_OL_I_ID );
            if ( i == ol_cnt - 1 && rbk == 1 ) {
                OL_I_ID = -1; //This should trigger a rollback
            }
            OL_I_IDs[i] = OL_I_ID;
            int OL_SUPPLY_W_ID;
            if ( generateUniform( 1, 100 ) > 1 ) {
                OL_SUPPLY_W_ID = W_ID;
            } else {
                OL_SUPPLY_W_ID = generateUniform( 1, NUMBER_OF_CONFIGURED_WAREHOUSES ); //4.2.2
            }
            OL_SUPPLY_W_IDs_[i] = OL_SUPPLY_W_ID;
            int OL_QUANTITY = generateUniform( 1, 10 );
            OL_QUANTITYs[i] = OL_QUANTITY;
            if ( OL_SUPPLY_W_ID != W_ID ) {
                O_ALL_LOCAL = 0;
            }
        }
        Timestamp O_ENTRY_D = Timestamp.from( Instant.now() );
        try {
            communicateInputData( D_ID, C_ID );
            startTransaction();
            Double W_TAX = getWTax( W_ID ).getW_TAX();
            District district = getDTAXandIncNextOID( W_ID, D_ID );
            Double D_TAX = district.getD_TAX();
            int O_ID = district.getD_NEXT_O_ID();
            Customer customer = getDiscountLastCredit( W_ID, D_ID, C_ID );
            double C_DISCOUNT = customer.getC_DISCOUNT();
            String C_LAST = customer.getC_LAST();
            String C_CREDIT = customer.getC_CREDIT();
            Integer O_CARRIER_ID = null;
            Order order = new Order( O_ID, C_ID, D_ID, W_ID, O_ENTRY_D, O_CARRIER_ID, ol_cnt, O_ALL_LOCAL );
            NewOrder newOrder = new NewOrder( O_ID, D_ID, W_ID );
            insertOrderNewOrder( order, newOrder );
            //For each O_OL_CNT item on the order:
            for ( int i = 0; i < ol_cnt; i++ ) {
                int OL_I_ID = OL_I_IDs[i];
                int OL_QUANTITY = OL_QUANTITYs[i];
                int OL_SUPPLY_W_ID = OL_SUPPLY_W_IDs_[i];
                try {
                    Item item = getItemPriceNameData( OL_I_ID );
                    Double I_PRICE = item.getI_PRICE();
                    String I_DATA = item.getI_DATA();
                    String I_NAME = item.getI_NAME();
                    Stock stock = getStockQuantityDataDist( D_ID, OL_I_ID, OL_SUPPLY_W_ID );
                    int S_QUANTITY = stock.getS_QUANTITY();
                    String S_DIST_xx = stock.retrieveS_DISTforID( D_ID );
                    String S_DATA = stock.getS_DATA();
                    updateStock( OL_SUPPLY_W_ID, OL_I_ID, S_QUANTITY, OL_QUANTITY,
                            O_ALL_LOCAL == 0 );
                    Double OL_AMOUNT = OL_QUANTITY * I_PRICE;
                    //Since we ignore terminal output, brand-generic is not computed
                    int OL_NUMBER = i;
                    String OL_DIST_INFO = S_DIST_xx;
                    OrderLine ol = new OrderLine( O_ID, D_ID, W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, null,
                            OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO );
                    insertOrderLine( ol );
                    //Since we ignore terminal output, total-amount is not computed
                    commitTransaction();
                    //We do not communicate to terminal
                } catch ( TupleNotFoundException e ) {
                    aborted = true;
                    logger.trace( "Item not found, rolling back transaction" );
                    rollback();
                }
            }
            long stop = System.currentTimeMillis();
            logger.trace( "New order transaction finished in {} ms", stop - start );
            return TPCCResultTuple( start, (stop - start), TPCCTransactionType.TPCCTRANSACTIONNEWORDER, queryID, aborted, queries );
        } catch ( TransactionAbortedException e ) {
            logger.debug( "Aborted transaction" );
            return TPCCResultTuple( start, 0, TPCCTransactionType.TPCCTRANSACTIONNEWORDER, queryID, true, queries );
        }

    }


    /**
     * Simply insert the whole Orderline
     */
    protected void insertOrderLine( OrderLine ol ) {
        executeAndLogFunction( () -> benchmarker.writeOrderLines( Collections.singletonList( ol ) ), QueryType.QUERYTYPEINSERT );
    }


    /**
     * If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or more, then S_QUANTITY is decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1. If the order-line is remote, then
     * S_REMOTE_CNT is incremented by 1.
     */
    protected abstract void updateStock( int S_W_ID, int S_I_ID, int s_quantity, int ol_quantity, boolean remote );


    /**
     * The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals OL_SUPPLY_W_ID) is selected . S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the district number, and S_DATA are retrieved .
     *
     * @param D_ID represents the 'xx' of the TPC-C Documentation
     */
    protected Stock getStockQuantityDataDist( int D_ID, int OL_I_ID, int OL_SUPPLY_W_ID ) {
        String query = "SELECT s_data, s_quantity, s_dist_" + (D_ID < 10 ? "0" + D_ID : D_ID) + " from tpcc_stock WHERE s_i_id=" + OL_I_ID + " and s_w_id=" + OL_SUPPLY_W_ID;
        return executeQuery( Stock::new, query, QueryType.QUERYTYPEUPDATE );
    }


    /**
     * The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected and I_PRICE, the price of the item, I_NAME, the name of the item, and I_DATA are retrieved. If I_ID has an unused value (see Clause 2.4.1.5), a "not-found " condition is signaled
     *
     * @throws TupleNotFoundException since it is expected that for some rows, no I_ID is found, this exception is thrown in that case and should be handled.
     */
    protected Item getItemPriceNameData( final int I_ID ) throws TupleNotFoundException {
        String query = "SELECT I_PRICE,I_NAME,I_DATA from tpcc_item where I_ID=" + I_ID;
        logger.trace( query );
        long start = System.currentTimeMillis();
        try ( ResultSet resultSet = connector.executeQuery( query ) ) {
            long stop = System.currentTimeMillis();
            logQuery( TPCCQueryTuple( query, stop - start, QueryType.QUERYTYPESELECT ) );
            if ( !resultSet.next() ) { //Expected Error, that's why this needs special handling
                throw new TupleNotFoundException();
            }
            return new Item( resultSet );
        } catch ( SQLException e ) {
            logger.error( e );
            throw new RuntimeException( e );
        }
    }


    /**
     * A new row is inserted into both the NEW-ORDER table and the ORDER table to reflect the creation of the new order.
     */
    protected void insertOrderNewOrder( Order order, NewOrder newOrder ) {
        executeAndLogFunction( () -> benchmarker.writeOrders( new Order[]{ order } ), QueryType.QUERYTYPEINSERT );
        executeAndLogFunction( () -> benchmarker.writeNewOrders( new NewOrder[]{ newOrder } ), QueryType.QUERYTYPEINSERT );
    }


    /**
     * The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's last name, and C_CREDIT, the customer's credit status, are retrieved . Hint: Just set the non-selected properties of the {@link Customer} to
     * null.
     */
    protected Customer getDiscountLastCredit( int w_id, int d_id, int c_id ) {
        String query = "SELECT C_LAST, C_DISCOUNT, C_CREDIT from tpcc_customer WHERE c_w_id=" + w_id + " and c_d_id=" + d_id + " and c_id=" + c_id;
        return executeQuery( Customer::new, query, QueryType.QUERYTYPESELECT );
    }


    /**
     * The row in the DISTRICT table with matching D_W_ID and D_ID is selected , D_TAX, the district tax rate, is retrieved , and D_NEXT_O_ID, the next available order number for the district, is retrieved and incremented by one.
     *
     * Hint: Just set the non-selected properties of the {@link District} to null.
     */
    protected abstract District getDTAXandIncNextOID( final int D_W_ID, final int D_ID );


    /**
     * The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse tax rate, is retrieved.
     *
     * Hint: Just set the non-selected properties of the {@link Warehouse} to null
     */
    protected Warehouse getWTax( int W_ID ) {
        String query = "SELECT W_TAX FROM tpcc_warehouse WHERE W_ID=" + W_ID;
        return executeQuery( Warehouse::new, query, QueryType.QUERYTYPESELECT );
    }


    /**
     * Input Data which is communicated to the SUT according to 2.4.3.2 and 2.4.2.2
     */
    protected void communicateInputData( int d_id, int c_id ) {
        logger.trace( "By default, input data is not communicated to the SUT" );
    }
}
