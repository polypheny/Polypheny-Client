package org.polypheny.client.db.tpcc.transactions;


import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.exceptions.TransactionAbortedException;
import org.polypheny.client.generator.RandomGenerator;
import org.polypheny.client.generator.tpcc.TPCCGenerator;
import org.polypheny.client.generator.tpcc.TPCCPopulationGenerator;
import org.polypheny.client.generator.tpcc.objects.Customer;
import org.polypheny.client.generator.tpcc.objects.Order;
import org.polypheny.client.generator.tpcc.objects.OrderLine;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCTransactionType;
import org.polypheny.client.rpc.ProtoObjectFactory;


/**
 * The Order-Status business transaction queries the status of a customer's last order. It represents a mid-weight read-only database transaction with a low frequency of execution and response time requirement to satisfy on-line users. In addition, this table includes non-primary key access to the
 * CUSTOMER table.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public abstract class OrderStatusTransactionExecutor extends TransactionExecutor {

    private static final Logger logger = LogManager.getLogger();


    public OrderStatusTransactionExecutor( DBConnector connector ) {
        super( connector );
    }


    public TPCCResultTuple orderStatusTransaction( final int queryID, final int C_C_LAST, final int C_C_ID, final int W_ID ) {
        queries.clear();
        logger.trace( "Starting order status transaction with queryID {}", queryID );
        long start = System.currentTimeMillis();
        startTransaction();
        int D_ID = RandomGenerator.generateUniform( 1, 10 );
        int C_W_ID = W_ID;
        int C_D_ID = D_ID;
        String C_LAST = null;
        Integer C_ID = null;
        if ( RandomGenerator.generateUniform( 1, 100 ) <= 60 ) {
            C_LAST = TPCCPopulationGenerator.generateC_LAST( TPCCGenerator.NURand( 255, 0, 999, C_C_LAST ) );
            logger.trace( "Choosing customer by last name for query {}, C_LAST={}", queryID, C_LAST );
        } else {
            C_ID = TPCCGenerator.NURand( 1023, 1, 3000, C_C_ID );
            logger.trace( "Choosing customer by c_id for query {}, C_ID={}", queryID, C_ID );
        }
        try {
            communicateInputData( D_ID, C_ID, C_LAST );
            Customer customer;
            if ( C_ID != null ) {
                customer = getCustomerInfo( C_W_ID, C_D_ID, C_ID );
                customer.setC_ID( C_ID );
            }
            if ( C_LAST != null ) {
                customer = getCustomerInfo( C_W_ID, C_D_ID, C_LAST );
                customer.setC_LAST( C_LAST );
                C_ID = customer.getC_ID();
            }
            Order order = getOrderInfo( C_W_ID, C_D_ID, C_ID );
            List<OrderLine> ol = getOrderLines( C_W_ID, C_D_ID, order.getO_ID() );
            commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished Order Status Transaction with execution time {} ms", stop - start );
            return ProtoObjectFactory.TPCCResultTuple( start, (stop - start), TPCCTransactionType.TPCCTRANSACTIONORDERSTATUS, queryID, false, queries );
        } catch ( TransactionAbortedException e ) {
            logger.debug( "Aborted transaction" );
            return ProtoObjectFactory.TPCCResultTuple( start, 0, TPCCTransactionType.TPCCTRANSACTIONORDERSTATUS, queryID, true, queries );
        }
    }


    /**
     * All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected and the corresponding sets of OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, and OL_DELIVERY_D are retrieved .
     */
    protected List<OrderLine> getOrderLines( int OL_W_ID, int OL_D_ID, Integer OL_O_ID ) {
        String query = "select ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d from tpcc_order_line where ol_w_id=" + OL_W_ID + " and ol_d_id=" + OL_D_ID + " and ol_o_id=" + OL_O_ID;
        return executeQuery( resultSet -> null, query, QueryType.QUERYTYPESELECT );    //Parsing not required //TODO But should be done anyway. This is an unfair advantage for certain DBs
    }


    /**
     * The row in the ORDER table with matching O_W_ID (equals C_W_ID), O_D_ID (equals C_D_ID), O_C_ID (equals C_ID), and with the largest existing O_ID, is selected. This is the most recent order placed by that customer. O_ID, O_ENTRY_D, and O_CARRIER_ID are retrieved .
     */
    protected Order getOrderInfo( int O_W_ID, int O_D_ID, Integer O_C_ID ) {
        String query = "select o_id, o_entry_d, o_carrier_id from tpcc_order WHERE o_w_id=" + O_W_ID + " and o_d_id=" + O_D_ID + " and o_c_id=" + O_C_ID + " ORDER BY o_id DESC LIMIT 1";
        return executeQuery( Order::new, query, QueryType.QUERYTYPESELECT );
    }


    /**
     * Case 2, the customer is selected based on customer last name: all rows in the CUSTOMER table with matching C_W_ID, C_D_ID and C_LAST are selected sorted by C_FIRST in ascending order. Let n be the number of rows selected. C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved from the row at
     * position n/ 2 rounded up in the sorted set of selected rows from the CUSTOMER table.
     */
    protected abstract Customer getCustomerInfo( int c_w_id, int c_d_id, String c_last );


    /**
     * Case 1, the customer is selected based on customer number: the row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected and C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved .
     */
    protected Customer getCustomerInfo( int c_w_id, int c_d_id, int c_id ) {
        String query = "select c_balance, c_first, c_middle, c_last from tpcc_customer where c_w_id=" + c_w_id + " and c_d_id=" + c_d_id + " and c_id=" + c_id;
        return executeQuery( Customer::new, query, QueryType.QUERYTYPESELECT );
    }


    /**
     * Clause 2.6.3.2 the Input data is communicated to the SUT
     *
     * @param c_id can be null
     * @param c_last can be null
     */
    protected void communicateInputData( int d_id, Integer c_id, String c_last ) {
        logger.trace( "By default, input data is not communicated to the SUT" );
    }
}
