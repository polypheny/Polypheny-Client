package org.polypheny.client.db.tpcc.transactions;


import java.sql.Timestamp;
import java.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.exceptions.TransactionAbortedException;
import org.polypheny.client.db.exceptions.TupleNotFoundException;
import org.polypheny.client.generator.RandomGenerator;
import org.polypheny.client.generator.tpcc.objects.NewOrder;
import org.polypheny.client.generator.tpcc.objects.Order;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCTransactionType;
import org.polypheny.client.rpc.ProtoObjectFactory;


/**
 * Implementation of a TPCC-Delivery Transaction according to 2.7. This implementation does not use a result file or a deferred execution mode.
 *
 * @author silvan on 06.07.17.
 * @tpccversion 5.11
 */
public abstract class DeliveryTransactionExecutor extends TransactionExecutor {

    private static final Logger logger = LogManager.getLogger();


    public DeliveryTransactionExecutor( DBConnector connector ) {
        super( connector );
    }


    /**
     * @param queryID unique id for this query
     */
    public TPCCResultTuple deliveryTransaction( final int queryID, final int W_ID ) {
        queries.clear();
        long start = System.currentTimeMillis();
        logger.trace( "Starting delivery transaction" );
        final int O_CARRIER_ID = RandomGenerator.generateUniform( 1, 10 );
        final Timestamp OL_DELIVERY_D = Timestamp.from( Instant.now() );
        try {
            startTopLevelTrx();
            for ( int D_ID = 1; D_ID <= 10; D_ID++ ) {
                startIndividualTrx();
                try {
                    NewOrder newOrder = getNewOrder( W_ID, D_ID );
                    logger.trace( "Retrieved newOrder with O_ID {} for W_ID {} and D_ID {}", newOrder.getNO_O_ID(), W_ID, D_ID );
                    newOrder.setNO_D_ID( D_ID );
                    newOrder.setNO_W_ID( W_ID );
                    deleteRow( newOrder );
                    Order order = getOrder( W_ID, D_ID, newOrder.getNO_O_ID(), O_CARRIER_ID );
                    order.setO_ID( newOrder.getNO_O_ID() );
                    double OL_AMOUNT = getOLAmountSum( W_ID, D_ID, order.getO_ID(), OL_DELIVERY_D );
                    updateCustomer( W_ID, D_ID, order.getO_C_ID(), OL_AMOUNT );
                    commitIndividualTrx();
                } catch ( TupleNotFoundException e ) {
                    rollbackIndividualTrx();
                }
                logger.trace( "Loop {} with elapsed time {} ms", D_ID, System.currentTimeMillis() - start );
            }
            commitTopLevelTrx();
            long stop = System.currentTimeMillis();
            logger.trace( "Delivery transaction finished in {} ms", stop - start );

            return ProtoObjectFactory.TPCCResultTuple( start, (stop - start), TPCCTransactionType.TPCCTRANSACTIONDELIVERY, queryID, false, queries );
        } catch ( TransactionAbortedException e ) {
            logger.debug( "Aborted transaction" );
            return ProtoObjectFactory.TPCCResultTuple( start, 0, TPCCTransactionType.TPCCTRANSACTIONDELIVERY, queryID, true, queries );
        }
    }


    /**
     * The database transaction is committed unless more orders will be delivered within this database transaction.
     */
    protected abstract void commitIndividualTrx();


    /**
     * The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID (equals D_ID), and C_ID (equals O_C_ID) is selected and C_BALANCE is increased by the sum of all order-line amounts (OL_AMOUNT) previously retrieved. C_DELIVERY_CNT is incremented by 1.
     */
    protected void updateCustomer( int C_W_ID, int C_D_ID, int C_ID, double OL_AMOUNT ) {
        String query = "UPDATE tpcc_customer SET c_balance = c_balance + " + OL_AMOUNT + ", c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = " + C_W_ID + " AND c_d_id = " + C_D_ID + " AND c_id = " + C_ID;
        executeAndLogStatement( query, QueryType.QUERYTYPEUPDATE );
    }


    /**
     * All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected . All OL_DELIVERY_D, the delivery dates, are updated to the current system time as returned by the operating system and the sum of all OL_AMOUNT is
     * retrieved.
     */
    protected abstract double getOLAmountSum( int OL_W_ID, int O_D_ID, int O_ID, Timestamp OL_DELIVERY_D );


    /**
     * The row in the ORDER table with matching O_W_ID (equals W_ID), O_D_ID (equals D_ID), and O_ID (equals NO_O_ID) is selected, O_C_ID, the customer number, is retrieved, and O_CARRIER_ID is updated .
     */
    protected abstract Order getOrder( int O_W_ID, int O_D_ID, Integer NO_O_ID, int O_CARRIER_ID );


    /**
     * The selected row in the NEW-ORDER table is deleted
     *
     * @param newOrder you can only expect NO_O_ID, NO_W_ID and NO_D_ID to be set
     */
    protected void deleteRow( NewOrder newOrder ) {
        String query = "delete from tpcc_new_order where no_o_id = " + newOrder.getNO_O_ID() + " and no_d_id=" + newOrder.getNO_D_ID() + " and no_w_id=" + newOrder.getNO_W_ID();
        executeAndLogStatement( query, QueryType.QUERYTYPEDELETE );
    }


    /**
     * @see DeliveryTransactionExecutor#startTopLevelTrx()
     */
    protected void commitTopLevelTrx() {
        commitTransaction();
    }


    /**
     * Select row in the NEW-ORDER table with matching NO_W_ID and NO_D_ID.
     *
     * @throws TupleNotFoundException if no matching row is found, then the delivery of an order for this district must be skipped.
     */
    protected abstract NewOrder getNewOrder( int NO_W_ID, int NO_D_ID ) throws TupleNotFoundException;


    /**
     * A database transaction is started unless a database transaction is already active from being started as part of the delivery of a previous order (i.e., more than one order is delivered within the same database transaction).
     */
    protected abstract void startIndividualTrx();


    /**
     * See 2.7.4.1 This business transaction can be done within a single database transaction or broken down into up to 10 database transactions to allow the test sponsor the flexibility to implement the business transaction with the most efficient number of database transactions.
     */
    protected void startTopLevelTrx() {
        startTransaction();
    }


    /**
     * Rollback of the transaction
     */
    protected abstract void rollbackIndividualTrx();

}
