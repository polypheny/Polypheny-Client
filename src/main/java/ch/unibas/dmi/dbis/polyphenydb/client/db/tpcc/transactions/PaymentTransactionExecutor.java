package ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions;


import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.TPCCResultTuple;

import ch.unibas.dmi.dbis.polyphenydb.client.db.access.DBConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.exceptions.TransactionAbortedException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.TPCCBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.RandomGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCPopulationGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Customer;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.District;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.History;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Warehouse;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.QueryType;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCTransactionType;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Core logic of a payment transaction according to 2.5
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public abstract class PaymentTransactionExecutor extends TransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private final TPCCBenchmarker benchmarker;


    public PaymentTransactionExecutor( DBConnector connector, TPCCBenchmarker benchmarker ) {
        super( connector );
        this.benchmarker = benchmarker;
    }


    /**
     * Executes a payment transaction as specified in 2.5. You are free to @Override this method if you feel like you can deliver better performance.
     *
     * @param C_C_LAST the runtime constant chosen by the master to generate C_LAST
     * @param queryID unique ID of this query
     */
    public TPCCResultTuple paymentTransaction( final int queryID, final int W_ID, final int NUMBER_OF_CONFIGURED_WAREHOUSES, final int C_C_LAST, final int C_C_ID ) {
        queries.clear();
        logger.trace( "Starting payment transaction with queryID {}", queryID );
        long start = System.currentTimeMillis();
        int D_ID = RandomGenerator.generateUniform( 1, 10 );
        int D_W_ID = W_ID;
        int C_D_ID;
        int C_W_ID;
        if ( RandomGenerator.generateUniform( 1, 100 ) >= 85 ) {
            C_D_ID = D_ID;
            C_W_ID = W_ID;
        } else {
            C_D_ID = RandomGenerator.generateUniform( 1, 10 );
            do {
                C_W_ID = RandomGenerator.generateUniform( 1, NUMBER_OF_CONFIGURED_WAREHOUSES );
                logger.trace( "Generated remote C_W_ID for query {}: {}", queryID, C_W_ID );
            } while ( C_W_ID == W_ID && NUMBER_OF_CONFIGURED_WAREHOUSES != 1 ); //Else this gets stuck in an endless-loop for one warehouse.
        }
        Integer C_ID = null;
        String C_LAST = null;
        if ( RandomGenerator.generateUniform( 1, 100 ) <= 60 ) {
            logger.trace( "Selecting customer based on C_LAST for query {}", queryID );
            C_LAST = TPCCPopulationGenerator.generateC_LAST( TPCCGenerator.NURand( 255, 0, 999, C_C_LAST ) );
        } else {
            logger.trace( "selecting customer based on C_ID for query {}", queryID );
            C_ID = TPCCGenerator.NURand( 1023, 1, 3000, C_C_ID );
        }
        Double H_AMOUNT = RandomGenerator.generateUniform( 1, 4_999 ) + RandomGenerator.generateUniform( 0, 99 ) / 100d;
        Timestamp H_DATE = Timestamp.from( Instant.now() );
        try {
            communicateInputData( D_ID, C_ID, C_LAST, C_D_ID, C_W_ID, H_AMOUNT );
            startTransaction();
            Warehouse warehouse = getWarehouseAndIncYTD( W_ID, H_AMOUNT );
            District district = getDistrictAndIncYTD( D_W_ID, D_ID, H_AMOUNT );
            Customer customer = null;
            if ( C_ID != null ) {
                customer = getCustomerAndPay( C_W_ID, C_D_ID, C_ID, H_AMOUNT );
                customer.setC_ID( C_ID );
            }
            if ( C_LAST != null ) {
                customer = getCustomerAndPay( C_W_ID, C_D_ID, C_LAST, H_AMOUNT );
                customer.setC_LAST( C_LAST );
                C_ID = customer.getC_ID();
            }
            customer.setC_W_ID( C_W_ID );
            customer.setC_D_ID( C_D_ID );
            if ( customer.getC_CREDIT().equals( "BC" ) ) {
                handleBCforCustomer( customer, D_ID, W_ID, H_AMOUNT );
            }
            String H_DATA = warehouse.getW_NAME() + "    " + district.getD_NAME();  //4 Spaces

            History history = new History( C_ID, C_D_ID, C_W_ID, H_DATE, H_AMOUNT, H_DATA, D_ID, W_ID );
            insertHistory( history );

            commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Finished Payment Transaction with execution time {} ms", stop - start );
            return TPCCResultTuple( start, (stop - start), TPCCTransactionType.TPCCTRANSACTIONPAYMENT, queryID, false, queries );
        } catch ( TransactionAbortedException e ) {
            logger.debug( "Aborted Transaction" );
            return TPCCResultTuple( start, 0, TPCCTransactionType.TPCCTRANSACTIONPAYMENT, queryID, true, queries );
        }
    }


    /**
     * If the value of C_CREDIT is equal to "BC", then C_DATA is also retrieved from the selected customer and the following history information: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are inserted at the left of the C_DATA field by shifting the existing content of C_DATA to the right by an
     * equal number of bytes and by discarding the bytes that are shifted out of the right side of the C_DATA field. The content of the C_DATA field never exceeds 500 characters. The selected customer is updated with the new C_DATA field. If C_DATA is implemented as two fields (see Clause 1.4.9),
     * they must be treated and operated on as one single field .
     *
     * @param customer Existing Information for the customer is stored there so you don't have to re-fetch everything.
     */
    protected abstract Customer handleBCforCustomer( Customer customer, int d_id, int w_id, Double h_amount );


    /**
     * A new row is inserted into the HISTORY table.
     */
    protected void insertHistory( History history ) {
        executeAndLogFunction( () -> benchmarker.writeHistory( Collections.singletonList( history ) ), QueryType.QUERYTYPEINSERT );
    }


    /**
     * <b>Case 1</b>, the customer is selected based on customer number: the row in the CUSTOMER table with matching C_W_ID, C_D_ID and C_ID is selected . C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, and C_BALANCE
     * are retrieved . C_BALANCE is decreased by H_AMOUN T. C_YTD_PAYMENT is increased by H_AMOUNT. C_PAYMENT_CNT is incremented by 1.
     */
    protected abstract Customer getCustomerAndPay( int c_w_id, int c_d_id, Integer c_id, Double h_amount );

    /**
     * <b>Case 2</b>, the customer is selected based on customer last name: all rows in the CUSTOMER table with matching C_W_ID, C_D_ID and C_LAST are selected sorted by C_FIRST in ascending order. Let n be the number of rows selected . C_ID, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY,
     * C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, and C_BALANCE are retrieved from the row at position (n/ 2 rounded up to the next integer) in the sorted set of selected rows from the CUSTOMER table. C_BALANCE is decreased by H_AMOUNT. C_YTD_PAYMENT is increased by
     * H_AMOUNT. C_PAYMENT_CNT is incremented by 1.
     */
    protected abstract Customer getCustomerAndPay( int c_w_id, int c_d_id, String C_LAST, Double h_amount );

    /**
     * The row in the DISTRICT table with matching D_W_ID and D_ID is selected . D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved and D_YTD, the district's year-to-date balance, is increased by H_AMOUNT.
     *
     * You can set non-selected columns to null
     */
    protected abstract District getDistrictAndIncYTD( int d_w_id, int d_id, Double h_amount );


    /**
     * The row in the WAREHOUSE table with matching W_ID is selected. W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved and W_YTD, the warehouse's year-to-date balance, is increased by H_AMOUNT.
     *
     * You can set non-selected columns to null
     */
    protected abstract Warehouse getWarehouseAndIncYTD( int W_ID, Double H_AMOUNT );


    /**
     * The input data (See Clause 2.5.3.2) are communicated to the SUT.
     *
     * @param C_ID can be null. In that case, C_LAST is set.
     * @param C_LAST can be null. Then C_ID is set.
     */
    void communicateInputData( int D_ID, Integer C_ID, String C_LAST, int C_D_ID, int C_W_ID, Double H_AMOUNT ) {
        logger.trace( "By default, input data is not communicated to the SUT" );
    }
}
