package org.polypheny.client.db.tpcc.transactions;


import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.exceptions.TransactionAbortedException;
import org.polypheny.client.generator.RandomGenerator;
import org.polypheny.client.generator.tpcc.objects.District;
import org.polypheny.client.generator.tpcc.objects.OrderLine;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCTransactionType;
import org.polypheny.client.rpc.ProtoObjectFactory;


/**
 * Core Logic of the Stock-level Transaction according to 2.8
 *
 * @author silvan on 03.07.17.
 * @tpccversion 5.11
 */
public abstract class StockLevelTransactionExecutor extends TransactionExecutor {

    private static final Logger logger = LogManager.getLogger();


    public StockLevelTransactionExecutor( DBConnector connector ) {
        super( connector );
    }


    /**
     * Executes a Stock-level Transaction as specified by the TPC-C Benchmark. Contains the Transaction Profile as described in 2.8.2.2
     *
     * You are free to @Override this method if you feel your database can perform some of this logic server-side. The standard way to benchmark a new DB is to simply implement the abstract methods and leave the logic to this method. It also enables better performance comparison.
     *
     * @param queryID Unique ID of this query
     * @param D_ID which district the warehouse belongs to
     * @return Information about how the transaction went.
     */
    public TPCCResultTuple stockLevelTransaction( final int W_ID, final int queryID, final int D_ID ) {
        queries.clear();
        logger.trace( "Starting new order transaction" );
        long start = System.currentTimeMillis();
        int threshold = RandomGenerator.generateUniform( 10, 20 );

        try {
            communicateInput( threshold );
            startTransaction();

            District district = getNextOID( D_ID, W_ID );
            int D_NEXT_O_ID = district.getD_NEXT_O_ID();
            OrderLine[] orderLines = getOrderLineOIDs( W_ID, D_ID, D_NEXT_O_ID );
            int low_stock = getLowStockCount( orderLines, W_ID, threshold );
            commitTransaction();
            long stop = System.currentTimeMillis();
            logger.trace( "Stock level transaction finished in {} ms", stop - start );
            return ProtoObjectFactory.TPCCResultTuple( start, (stop - start), TPCCTransactionType.TPCCTRANSACTIONSTOCK, queryID, false, queries );
        } catch ( TransactionAbortedException e ) {
            logger.debug( "Transaction Aborted" );
            return ProtoObjectFactory.TPCCResultTuple( start, 0, TPCCTransactionType.TPCCTRANSACTIONSTOCK, queryID, true, queries );
        }
    }


    /**
     * All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals W_ID) from the list of distinct item numbers and with S_QUANTITY lower than threshold are counted (giving low_stock). Comment: Stocks must be counted only for distinct items. Thus, items that have been
     * ordered more than once in the 20 selected orders must be aggregated into a single summary count for that item .
     *
     * @param orderLines will not be distinct yet.
     */
    protected abstract int getLowStockCount( OrderLine[] orderLines, int w_id, int threshold );


    /**
     * All rows in the ORDER-LINE table with matching OL_W_ID (equals W_ID), OL_D_ID (equals D_ID), and OL_O_ID (lower than D_NEXT_O_ID and greater than or equal to D_NEXT_O_ID minus 20) are selected. They are the items for 20 recent orders of the district.
     *
     * @return Only the OL_I_ID information will be provided
     */
    protected OrderLine[] getOrderLineOIDs( int w_id, int d_id, int d_next_o_id ) {
        String query = "select ol_i_id from tpcc_order_line where ol_w_id=" + w_id + " and ol_d_id=" + d_id + " and ol_o_id<" + d_next_o_id + " and ol_o_id>=" + d_next_o_id + "-20";
        return executeQuery( resultSet -> {
            List<OrderLine> orderLines = new ArrayList<>();
            do {    //Since the top-level executeQuery-method moves the resultset already, we use a do-while loop
                orderLines.add( new OrderLine( resultSet ) );
            } while ( resultSet.next() );
            return orderLines.toArray( new OrderLine[orderLines.size()] );
        }, query, QueryType.QUERYTYPESELECT );
    }


    /**
     * The row in the DISTRICT table with matching D_W_ID and D_ID is selected and D_NEXT_O_ID is retrieved .
     */
    protected District getNextOID( int d_id, int w_id ) {
        String query = "select d_next_o_id from tpcc_district where d_w_id=" + w_id + " and d_id=" + d_id;
        return executeQuery( District::new, query, QueryType.QUERYTYPESELECT );
    }


    /**
     * The input data (see Clause 2.8.3.2) are communicated to the SUT.
     */
    protected void communicateInput( int threshold ) {
        logger.trace( "By default, Input data is not communicated to the SUT" );
    }
}
