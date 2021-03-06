package org.polypheny.client.db.tpcc.transactions.icarus;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.RESTConnector;
import org.polypheny.client.db.tpcc.IcarusTpccBenchmarker;
import org.polypheny.client.db.tpcc.transactions.NewOrderTransactionExecutor;
import org.polypheny.client.generator.tpcc.objects.District;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;


/**
 * @author silvan on 18.07.17.
 */
public class IcarusNewOrderTransactionExecutor extends NewOrderTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();


    public IcarusNewOrderTransactionExecutor( int NUMBER_OF_CONFIGURED_WAREHOUSES, RESTConnector connector, IcarusTpccBenchmarker benchmarker ) {
        super( NUMBER_OF_CONFIGURED_WAREHOUSES, connector, benchmarker );
    }


    /**
     * If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or more, then S_QUANTITY is decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1. If the order-line is remote, then
     * S_REMOTE_CNT is incremented by 1.
     */
    @Override
    protected void updateStock( int S_W_ID, int S_I_ID, int s_quantity, int ol_quantity, boolean remote ) {
        StringBuilder queryBuilder = new StringBuilder( "UPDATE tpcc_stock SET s_quantity=" );
        if ( s_quantity - ol_quantity >= 10 ) {
            queryBuilder.append( s_quantity - ol_quantity );
        } else {
            queryBuilder.append( s_quantity - ol_quantity + 91 );
        }
        queryBuilder.append( ", s_ytd = s_ytd + " ).append( ol_quantity );
        queryBuilder.append( ", s_order_cnt = s_order_cnt+1" );
        if ( remote ) {
            queryBuilder.append( ", s_remote_cnt = s_remote_cnt+1" );
        }
        queryBuilder.append( " WHERE s_w_id = " ).append( S_W_ID );
        queryBuilder.append( " AND s_i_id = " ).append( S_I_ID );
        String query = queryBuilder.toString();
        executeAndLogStatement( query, QueryType.QUERYTYPEUPDATE );
    }


    @Override
    protected District getDTAXandIncNextOID( int D_W_ID, int D_ID ) {
        String update = "UPDATE tpcc_district SET d_next_o_id = d_next_o_id+1 WHERE d_id = " + D_ID + " and d_w_id = " + D_W_ID;
        executeAndLogStatement( update, QueryType.QUERYTYPEUPDATE );

        String query = "SELECT d_next_o_id, d_tax FROM tpcc_district WHERE d_id = " + D_ID + " and D_W_ID = " + D_W_ID;
        return executeQuery( District::new, query, QueryType.QUERYTYPESELECT );
    }
}
