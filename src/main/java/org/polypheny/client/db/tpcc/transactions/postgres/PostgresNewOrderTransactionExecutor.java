package org.polypheny.client.db.tpcc.transactions.postgres;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.tpcc.PostgresTpccBenchmarker;
import org.polypheny.client.db.tpcc.transactions.NewOrderTransactionExecutor;
import org.polypheny.client.generator.tpcc.objects.District;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class PostgresNewOrderTransactionExecutor extends NewOrderTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();


    public PostgresNewOrderTransactionExecutor( int NUMBER_OF_CONFIGURED_WAREHOUSES,
            DBConnector connector, PostgresTpccBenchmarker benchmarker ) {
        super( NUMBER_OF_CONFIGURED_WAREHOUSES, connector, benchmarker );
    }


    @Override
    protected void updateStock( int S_W_ID, int S_I_ID, int s_quantity, int ol_quantity, boolean remote ) {
        String query = "UPDATE tpcc_stock SET s_quantity=CASE WHEN s_quantity>" + ol_quantity + "+10 THEN s_quantity-" + ol_quantity + " ELSE s_quantity-" + ol_quantity + "+91 END, s_order_cnt=s_order_cnt+1,s_ytd=s_ytd+" + ol_quantity + ", s_remote_cnt=CASE WHEN " + remote
                + " then s_remote_cnt+1 else s_remote_cnt END WHERE s_w_id=" + S_W_ID + " and s_i_id=" + S_I_ID;
        executeAndLogStatement( query, QueryType.QUERYTYPEUPDATE );
    }


    @Override
    protected District getDTAXandIncNextOID( final int D_W_ID, final int D_ID ) {
        String query = "UPDATE tpcc_district SET d_next_o_id = d_next_o_id+ 1 WHERE d_id = " + D_ID + " and d_w_id = " + D_W_ID + " RETURNING d_next_o_id,d_tax";
        return executeQuery( District::new, query, QueryType.QUERYTYPEUPDATE );
    }
}
