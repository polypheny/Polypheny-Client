package org.polypheny.client.db.tpcc.transactions.postgres;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.tpcc.transactions.OrderStatusTransactionExecutor;
import org.polypheny.client.generator.tpcc.objects.Customer;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class PostgresOrderStatusTransactionExecutor extends OrderStatusTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private DBConnector connector;


    public PostgresOrderStatusTransactionExecutor( DBConnector connector ) {
        super( connector );
        this.connector = connector;
    }


    @Override
    protected Customer getCustomerInfo( int c_w_id, int c_d_id, String c_last ) {
        String query = "SELECT c_balance, c_first, c_middle, c_last, c_id FROM tpcc_customer WHERE c_last = '" + c_last + "' AND c_id = (SELECT c_id FROM tpcc_customer WHERE c_last = '" + c_last + "' AND c_w_id = " + c_w_id + " AND c_d_id = " + c_d_id
                + " ORDER BY c_first ASC LIMIT 1 OFFSET (SELECT ceil(count(*) :: NUMERIC / 2) - 1 FROM tpcc_customer WHERE c_last ='" + c_last + "' AND c_w_id = " + c_w_id + " AND c_d_id = " + c_d_id + ")) AND c_w_id = " + c_w_id + " AND c_d_id = " + c_d_id;
        return executeQuery( Customer::new, query, QueryType.QUERYTYPESELECT );
    }
}
