package org.polypheny.client.db.tpcc.transactions.icarus;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.IcarusResultSet;
import org.polypheny.client.db.access.RESTConnector;
import org.polypheny.client.db.tpcc.transactions.OrderStatusTransactionExecutor;
import org.polypheny.client.generator.tpcc.objects.Customer;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;


/**
 * @author silvan on 18.07.17.
 */
public class IcarusOrderStatusTransactionExecutor extends OrderStatusTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private RESTConnector connector;


    public IcarusOrderStatusTransactionExecutor( RESTConnector connector ) {
        super( connector );
        this.connector = connector;
    }


    /**
     * Case 2, the customer is selected based on customer last name: all rows in the CUSTOMER table with matching C_W_ID, C_D_ID and C_LAST are selected sorted by C_FIRST in ascending order. Let n be the number of rows selected. C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved from the row at
     * position n/ 2 rounded up in the sorted set of selected rows from the CUSTOMER table.
     */
    @Override
    protected Customer getCustomerInfo( int c_w_id, int c_d_id, String c_last ) {
        String query = "SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_ID FROM tpcc_customer WHERE C_W_ID=" + c_w_id + " AND C_D_ID=" + c_d_id + " AND C_LAST= '" + c_last + "' ORDER BY C_FIRST ASC";
        return executeQuery( resultSet -> {
            int index;
            IcarusResultSet rs = (IcarusResultSet) resultSet;
            int count = rs.size();
            if ( count == 0 ) {
                logger.error( "No elements retrieved for query {}. Aborting.", query );
                throw new RuntimeException();
            }
            if ( count % 2 == 0 ) {
                index = count / 2;
            } else {
                index = count / 2 + 1;
            }
            int counter = 1;
            while ( counter < index ) {
                counter++;
                resultSet.next(); //The first call moves this to index 1 in TPC-C terminology
            }
            return new Customer( resultSet );
        }, query, QueryType.QUERYTYPESELECT );
    }
}
