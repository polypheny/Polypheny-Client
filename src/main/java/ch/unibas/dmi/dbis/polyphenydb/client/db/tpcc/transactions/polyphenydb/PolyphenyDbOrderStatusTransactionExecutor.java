package ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.client.db.access.DBConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.OrderStatusTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Customer;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.QueryType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class PolyphenyDbOrderStatusTransactionExecutor extends OrderStatusTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private DBConnector connector;


    public PolyphenyDbOrderStatusTransactionExecutor( DBConnector connector ) {
        super( connector );
        this.connector = connector;
    }


    /**
     * Case 2, the customer is selected based on customer last name: all rows in the CUSTOMER table with matching C_W_ID, C_D_ID and C_LAST are selected sorted by C_FIRST in ascending order. Let n be the number of rows selected. C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved from the row at
     * position n/ 2 rounded up in the sorted set of selected rows from the CUSTOMER table.
     */
    @Override
    protected Customer getCustomerInfo( int c_w_id, int c_d_id, String c_last ) {
        String counterQuery = "SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_ID FROM tpcc_customer WHERE C_W_ID=" + c_w_id + " AND C_D_ID=" + c_d_id + " AND C_LAST= '" + c_last + "' ORDER BY C_FIRST ASC";
        String query = "SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_ID FROM tpcc_customer WHERE C_W_ID=" + c_w_id + " AND C_D_ID=" + c_d_id + " AND C_LAST= '" + c_last + "' ORDER BY C_FIRST ASC";

        //TODO: not good to execute it twice ...
        int count = executeQuery( resultSet1 -> {
            int rows = 0;

            while ( resultSet1.next() ) {
                rows++;
            }

            resultSet1.close();
            return rows;
        }, counterQuery, QueryType.QUERYTYPESELECT );

        return executeQuery( resultSet -> {
            int index;

            if ( count == 0 ) {
                logger.error( "No elements retrieved for query {}. Aborting.", query );
                throw new RuntimeException();
            }
            if ( count % 2 == 0 ) {
                index = count / 2;
            } else {
                index = count / 2 + 1;
            }
            int counter = 1; //remember, we are already in the first row
            while ( counter < index ) {
                counter++;
                resultSet.next();
            }
            return new Customer( resultSet );
        }, query, QueryType.QUERYTYPESELECT );
    }
}
