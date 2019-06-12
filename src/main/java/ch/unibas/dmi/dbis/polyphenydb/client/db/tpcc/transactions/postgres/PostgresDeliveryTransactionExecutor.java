package ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.postgres;


import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.TPCCQueryTuple;

import ch.unibas.dmi.dbis.polyphenydb.client.db.access.DBConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.exceptions.TupleNotFoundException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.DeliveryTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.NewOrder;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Order;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.QueryType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author silvan on 06.07.17.
 */
public class PostgresDeliveryTransactionExecutor extends DeliveryTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private DBConnector connector;


    public PostgresDeliveryTransactionExecutor( DBConnector connector ) {
        super( connector );
        this.connector = connector;
    }


    @Override
    protected void commitIndividualTrx() {
        logger.trace( "Postgres only does the toplevel transaction" );
    }


    @Override
    protected double getOLAmountSum( int OL_W_ID, int O_D_ID, int O_ID, Timestamp OL_DELIVERY_D ) {
        String query = "update tpcc_order_line set ol_delivery_d = '" + OL_DELIVERY_D + "' where ol_d_id=" + O_D_ID + " and ol_w_id=" + OL_W_ID + " and ol_o_id=" + O_ID + " returning ol_amount";
        return executeQuery( resultSet -> {
            double OL_AMOUNT = 0;
            do {
                OL_AMOUNT += resultSet.getDouble( "OL_AMOUNT" );
            } while ( resultSet.next() );
            logger.trace( "OL_AMOUNT = {}", OL_AMOUNT );
            return OL_AMOUNT;
        }, query, QueryType.QUERYTYPEUPDATE );
    }


    @Override
    protected Order getOrder( int O_W_ID, int O_D_ID, Integer NO_O_ID, int O_CARRIER_ID ) {
        String query = "update tpcc_order set o_carrier_id=" + O_CARRIER_ID + " where o_id=" + NO_O_ID + " and o_d_id = " + O_D_ID + " and o_w_id = " + O_W_ID + " returning o_c_id";
        return executeQuery( Order::new, query, QueryType.QUERYTYPEUPDATE );
    }


    @Override
    protected NewOrder getNewOrder( int NO_W_ID, int NO_D_ID ) throws TupleNotFoundException {
        String query = "select * from tpcc_new_order where no_d_id=" + NO_D_ID + " and no_w_id=" + NO_W_ID + " order by no_o_id asc LIMIT 1;";
        logger.trace( query );
        long start = System.currentTimeMillis();
        try ( ResultSet resultSet = connector.executeQuery( query ) ) {
            long stop = System.currentTimeMillis();
            logQuery( TPCCQueryTuple( query, stop - start, QueryType.QUERYTYPESELECT ) );
            if ( !resultSet.next() ) {
                logger.error( "ResultSet could not be moved for query \n{} \n", query );
                throw new TupleNotFoundException();
            }
            NewOrder newOrder = new NewOrder();
            newOrder.setNO_O_ID( resultSet.getInt( "NO_O_ID" ) );
            newOrder.setNO_W_ID( NO_W_ID );
            newOrder.setNO_D_ID( NO_D_ID );
            return newOrder;
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    protected void startIndividualTrx() {
        logger.trace( "Postgres chooses toplevel transactions" );
    }


    @Override
    protected void rollbackIndividualTrx() {
        logger.trace( "postgres chooses toplevel transactions" );
    }
}
