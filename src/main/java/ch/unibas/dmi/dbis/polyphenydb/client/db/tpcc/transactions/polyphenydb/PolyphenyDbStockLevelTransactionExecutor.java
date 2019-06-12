package ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.client.db.access.DBConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.StockLevelTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.OrderLine;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.QueryType;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author silvan on 05.07.17.
 */
public class PolyphenyDbStockLevelTransactionExecutor extends StockLevelTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private DBConnector connector;


    public PolyphenyDbStockLevelTransactionExecutor(
            DBConnector connector ) {
        super( connector );
        this.connector = connector;
    }


    @Override
    protected int getLowStockCount( OrderLine[] orderLines, int w_id, int threshold ) {
        Set<Integer> uniqueIIDs = new HashSet<>();
        for ( OrderLine orderLine : orderLines ) {
            uniqueIIDs.add( orderLine.getOL_I_ID() );
        }
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append( "select count(*) as low_stock from tpcc_stock where s_i_id = " );
        for ( Integer i_id : uniqueIIDs ) {
            queryBuilder.append( i_id ).append( " or s_i_id =  " );
        }
        queryBuilder.delete( queryBuilder.length() - 14, queryBuilder.length() );  //Replace last 'or s_id_id='
        queryBuilder.append( " and s_w_id = 1 and s_quantity<" ).append( threshold );
        String query = queryBuilder.toString();

        return executeQuery( resultSet -> resultSet.getInt( "low_stock" ), query, QueryType.QUERYTYPESELECT );
    }
}
