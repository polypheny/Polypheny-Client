package org.polypheny.client.db.tpcc.transactions.postgres;


import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.tpcc.transactions.StockLevelTransactionExecutor;
import org.polypheny.client.generator.tpcc.objects.OrderLine;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;


/**
 * @author silvan on 05.07.17.
 */
public class PostgresStockLevelTransactionExecutor extends StockLevelTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private DBConnector connector;


    public PostgresStockLevelTransactionExecutor(
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
        queryBuilder.append( "select count(*) as low_stock from tpcc_stock where s_i_id in (" );
        for ( Integer i_id : uniqueIIDs ) {
            queryBuilder.append( i_id + ", " );
        }
        queryBuilder.delete( queryBuilder.length() - 2, queryBuilder.length() );  //Replace last ,
        queryBuilder.append( ") and s_w_id = 1 and s_quantity<" + threshold );
        String query = queryBuilder.toString();
        return executeQuery( resultSet -> resultSet.getInt( "low_stock" ), query, QueryType.QUERYTYPESELECT );
    }
}
