package org.polypheny.client.db.tpcc.transactions.icarus;


import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.RESTConnector;
import org.polypheny.client.db.tpcc.transactions.StockLevelTransactionExecutor;
import org.polypheny.client.generator.tpcc.objects.OrderLine;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;


/**
 * @author silvan on 18.07.17.
 */
public class IcarusStockLevelTransactionExecutor extends StockLevelTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();


    public IcarusStockLevelTransactionExecutor( RESTConnector connector ) {
        super( connector );
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
