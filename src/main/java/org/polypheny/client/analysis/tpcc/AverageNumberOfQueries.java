package org.polypheny.client.analysis.tpcc;


import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCTransactionType;


/**
 * Stores the average {@link TPCCResultTuple#getQueriesCount()} per {@link TPCCTransactionType}
 *
 * @author silvan on 26.07.17.
 */
public class AverageNumberOfQueries implements TPCCAnalyzer {

    public static final Logger logger = LogManager.getLogger();
    HashMap<TPCCTransactionType, MutablePair<Integer, Long>> queryCounts = new HashMap<>();


    public AverageNumberOfQueries() {
        for ( TPCCTransactionType transactionType : TPCCTransactionType.values() ) {
            queryCounts.put( transactionType, new MutablePair<>( 0, 0L ) );
        }
    }


    @Override
    public void process( TPCCResultTuple tuple ) {
//Count number of queries
        MutablePair<Integer, Long> oldQueryCount = queryCounts.get( tuple.getTransactionType() );
        queryCounts.get( tuple.getTransactionType() ).setLeft( oldQueryCount.getLeft() + 1 );
        queryCounts.get( tuple.getTransactionType() ).setRight( oldQueryCount.getRight() + tuple.getQueriesCount() );
    }


    @Override
    public JsonObject getResults() {
        JsonObject object = new JsonObject();

        for ( Entry<TPCCTransactionType, MutablePair<Integer, Long>> entry : queryCounts.entrySet() ) {
            if ( entry.getValue().getLeft() == 0 ) {
                continue;
            }
            TPCCTransactionType type = entry.getKey();
            long avgQueries = entry.getValue().getRight() / entry.getValue().getLeft();
            logger.info( "Transaction {} had an average of {} queries", type, avgQueries );
            object.addProperty( type.toString(), avgQueries );
        }
        return object;
    }
}
