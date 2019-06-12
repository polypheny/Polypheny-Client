package ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCQueryTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCTransactionType;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Calculates average {@link TPCCQueryTuple#getExecutionTime()} per query and stores it along with an example query
 *
 * @author silvan on 26.07.17.
 */
public class ResponseTimePerQuery implements TPCCAnalyzer {

    private static Logger logger = LogManager.getLogger();

    HashMap<TPCCTransactionType, List<MutablePair<Integer, Long>>> queryResponseTimes = new HashMap<>();
    HashMap<TPCCTransactionType, List<String>> exampleQueries = new HashMap<>();


    public ResponseTimePerQuery() {
        for ( TPCCTransactionType transactionType : TPCCTransactionType.values() ) {
            queryResponseTimes.put( transactionType, new ArrayList<>() );
            exampleQueries.put( transactionType, new ArrayList<>() );
        }
    }


    @Override
    public void process( TPCCResultTuple tuple ) {
        //Response Time / query
        List<MutablePair<Integer, Long>> currentList = queryResponseTimes.get( tuple.getTransactionType() );

        for ( int i = 0; i < tuple.getQueriesList().size(); i++ ) {
            TPCCQueryTuple queryTuple = tuple.getQueries( i );
            if ( currentList.size() <= i ) {
                currentList.add( new MutablePair<>( 0, 0L ) );
                exampleQueries.get( tuple.getTransactionType() ).add( queryTuple.getQuery() );
            }
            currentList.get( i ).setLeft( currentList.get( i ).getLeft() + 1 );
            currentList.get( i ).setRight( currentList.get( i ).getRight() + queryTuple.getExecutionTime() );
        }
    }


    @Override
    public JsonObject getResults() {
        JsonObject object = new JsonObject();
        for ( Entry<TPCCTransactionType, List<MutablePair<Integer, Long>>> entry : queryResponseTimes.entrySet() ) {
            JsonArray array = new JsonArray();
            for ( int i = 0; i < entry.getValue().size(); i++ ) {
                MutablePair<Integer, Long> pair = entry.getValue().get( i );
                if ( pair.getLeft() == 0 ) {
                    continue;
                }
                logger.trace( "Transaction {}, query with index {} had an average response time of {} ms \n Example query: {}", entry.getKey(), i, pair.getRight() / pair.getLeft(), exampleQueries.get( entry.getKey() ).get( i ) );
                JsonObject result = new JsonObject();
                result.addProperty( "index", i );
                result.addProperty( "time", pair.getRight() / pair.getLeft() );
                result.addProperty( "Example query", exampleQueries.get( entry.getKey() ).get( i ) );
                array.add( result );
            }
            object.add( entry.getKey().toString(), array );
        }
        return object;
    }
}
