package ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.QueryType;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCQueryTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Stores average {@link TPCCQueryTuple#getExecutionTime()} per {@link QueryType}
 *
 * @author silvan on 26.07.17.
 */
public class ResponseTimePerQueryType implements TPCCAnalyzer {

    private static Logger logger = LogManager.getLogger();

    private HashMap<QueryType, MutablePair<Integer, Long>> avgResponseTime = new HashMap<>();


    public ResponseTimePerQueryType() {
        for ( QueryType type : QueryType.values() ) {
            avgResponseTime.put( type, new MutablePair<>( 0, 0L ) );
        }
    }


    @Override
    public void process( TPCCResultTuple tuple ) {
        for ( TPCCQueryTuple queryTuple : tuple.getQueriesList() ) {
            MutablePair<Integer, Long> old = avgResponseTime.get( queryTuple.getQueryType() );
            avgResponseTime.get( queryTuple.getQueryType() ).setLeft( old.getLeft() + 1 );
            avgResponseTime.get( queryTuple.getQueryType() ).setRight( (long) (old.getRight() + queryTuple.getExecutionTime()) );
        }
    }


    @Override
    public JsonObject getResults() {
        JsonObject results = new JsonObject();
        for ( Entry<QueryType, MutablePair<Integer, Long>> entry : avgResponseTime.entrySet() ) {
            int timesExecuted = entry.getValue().getLeft();
            if ( timesExecuted == 0 ) {
                continue;
            }
            Long totalResponseTime = entry.getValue().getRight();
            logger.info( "QueryType {} was performed {} times with an avg response of {} ms", entry.getKey(), timesExecuted, totalResponseTime / timesExecuted );
            results.addProperty( entry.getKey().toString(), totalResponseTime / timesExecuted );
        }
        return results;
    }
}
