package org.polypheny.client.analysis.tpch;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHTransactionType;


/**
 * Stores for each {@link TPCHResultTuple} the {@link TPCHTransactionType}, {@link TPCHResultTuple#getResponseTime()} and {@link TPCHResultTuple#getStartTimestamp()} which allows plotting response time versus time
 *
 * @author silvan on 26.07.17.
 */
public class TransactionResponseTimeFull implements TPCHAnalyzer {

    private static final Logger logger = LogManager.getLogger();
    private HashMap<TPCHTransactionType, JsonArray> results = new HashMap<>();


    TransactionResponseTimeFull() {
        for ( TPCHTransactionType type : TPCHTransactionType.values() ) {
            results.put( type, new JsonArray() );
        }
    }


    @Override
    public void process( TPCHResultTuple tuple ) {
        JsonObject obj = new JsonObject();
        obj.addProperty( "start", tuple.getStartTimestamp() );
        obj.addProperty( "executionTime", tuple.getResponseTime() );
        results.get( tuple.getTransactionType() ).add( obj );
    }


    @Override
    public JsonObject getResults() {
        logger.trace( results );
        JsonObject object = new JsonObject();
        for ( Entry<TPCHTransactionType, JsonArray> entry : results.entrySet() ) {
            object.add( entry.getKey().toString(), entry.getValue() );
        }
        return object;
    }
}
