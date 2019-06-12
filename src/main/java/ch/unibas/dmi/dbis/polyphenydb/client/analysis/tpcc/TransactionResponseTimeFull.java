package ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCTransactionType;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Stores for each {@link TPCCResultTuple} the {@link TPCCTransactionType}, {@link TPCCResultTuple#getResponseTime()} and {@link TPCCResultTuple#getStartTimestamp()} which allows plotting response time versus time
 *
 * @author silvan on 26.07.17.
 */
public class TransactionResponseTimeFull implements TPCCAnalyzer {

    private static final Logger logger = LogManager.getLogger();
    private HashMap<TPCCTransactionType, JsonArray> results = new HashMap<>();


    public TransactionResponseTimeFull() {
        for ( TPCCTransactionType type : TPCCTransactionType.values() ) {
            results.put( type, new JsonArray() );
        }
    }


    @Override
    public void process( TPCCResultTuple tuple ) {
        JsonObject obj = new JsonObject();
        obj.addProperty( "start", tuple.getStartTimestamp() );
        obj.addProperty( "executionTime", tuple.getResponseTime() );
        results.get( tuple.getTransactionType() ).add( obj );
    }


    @Override
    public JsonObject getResults() {
        logger.trace( results );
        JsonObject object = new JsonObject();
        for ( Entry<TPCCTransactionType, JsonArray> entry : results.entrySet() ) {
            object.add( entry.getKey().toString(), entry.getValue() );
        }
        return object;
    }
}
