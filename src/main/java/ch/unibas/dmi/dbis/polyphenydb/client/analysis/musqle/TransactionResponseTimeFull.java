package ch.unibas.dmi.dbis.polyphenydb.client.analysis.musqle;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MusqleTransactionType;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Stores for each {@link MUSQLEResultTuple} the {@link MusqleTransactionType}, {@link MUSQLEResultTuple#getResponseTime()} and {@link MUSQLEResultTuple#getStartTimestamp()} which allows plotting response time versus time
 *
 * @author silvan on 26.07.17.
 */
public class TransactionResponseTimeFull implements MusqleAnalyzer {

    private static final Logger logger = LogManager.getLogger();
    private HashMap<MusqleTransactionType, JsonArray> results = new HashMap<>();


    TransactionResponseTimeFull() {
        for ( MusqleTransactionType type : MusqleTransactionType.values() ) {
            results.put( type, new JsonArray() );
        }
    }


    @Override
    public void process( MUSQLEResultTuple tuple ) {
        JsonObject obj = new JsonObject();
        obj.addProperty( "start", tuple.getStartTimestamp() );
        obj.addProperty( "executionTime", tuple.getResponseTime() );
        results.get( tuple.getTransactionType() ).add( obj );
    }


    @Override
    public JsonObject getResults() {
        logger.trace( results );
        JsonObject object = new JsonObject();
        for ( Entry<MusqleTransactionType, JsonArray> entry : results.entrySet() ) {
            object.add( entry.getKey().toString(), entry.getValue() );
        }
        return object;
    }
}
