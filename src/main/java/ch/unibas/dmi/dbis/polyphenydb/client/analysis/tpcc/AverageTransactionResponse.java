package ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCTransactionType;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Caculates average {@link TPCCResultTuple#getResponseTime()} per {@link TPCCTransactionType}
 *
 * @author silvan on 26.07.17.
 */
public class AverageTransactionResponse implements TPCCAnalyzer {

    private static Logger logger = LogManager.getLogger();

    private HashMap<TPCCTransactionType, MutablePair<Integer, Long>> avgResponseTime = new HashMap<>();


    public AverageTransactionResponse() {
        for ( TPCCTransactionType transactionType : TPCCTransactionType.values() ) {
            avgResponseTime.put( transactionType, new MutablePair<>( 0, 0L ) );
        }
    }


    @Override
    public void process( TPCCResultTuple tuple ) {
        MutablePair<Integer, Long> old = avgResponseTime.get( tuple.getTransactionType() );
        avgResponseTime.get( tuple.getTransactionType() ).setLeft( old.getLeft() + 1 );
        avgResponseTime.get( tuple.getTransactionType() ).setRight( (long) (old.getRight() + tuple.getResponseTime()) );
    }


    @Override
    public JsonObject getResults() {
        JsonObject results = new JsonObject();
        for ( Entry<TPCCTransactionType, MutablePair<Integer, Long>> entry : avgResponseTime.entrySet() ) {
            int timesExecuted = entry.getValue().getLeft();
            if ( timesExecuted == 0 ) {
                continue;
            }
            Long totalResponseTime = entry.getValue().getRight();
            logger.info( "Transaction {} was performed {} times with an avg response of {} ms", entry.getKey(), timesExecuted, totalResponseTime / timesExecuted );
            results.addProperty( entry.getKey().toString(), totalResponseTime / timesExecuted );
        }
        return results;
    }
}
