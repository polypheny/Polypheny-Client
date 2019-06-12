package ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import com.google.gson.JsonObject;


/**
 * Top-level analyzer class for map-like aggregation.
 *
 * @author silvan on 26.07.17.
 */
public interface TPCCAnalyzer {

    /**
     * Receive and process the next {@link TPCCResultTuple}
     */
    public void process( TPCCResultTuple tuple );

    /**
     * Receive the results of the aggregation
     */
    public JsonObject getResults();

}
