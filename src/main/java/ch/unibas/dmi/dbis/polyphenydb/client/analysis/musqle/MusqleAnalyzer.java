package ch.unibas.dmi.dbis.polyphenydb.client.analysis.musqle;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import com.google.gson.JsonObject;


/**
 * Top-level analyzer class for map-like aggregation
 */
public interface MusqleAnalyzer {


    /**
     * Receive and process the next tuple
     */
    public void process( MUSQLEResultTuple tuple );


    /**
     * Receive the results of the aggregation
     */
    public JsonObject getResults();

}
