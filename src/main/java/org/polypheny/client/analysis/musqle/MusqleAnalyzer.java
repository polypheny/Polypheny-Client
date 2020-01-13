package org.polypheny.client.analysis.musqle;


import com.google.gson.JsonObject;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEResultTuple;


/**
 * Top-level analyzer class for map-like aggregation
 */
public interface MusqleAnalyzer {


    /**
     * Receive and process the next tuple
     */
    void process( MUSQLEResultTuple tuple );


    /**
     * Receive the results of the aggregation
     */
    JsonObject getResults();

}
