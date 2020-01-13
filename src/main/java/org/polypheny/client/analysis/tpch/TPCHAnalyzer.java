package org.polypheny.client.analysis.tpch;


import com.google.gson.JsonObject;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;


/**
 * Top-level analyzer class for map-like aggregation.
 *
 * @author silvan on 26.07.17.
 */
public interface TPCHAnalyzer {


    /**
     * Receive and process the next tuple
     */
    void process( TPCHResultTuple tuple );


    /**
     * Receive the results of the aggregation
     */
    JsonObject getResults();

}
