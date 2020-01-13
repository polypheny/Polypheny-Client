package org.polypheny.client.analysis.tpcc;


import com.google.gson.JsonObject;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;


/**
 * Top-level analyzer class for map-like aggregation.
 *
 * @author silvan on 26.07.17.
 */
public interface TPCCAnalyzer {

    /**
     * Receive and process the next {@link TPCCResultTuple}
     */
    void process( TPCCResultTuple tuple );

    /**
     * Receive the results of the aggregation
     */
    JsonObject getResults();

}
