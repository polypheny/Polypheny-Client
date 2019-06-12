package ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpch;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCHResultTuple;
import com.google.gson.JsonObject;


/**
 * Top-level analyzer class for map-like aggregation.
 *
 * @author silvan on 26.07.17.
 */
public interface TPCHAnalyzer {


    /**
     * Receive and process the next tuple
     */
    public void process( TPCHResultTuple tuple );


    /**
     * Receive the results of the aggregation
     */
    public JsonObject getResults();

}
