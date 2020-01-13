package org.polypheny.client.storage;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.WorkerMonitorResult;
import org.polypheny.client.grpc.PolyClientGRPC.YCSBResultMessage;
import org.polypheny.client.grpc.PolyClientGRPC.YCSBResultTuple;
import org.polypheny.client.storage.rpc.TPCCResultMessageDeserializer;
import org.polypheny.client.storage.rpc.TPCCResultTupleDeserializer;
import org.polypheny.client.storage.rpc.TPCHResultMessageDeserializer;
import org.polypheny.client.storage.rpc.TPCHResultTupleDeserializer;
import org.polypheny.client.storage.rpc.WorkerMonitorResultDeserializer;
import org.polypheny.client.storage.rpc.YCSBResultMessageDeserializer;
import org.polypheny.client.storage.rpc.YCSBResultTupleDeserializer;


/**
 * Entry point for all custom deserializers.
 *
 * @author silvan on 11.07.17.
 */
public class StorageGson {

    private static Gson gson;


    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter( TPCCResultTuple.class, new TPCCResultTupleDeserializer() );
        gsonBuilder.registerTypeAdapter( TPCCResultMessage.class, new TPCCResultMessageDeserializer() );
        gsonBuilder.registerTypeAdapter( TPCHResultTuple.class, new TPCHResultTupleDeserializer() );
        gsonBuilder.registerTypeAdapter( TPCHResultMessage.class, new TPCHResultMessageDeserializer() );
        gsonBuilder.registerTypeAdapter( YCSBResultMessage.class, new YCSBResultMessageDeserializer() );
        gsonBuilder.registerTypeAdapter( YCSBResultTuple.class, new YCSBResultTupleDeserializer() );
        gsonBuilder.registerTypeAdapter( WorkerMonitorResult.class, new WorkerMonitorResultDeserializer() );
        gson = gsonBuilder.create();
    }


    /**
     * @return a Gson object which has relevant deserializers for all used Protobuf-objects
     */
    public static Gson getGson() {
        return gson;
    }
}
