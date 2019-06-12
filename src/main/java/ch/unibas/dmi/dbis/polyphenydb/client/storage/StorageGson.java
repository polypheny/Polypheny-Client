package ch.unibas.dmi.dbis.polyphenydb.client.storage;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCHResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCHResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.WorkerMonitorResult;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.YCSBResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.YCSBResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.rpc.TPCCResultMessageDeserializer;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.rpc.TPCCResultTupleDeserializer;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.rpc.TPCHResultMessageDeserializer;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.rpc.TPCHResultTupleDeserializer;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.rpc.WorkerMonitorResultDeserializer;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.rpc.YCSBResultMessageDeserializer;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.rpc.YCSBResultTupleDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


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
