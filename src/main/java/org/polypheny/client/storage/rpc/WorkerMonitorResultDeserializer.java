package org.polypheny.client.storage.rpc;


import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.polypheny.client.grpc.PolyClientGRPC.WorkerMonitorResult;


/**
 * This fixes a map-specific issue with the gson serializer and deserializer.
 *
 * @author Silvan Heller
 */
public class WorkerMonitorResultDeserializer implements JsonDeserializer<WorkerMonitorResult> {

    @Override
    public WorkerMonitorResult deserialize( JsonElement json,
            java.lang.reflect.Type typeOfT,
            JsonDeserializationContext context ) throws JsonParseException {
        WorkerMonitorResult.Builder builder = WorkerMonitorResult.newBuilder();

        try {
            JsonFormat.parser().merge( json.toString(), builder );
        } catch ( InvalidProtocolBufferException e ) {
            throw new RuntimeException( e );
        }
        return builder.build();
    }
}
