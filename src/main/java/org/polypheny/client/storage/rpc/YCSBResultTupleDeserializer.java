package org.polypheny.client.storage.rpc;


import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.lang.reflect.Type;
import org.polypheny.client.grpc.PolyClientGRPC.YCSBResultTuple;


/**
 * @author silvan on 13.07.17.
 */
public class YCSBResultTupleDeserializer implements JsonDeserializer<YCSBResultTuple> {

    @Override
    public YCSBResultTuple deserialize( JsonElement json, Type typeOfT,
            JsonDeserializationContext context ) throws JsonParseException {
        YCSBResultTuple.Builder builder = YCSBResultTuple.newBuilder();
        try {
            JsonFormat.parser().merge( json.toString(), builder );
        } catch ( InvalidProtocolBufferException e ) {
            throw new RuntimeException( e );
        }
        return builder.build();
    }
}
