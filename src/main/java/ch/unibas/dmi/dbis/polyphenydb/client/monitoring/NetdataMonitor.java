package ch.unibas.dmi.dbis.polyphenydb.client.monitoring;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.NetdataMeasurement;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.WorkerMonitorResult;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.JsonStreamReader;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StorageGson;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StreamWriter;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Monitors Netdata :)
 * The constructor takes a list of options which we expect to be direct API names. The results are written to a JSON-File while and retrieved via {@link #sendMeasurements(StreamObserver)}
 *
 * @author Silvan Heller
 */
public class NetdataMonitor implements Runnable {

    private static final Logger logger = LogManager.getLogger();
    private volatile boolean running = false;
    private String url;
    private StreamWriter<WorkerMonitorResult> writer;
    private JsonStreamReader<WorkerMonitorResult> reader;
    private File jsonStorage;
    private List<String> options;


    /**
     * @param url URL from which netdata results should be fetched
     * @param jsonStorage where you want the json to be written to
     * @throws IOException if something goes wrong with the JSON reader/writer
     */
    public NetdataMonitor( String url, File jsonStorage, List<String> options ) throws IOException {
        this.url = url;
        this.writer = new StreamWriter<>( SettableFuture.create(), jsonStorage, WorkerMonitorResult.class );
        this.jsonStorage = jsonStorage;
        this.options = options;
    }


    public void stop() {
        running = false;
        writer.onCompleted();
    }


    @Override
    public void run() {
        try {
            logger.debug( "Starting netdata monitor for url {}", url );
            running = true;
            long timestamp = System.currentTimeMillis() - 5_000;
            while ( running ) {
                try {
                    long newTimestamp = System.currentTimeMillis();
                    ArrayList<NetdataMeasurement> measurements = new ArrayList<>();
                    for ( String option : options ) {
                        measurements.addAll( measure( option, timestamp ) );
                    }
                    WorkerMonitorResult res = WorkerMonitorResult.newBuilder().addAllMeasurements( measurements ).build();
                    writer.onNext( res );
                    timestamp = newTimestamp;
                } catch ( Throwable t ) {
                    logger.error( t );  //We just continue measuring
                }
                Thread.sleep( 2_000 );
            }
        } catch ( InterruptedException e ) {
            writer.onCompleted();
            throw new RuntimeException( e );
        }
    }


    /**
     * Measures the given load and stores measurements associated with labels
     *
     * @param type which metric you want to measure
     * @param timestamp after this timestamp
     * @return maximum timestamp received by this measurement
     */
    private List<NetdataMeasurement> measure( String type, long timestamp ) throws IOException {
        String measurement = getUrl( "http://" + url + ":19999/api/v1/data?chart=" + type + "&format=json&group=average&options=ms|nonzero&after=-10&_=" + timestamp );

        ArrayList<NetdataMeasurement> measurements = new ArrayList<>();

        JsonObject obj = new JsonParser().parse( measurement ).getAsJsonObject();
        JsonArray labels = obj.getAsJsonArray( "labels" );
        for ( JsonElement element : obj.getAsJsonArray( "data" ) ) {
            JsonArray arr = element.getAsJsonArray();
            long _timestamp = arr.get( 0 ).getAsLong();
            Map<String, String> measured = new HashMap<>();
            try {
                for ( int i = 0; i < labels.size(); i++ ) {
                    measured.put( labels.get( i ).getAsString(), arr.get( i ).getAsString() );
                }
            } catch ( ArrayIndexOutOfBoundsException e ) {
                logger.debug( e );
                continue;
            }
            measurements.add( NetdataMeasurement.newBuilder().setType( type ).setTimestamp( _timestamp ).putAllMeasurements( measured ).build() );
        }
        return measurements;
    }


    /**
     * Performs a simple get request to the given url, returning the string.
     */
    private String getUrl( String url ) throws IOException {
        OkHttpClient client = new OkHttpClient();

        Request request = new Request.Builder().url( url ).build();

        Response response = client.newCall( request ).execute();
        return response.body().string();
    }


    /**
     * Streams the measurements, reading from storage. Stops also the netdata-monitor
     *
     * We use a custom deserializer for the protobuf objects since the default GSON serializer & deserializer is incapable of properly handling protobuf maps
     */
    public void sendMeasurements( StreamObserver<WorkerMonitorResult> responseObserver ) throws IOException {
        stop();
        this.reader = new JsonStreamReader<>( jsonStorage, WorkerMonitorResult.class,
                StorageGson.getGson() );
        while ( reader.hasNext() ) {
            for ( WorkerMonitorResult result : reader.readFromStream( 10000 ) ) {
                responseObserver.onNext( result );
            }
        }
        reader.cleanup();
    }
}
