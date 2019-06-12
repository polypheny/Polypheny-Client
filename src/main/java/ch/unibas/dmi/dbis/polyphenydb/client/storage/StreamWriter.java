package ch.unibas.dmi.dbis.polyphenydb.client.storage;


import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Writes all objects of a stream into a JSON-File. Used for large amounts of streamed data. Be aware that for Protobuf serialization, we use {@link JsonFormat} and not the default Gson serialization.
 *
 * @author Silvan Heller
 */
public class StreamWriter<T> implements StreamObserver<T> {

    private static final Logger logger = LogManager.getLogger();
    private SettableFuture<Boolean> future;
    private File storage;
    private JsonWriter writer;
    private Class<T> typeParameterClass;
    private boolean closed = false;


    /**
     * Code idea taken from https://sites.google.com/site/gson/streaming
     *
     * @param future Will be set to true or false when the stream is completed.
     * @param storage Where the data should be written to
     * @param typeParameterClass used for JSON serialization. Class of the type of objects you plan to store
     */
    public StreamWriter( SettableFuture<Boolean> future, File storage, Class<T> typeParameterClass ) {
        this.future = future;
        this.storage = storage;
        try {
            writer = new JsonWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( storage ) ), "UTF-8" ) );
            this.typeParameterClass = typeParameterClass;

            writer.setIndent( "  " );
            writer.beginArray();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Simply writes the element to the {@link #writer}, using either default gson-serialization or {@link JsonFormat} for Proto-objects
     */
    @Override
    public synchronized void onNext( T t ) {
        if ( closed ) {
            logger.warn(
                    "Trying to write on a closed stream. This indicates that this client was either under very high load or still running a transaction when it received the signal to abort." );
            return;
        }
        try {
            if ( t instanceof MessageOrBuilder ) {
                String json = JsonFormat.printer().print( (MessageOrBuilder) t );
                logger.trace( "Writing protobuf object {}", json );
                writer.jsonValue( json );
                return;
            }
            Gson gson = new Gson();
            logger.debug( "Writing non-protobuf object {}", gson.toJson( t, typeParameterClass ) );
            gson.toJson( t, typeParameterClass, writer );
        } catch ( Exception e ) {
            cleanup();
            throw new RuntimeException( e );
        }
    }


    @Override
    public synchronized void onError( Throwable throwable ) {
        cleanup();
        logger.error( throwable );
        if ( future != null ) {
            future.set( false );
        }
    }


    /**
     * Closes the writer and frees underlying resources
     */
    private synchronized void cleanup() {
        if ( closed ) {
            return;
        }
        closed = true;
        try {
            Thread.sleep( 10 );
        } catch ( InterruptedException e ) {
            //ignore
        }
        try {
            if ( writer != null ) {
                writer.endArray();
            }
        } catch ( IOException e ) {
            logger.trace( "error while cleaning up writer {}", e.getMessage() );
        }
        try {
            if ( writer != null ) {
                writer.close();
            }
        } catch ( IOException e ) {
            logger.trace( "error while cleaning up writer {}", e.getMessage() );
        }
    }


    @Override
    public synchronized void onCompleted() {
        logger.trace( "All data received" );
        cleanup();
        if ( future != null ) {
            future.set( true );
        }
    }
}
