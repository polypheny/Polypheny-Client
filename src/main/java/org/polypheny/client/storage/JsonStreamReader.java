package org.polypheny.client.storage;


import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.monitoring.NetdataMonitor;
import org.polypheny.client.storage.rpc.WorkerMonitorResultDeserializer;


/**
 * Reads from a JSON Array in a streamed manner.
 *
 * @author Silvan Heller
 */
public class JsonStreamReader<T> {

    public static final Logger logger = LogManager.getLogger();
    private File storage;
    private Class<T> typeParameterClass;
    private JsonReader reader;
    private Gson gson;
    private boolean started = false;


    /**
     * @param storage where the json data is located
     * @param typeParameterClass since we can't call T.class, we use this to help with the JSON-Parsing
     * @param gson Register your custom deserializers here for Protobuf objects. For an Example, see {@link WorkerMonitorResultDeserializer} and {@link NetdataMonitor}
     */
    public JsonStreamReader( File storage, Class<T> typeParameterClass, Gson gson ) {
        this.storage = storage;
        this.typeParameterClass = typeParameterClass;
        this.gson = gson;
        try {
            reader = new JsonReader( new InputStreamReader( new BufferedInputStream( new FileInputStream( storage ) ) ) );
        } catch ( Exception e ) {
            cleanup();
            throw new RuntimeException( e );
        }
    }


    /**
     * Close the reader and its associated resources.
     */
    public void cleanup() {
        try {
            if ( reader != null ) {
                reader.endArray();
                reader.close();
            }
        } catch ( Exception e ) {
            logger.error( "Exception during cleanup {}", e.getMessage() );
            logger.trace( "Detailed stacktrace: ", e );
        }
    }


    /**
     * @return true if there is a next object in the underlying {@link JsonReader}
     */
    public boolean hasNext() {
        try {
            return reader.hasNext();
        } catch ( IOException e ) {
            cleanup();
            return false;
        }
    }


    /**
     * Reads the next n elements from this instance of the underlying {@link JsonReader}
     *
     * @param n how many elements you want to read
     * @return the next n elements from the stored json-array (or how many there are remaining)
     */
    public synchronized List<T> readFromStream( int n ) {
        start();
        List<T> results = new ArrayList<>( n );
        try {
            int counter = 0;
            while ( reader.hasNext() && counter < n ) {
                counter++;
                T element = gson.fromJson( reader, typeParameterClass );
                results.add( element );
            }
        } catch ( Exception e ) {
            logger.error( "Error during readFromStream {}", e.getMessage() );
            cleanup();
            throw new RuntimeException( e );
        }
        return results;
    }


    public void start() {
        if ( !started ) {
            try {
                started = true;
                reader.beginArray();
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }
    }


    /**
     * Resets this reader to the start of the file
     */
    public void reset() {
        try {
            started = false;
            reader = new JsonReader( new InputStreamReader( new BufferedInputStream( new FileInputStream( storage ) ) ) );
            start();
        } catch ( Exception e ) {
            cleanup();
            throw new RuntimeException( e );
        }
    }
}
