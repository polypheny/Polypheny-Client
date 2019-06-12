package ch.unibas.dmi.dbis.polyphenydb.client.scenarios.ycsb.worker;


import static ch.unibas.dmi.dbis.polyphenydb.client.config.Config.DEFAULT_WORKER_STORAGE_LOCATION;

import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.YCSBResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.JsonStreamReader;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StorageGson;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StreamWriter;
import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.ycsb.Client;
import java.io.File;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * One Threads which runs queries
 *
 * @author silvan on 13.07.17.
 */
public class YCSBThread implements Runnable {

    private final static Logger logger = LogManager.getLogger();
    private volatile boolean running;
    private StreamWriter<YCSBResultTuple> resultWriter;
    private JsonStreamReader<YCSBResultTuple> resultReader;
    private LaunchWorkerMessage workerMessage;


    public YCSBThread( LaunchWorkerMessage workerMessage ) {
        this.workerMessage = workerMessage;
        File storageFolder = new File( DEFAULT_WORKER_STORAGE_LOCATION, "ycsb" );
        storageFolder.mkdirs();
        File storage = new File( storageFolder, "thread_measurements.json" );
        resultWriter = new StreamWriter<>( SettableFuture.create(), storage, YCSBResultTuple.class );
        resultReader = new JsonStreamReader<>( storage, YCSBResultTuple.class, StorageGson.getGson() );
    }


    @Override
    public void run() {
        running = true;
        logger.info( "Starting YCSBThread" );

        Properties properties = new Properties();
        properties.putAll( workerMessage.getYcsbWorkerMessage().getPropertiesMap() );
        Client.executeWithProperties( properties );
        logger.info( "Leaving YCSBThread" );
    }


    /**
     * Stops execution
     */
    public void stop() {
        resultWriter.onCompleted();
        this.running = false;
    }
}
