package org.polypheny.client.scenarios.musqle.worker;


import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.config.Config;
import org.polypheny.client.db.musqle.MusqleBenchmarker;
import org.polypheny.client.grpc.PolyClientGRPC.FetchResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEResultMessage;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEResultMessage.Builder;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.ResultMessage;
import org.polypheny.client.storage.JsonStreamReader;
import org.polypheny.client.storage.StorageGson;
import org.polypheny.client.storage.StreamWriter;


/**
 * Polypheny-Client abstraction for one TPC-CH Executor, is called terminal to mirror TPC-CH Implementation. Is controlled by a {@link MusqleWorker}
 *
 * @author Marco Vogt
 */
public class Terminal implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private MusqleWorker worker;
    private volatile boolean running;
    private MusqleBenchmarker benchmarker;
    private StreamWriter<MUSQLEResultTuple> resultWriter;
    private JsonStreamReader<MUSQLEResultTuple> resultReader;
    private int terminalID;


    Terminal( MusqleWorker worker, int terminalID ) {
        this.worker = worker;
        this.running = false;
        this.benchmarker = worker.createBenchmarker( this );
        this.terminalID = terminalID;
        File storageFolder = new File( new File( Config.DEFAULT_WORKER_STORAGE_LOCATION ), "terminals" );
        storageFolder.mkdirs();
        File storage = new File( storageFolder, terminalID + ".json" );
        resultWriter = new StreamWriter<>( SettableFuture.create(), storage, MUSQLEResultTuple.class );
        resultReader = new JsonStreamReader<>( storage, MUSQLEResultTuple.class, StorageGson.getGson() );
    }


    /**
     * Starts this Terminal
     */
    @Override
    public void run() {
        running = true;
        while ( running ) {
            for ( int i = 1; i <= 18; i++ ) {
                if ( !running ) {
                    continue;
                }
                logTransaction( benchmarker.genericQueryExecutor( i ) );
            }
        }
        benchmarker.abort();
    }


    /**
     * Stops execution
     */
    public void stop() {
        resultWriter.onCompleted();
        this.running = false;
    }


    public boolean isRunning() {
        return running;
    }


    /**
     * Stores the transaction
     *
     * @param tuple {@link MUSQLEResultTuple} which stores all relevant information about a transaction
     */
    private void logTransaction( MUSQLEResultTuple tuple ) {
        if ( tuple.getQueryID() == 0 ) {
            logger.trace( "Ignoring unsupported query" );
            return;
        }
        resultWriter.onNext( tuple );
        logger.info( "Terminal {}: Query {} took {} ms", terminalID, tuple.getQueryID(), tuple.getResponseTime() );
    }


    /**
     * This only reads from the Cached Results. Once we have a real storage module, this needs to read from disk
     *
     * @param responseObserver The stored results will be sent to this StreamObserver
     */
    public void sendResults( StreamObserver<ResultMessage> responseObserver, FetchResultsMessage request ) {
        resultReader.reset();
        Builder builder = MUSQLEResultMessage.newBuilder();

        resultReader.start();

        int counter = 0;
        while ( resultReader.hasNext() ) {
            //Iterate in batches of 100
            for ( MUSQLEResultTuple tuple : resultReader.readFromStream( 100 ) ) {
                if ( request.getStartTime() < tuple.getStartTimestamp() && request.getStopTime() > tuple.getStartTimestamp() ) {
                    counter++;
                    builder.addResults( tuple );
                    if ( counter % 100 == 0 ) {
                        responseObserver.onNext( ResultMessage.newBuilder().setMusqleResultMessage( builder.build() ).build() );
                        builder.clear();
                    }
                }
            }
        }
        responseObserver.onNext( ResultMessage.newBuilder().setMusqleResultMessage( builder.build() ).build() );
    }
}
