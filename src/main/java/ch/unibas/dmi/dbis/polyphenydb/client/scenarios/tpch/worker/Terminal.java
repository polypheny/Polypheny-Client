package ch.unibas.dmi.dbis.polyphenydb.client.scenarios.tpch.worker;


import static ch.unibas.dmi.dbis.polyphenydb.client.config.Config.DEFAULT_WORKER_STORAGE_LOCATION;

import ch.unibas.dmi.dbis.polyphenydb.client.config.Config;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.ConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpch.TPCHBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.FetchResultsMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCHResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCHResultMessage.Builder;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCHResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.JsonStreamReader;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StorageGson;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StreamWriter;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Polypheny-Client abstraction for one TPC-H Executor, is called terminal to mirror TPC-H Implementation. Is controlled by a {@link TPCHWorker}
 *
 * @author manuelhuerbin on 11.04.17.
 */
public class Terminal implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private TPCHWorker worker;
    private volatile boolean running;
    private TPCHBenchmarker benchmarker;
    private boolean refreshStream;
    private StreamWriter<TPCHResultTuple> resultWriter;
    private JsonStreamReader<TPCHResultTuple> resultReader;
    private int terminalID;


    Terminal( TPCHWorker worker, boolean refreshStream, int terminalID ) {
        this.worker = worker;
        this.running = false;
        this.benchmarker = worker.createBenchmarker( this );
        this.refreshStream = refreshStream;
        this.terminalID = terminalID;
        File storageFolder = new File( new File( DEFAULT_WORKER_STORAGE_LOCATION ), "terminals" );
        storageFolder.mkdirs();
        File storage = new File( storageFolder, terminalID + ".json" );
        resultWriter = new StreamWriter<>( SettableFuture.create(), storage, TPCHResultTuple.class );
        resultReader = new JsonStreamReader<>( storage, TPCHResultTuple.class, StorageGson.getGson() );
    }


    /**
     * Starts this Terminal according to TPC-H Specifications
     */
    @Override
    public void run() {
        running = true;
        while ( running ) {
            try {
                // througput test (5.3.4, page 95) run queryStream (TPCHConfig.STREAM times)
                if ( refreshStream ) {
                    //run refreshStream (once with TPCHConfig.STREAM RF pairs)
                    for ( int i = 0; i < worker.getWorkerMessage().getStreams(); i++ ) {
                        if ( !running ) {
                            continue;
                        }

                        if ( !Config.EXECUTE_REFRESH_FUNCTIONS ) {
                            logger.info( "Stopping execution on refresh-stream" );
                            this.stop();
                            return;
                        }
                        //RF1
                        TPCHResultTuple tupleRF1 = benchmarker.genericRefreshFunctionExecutor( 23 );
                        logTransaction( tupleRF1 );
                        worker.queryCounter.incrementAndGet();
                        if ( !running ) {
                            continue;
                        }
                        TPCHResultTuple tupleRF2 = benchmarker.genericRefreshFunctionExecutor( 24 );
                        logTransaction( tupleRF2 );
                        worker.queryCounter.incrementAndGet();

                    }
                } else {
                    for ( int i = 1; i <= 22; i++ ) {
                        if ( !running ) {
                            continue;
                        }
                        logTransaction( benchmarker.genericQueryExecutor( i ) );
                        worker.queryCounter.incrementAndGet();
                    }
                }
            } catch ( ConnectionException e ) {
                logger.error( "ConnectionException during transaction. Exiting." );
                throw new RuntimeException( e );
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
     * @param tuple {@link TPCHResultTuple} which stores all relevant information about a transaction
     */
    private void logTransaction( TPCHResultTuple tuple ) {
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
        Builder builder = TPCHResultMessage.newBuilder();

        resultReader.start();

        int counter = 0;
        while ( resultReader.hasNext() ) {
            //Iterate in batches of 100
            for ( TPCHResultTuple tuple : resultReader.readFromStream( 100 ) ) {
                if ( request.getStartTime() < tuple.getStartTimestamp() && request.getStopTime() > tuple.getStartTimestamp() ) {
                    counter++;
                    builder.addResults( tuple );
                    if ( counter % 100 == 0 ) {
                        responseObserver.onNext( ResultMessage.newBuilder().setTpchResultMessage( builder.build() ).build() );
                        builder.clear();
                    }
                }
            }
        }
        responseObserver.onNext( ResultMessage.newBuilder().setTpchResultMessage( builder.build() ).build() );
    }
}
