package ch.unibas.dmi.dbis.polyphenydb.client.scenarios.musqle.worker;


import ch.unibas.dmi.dbis.polyphenydb.client.db.musqle.IcarusMusqleBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.db.musqle.MusqleBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.FetchResultsMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ProgressMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCHWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Worker;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The thread which each worker of the master-worker architecture executes. A {@link MusqleWorker} gets assigned a series of warehouses and launches {@link Terminal} for each of those.
 *
 * @author Marco Vogt
 */
public class MusqleWorker implements Worker {

    private static final Logger logger = LogManager.getLogger();

    private final LaunchWorkerMessage workerMessage;
    private List<Terminal> terminals;
    private volatile boolean running = false;
    private boolean runRefreshStream;


    public MusqleWorker( LaunchWorkerMessage workerMessage ) {
        this.workerMessage = workerMessage;
        this.terminals = new ArrayList<>();
        runRefreshStream = workerMessage.getTpchWorkerMessage().getExecuteRefreshStream();
    }


    /**
     * Starts the MusqleWorker. Launches {@link ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MUSQLEWorkerMessage}@link Terminal}s per District. Launch from lower to upper bound (lower inclusive, upper exclusive)
     */
    public void start() {
        running = true;
        logger.info( "Starting MuSQLE Worker with {} terminals", workerMessage.getMusqleWorkerMessage().getStreams() );

        for ( int terminalIdx = 0; terminalIdx < workerMessage.getMusqleWorkerMessage().getStreams(); terminalIdx++ ) {
            Terminal terminal = new Terminal( this, terminalIdx );
            logger.info( "Starting terminal with id {}", terminalIdx );
            terminals.add( terminal );
            new Thread( terminal ).start();
        }
        logger.info( "All Terminals started" );
    }


    /**
     * Aborts the currently running benchmark
     */
    public void abort() {
        logger.debug( "Aborting benchmark" );
        running = false;
        for ( Terminal terminal : terminals ) {
            terminal.stop();
        }
    }


    /**
     * Stream the Results back in batched fashion. Uses the {@link Terminal} for that.
     *
     * @param responseObserver Listening for results
     */
    public void sendResults( StreamObserver<ResultMessage> responseObserver, FetchResultsMessage request ) {
        for ( Terminal terminal : terminals ) {
            terminal.sendResults( responseObserver, request );
        }
    }


    public TPCHWorkerMessage getWorkerMessage() {
        return workerMessage.getTpchWorkerMessage();
    }


    /**
     * Parses the {@link ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MUSQLEWorkerMessage} to determine the kind of benchmarker which should be used. Each Terminal gets its own benchmarker
     *
     * @param terminal the {@link Terminal} which wants to get a benchmarker
     * @return a {@link MusqleBenchmarker} which can be used to run queries against the System
     */
    public MusqleBenchmarker createBenchmarker( Terminal terminal ) {
        switch ( this.workerMessage.getDbInfo().getSystem() ) {
            case SYSTEMICARUS:
                return new IcarusMusqleBenchmarker( this.workerMessage );
            default:
                logger.error( "System {} not supported", this.workerMessage.getDbInfo().getSystem() );
                throw new UnsupportedOperationException();
        }
    }


    public boolean isRunning() {
        return running;
    }


    @Override
    public ProgressMessage progress() {
        return ProtoObjectFactory.ProgressMessage( !running, -1 );
    }
}
