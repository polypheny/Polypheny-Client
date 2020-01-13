package org.polypheny.client.scenarios.tpch.worker;


import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.tpch.IcarusTpchBenchmarker;
import org.polypheny.client.db.tpch.PostgresTpchBenchmarker;
import org.polypheny.client.db.tpch.TPCHBenchmarker;
import org.polypheny.client.grpc.PolyClientGRPC;
import org.polypheny.client.grpc.PolyClientGRPC.FetchResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.ProgressMessage;
import org.polypheny.client.grpc.PolyClientGRPC.ResultMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHWorkerMessage;
import org.polypheny.client.rpc.ProtoObjectFactory;
import org.polypheny.client.scenarios.Worker;


/**
 * The thread which each worker of the master-worker architecture executes. A {@link TPCHWorker} gets assigned a series of warehouses and launches {@link Terminal} for each of those.
 *
 * @author manuelhuerbin on 11.04.17.
 */
public class TPCHWorker implements Worker {

    private static final Logger logger = LogManager.getLogger();

    private final LaunchWorkerMessage workerMessage;
    private List<Terminal> terminals;
    private volatile boolean running = false;
    private boolean runRefreshStream;
    AtomicInteger queryCounter = new AtomicInteger( 0 );


    public TPCHWorker( LaunchWorkerMessage workerMessage ) {
        this.workerMessage = workerMessage;
        this.terminals = new ArrayList<>();
        runRefreshStream = workerMessage.getTpchWorkerMessage().getExecuteRefreshStream();
    }


    /**
     * Starts the TPC-H Worker. Launches {@link PolyClientGRPC.TPCHWorkerMessage} {@link Terminal}s per District. Launch from lower to upper bound (lower inclusive, upper exclusive)
     */
    public void start() {
        running = true;
        logger.info( "Starting TPC-H Worker with {} terminals", workerMessage.getTpchWorkerMessage().getStreams() );

        //Create Terminal for RFs (worker 0) and start them
        int terminalID = 0;
        Terminal terminalRF = new Terminal( this, runRefreshStream, terminalID );
        terminals.add( terminalRF );

        new Thread( terminalRF ).start();
        for ( int terminalIdx = 0; terminalIdx < workerMessage.getTpchWorkerMessage().getStreams(); terminalIdx++ ) {
            Terminal terminal = new Terminal( this, false, terminalIdx + 1 );
            logger.info( "Starting terminal with id {}", terminalIdx + 1 );
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
     * Parses the {@link TPCHWorkerMessage} to determine the kind of benchmarker which should be used. Each Terminal gets its own benchmarker
     *
     * @param terminal the {@link Terminal} which wants to get a benchmarker
     * @return a {@link TPCHBenchmarker} which can be used to run queries against the System
     */
    public TPCHBenchmarker createBenchmarker( Terminal terminal ) {
        switch ( this.workerMessage.getDbInfo().getSystem() ) {
            case SYSTEMPOSTGRESQL:
                try {
                    return new PostgresTpchBenchmarker( this.workerMessage );
                } catch ( ConnectionException e ) {
                    logger.fatal( "could not create a benchmarker for terminal {}", terminal.toString() );
                    throw new RuntimeException( e );
                }
            case SYSTEMICARUS:
                return new IcarusTpchBenchmarker( this.workerMessage );
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
        return ProtoObjectFactory.ProgressMessage( !running, queryCounter.get() );
    }
}
