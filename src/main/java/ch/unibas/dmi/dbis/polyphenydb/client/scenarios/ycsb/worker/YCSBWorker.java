package ch.unibas.dmi.dbis.polyphenydb.client.scenarios.ycsb.worker;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.FetchResultsMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ProgressMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Worker;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.tpcc.worker.Terminal;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * YCSB-Worker. Contains Worker-code to execute benchmark
 *
 * @author silvan on 13.07.17.
 */
public class YCSBWorker implements Worker {

    private static final Logger logger = LogManager.getLogger();
    private final LaunchWorkerMessage workerMessage;
    private List<YCSBThread> threads;
    private boolean running = false;


    public YCSBWorker( LaunchWorkerMessage workerMessage ) {
        this.workerMessage = workerMessage;
        this.threads = new ArrayList<>();
    }


    public void start() {
        running = true;
        logger.info( "Starting benchmark @ worker" );
        YCSBThread thread = new YCSBThread( workerMessage );
        new Thread( thread ).start();
        threads.add( thread );
        logger.info( "Thread launched" );
    }


    /**
     * Aborts the currently running benchmark
     */
    public void abort() {
        running = false;
        logger.info( "Aborting benchmark @ client" );
        for ( YCSBThread thread : threads ) {
            thread.stop();
        }
    }


    /**
     * Stream the Results back in batched fashion. Uses the {@link Terminal} for that.
     *
     * @param responseObserver Listening for results
     */
    public void sendResults( StreamObserver<ResultMessage> responseObserver, FetchResultsMessage request ) {
        responseObserver.onCompleted(); //TODO
    }


    public boolean isRunning() {
        return running;
    }


    @Override
    public ProgressMessage progress() {
        return ProtoObjectFactory.ProgressMessage( !running, -1 );
    }
}
