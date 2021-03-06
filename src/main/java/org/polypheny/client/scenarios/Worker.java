package org.polypheny.client.scenarios;


import io.grpc.stub.StreamObserver;
import org.polypheny.client.grpc.PolyClientGRPC.FetchResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.ProgressMessage;
import org.polypheny.client.grpc.PolyClientGRPC.ResultMessage;


/**
 * Generic interface for all workers
 *
 * @author silvan on 02.08.17.
 */
public interface Worker {

    /**
     * Starts the worker
     */
    void start();

    /**
     * Aborts the currently running scenario
     */
    void abort();

    /**
     * Stream the Results back in batched fashion.
     *
     * @param responseObserver Listening for results. Do not complete when done, this is the responsibility of the RPC Layer
     */
    void sendResults( StreamObserver<ResultMessage> responseObserver, FetchResultsMessage request );

    /**
     * @return whether the worker is currently running
     */
    boolean isRunning();

    ProgressMessage progress();
}
