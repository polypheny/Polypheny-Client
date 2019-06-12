package ch.unibas.dmi.dbis.polyphenydb.client.rpc.master;


import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.FetchMonitorWorkerMessage;
import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.createMonitorWorkerMessage;

import ch.unibas.dmi.dbis.polyphenydb.client.grpc.ClientWorkerGrpc;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.ClientWorkerGrpc.ClientWorkerStub;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.AckMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.FetchMonitorWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.FetchResultsMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MonitorWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.NetdataMeasurement;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ProgressMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ProgressRequestMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.StopWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCHResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.WorkerMonitorResult;
import ch.unibas.dmi.dbis.polyphenydb.client.job.PolyphenyJobCdl;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StreamWriter;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Abstraction for the master to communicate with a Client-Worker. Imagine this as a GRPC-Wrapper.
 *
 * @author Silvan Heller
 */
public class WorkerStub {

    private ClientWorkerStub clientStub;
    private ManagedChannel channel;
    private Logger logger = LogManager.getLogger();
    private String host;


    /**
     * @param host ip / qualified name of the worker
     * @param port port at which the worker is located
     */
    public WorkerStub( String host, int port ) {
        logger.debug( "Creating Workerstub @ Server for IP {}:{}", host, port );
        this.channel = NettyChannelBuilder.forAddress( host, port ).usePlaintext().build();
        this.clientStub = ClientWorkerGrpc.newStub( channel );
        this.host = host;
    }


    /**
     * @param host ip:port
     */
    public WorkerStub( String host ) {
        this( host.split( ":" )[0], Integer.parseInt( host.split( ":" )[1] ) );
    }


    /**
     * RPC Call to launch a {@link ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Worker}
     *
     * @param message {@link LaunchWorkerMessage} containing information for the worker
     * @return {@link AckMessage} containing information about the call
     */
    public synchronized AckMessage launchWorker( LaunchWorkerMessage message ) {
        SettableFuture<AckMessage> future = SettableFuture.create();
        this.clientStub.launchWorker( message, new LastObserver<>( future ) );
        return getFuture( future );
    }


    /**
     * Aborts a remote worker
     */
    public synchronized void stopWorker( StopWorkerMessage stopWorkerMessage ) {
        SettableFuture<AckMessage> future = SettableFuture.create();
        this.clientStub.stopWorker( stopWorkerMessage, new LastObserver<>( future ) );
        getFuture( future );
    }


    /**
     * @param writer each {@link TPCCResultTuple} will be written to this writer
     */
    public synchronized void writeTPCCResults( StreamWriter<TPCCResultTuple> writer, FetchResultsMessage fetchMessage ) {
        SettableFuture<Boolean> future = SettableFuture.create();
        this.clientStub.fetchResults( fetchMessage, new StreamObserver<ResultMessage>() {

            @Override
            public void onNext( ResultMessage resultMessage ) {
                for ( TPCCResultTuple tuple : resultMessage.getTpccResultMessage().getResultsList() ) {
                    writer.onNext( tuple );
                }
            }


            @Override
            public void onError( Throwable throwable ) {
                future.setException( throwable );
            }


            @Override
            public void onCompleted() {
                future.set( true );
            }
        } );
        getFuture( future );
    }


    /**
     * Write all results to a streamwriter
     *
     * @param writer each {@link TPCHResultTuple} will be written to this writer
     */
    public synchronized void writeTPCHResults( StreamWriter<TPCHResultTuple> writer, FetchResultsMessage fetchMessage ) {
        SettableFuture<Boolean> future = SettableFuture.create();
        this.clientStub.fetchResults( fetchMessage, new StreamObserver<ResultMessage>() {

            @Override
            public void onNext( ResultMessage resultMessage ) {
                for ( TPCHResultTuple tuple : resultMessage.getTpchResultMessage().getResultsList() ) {
                    writer.onNext( tuple );
                }
            }


            @Override
            public void onError( Throwable throwable ) {
                future.setException( throwable );
            }


            @Override
            public void onCompleted() {
                future.set( true );
            }
        } );
        getFuture( future );
    }


    public synchronized void writeYCSBResults( File storage, FetchResultsMessage fetchMessage ) throws IOException {
        SettableFuture<Boolean> future = SettableFuture.create();
        StreamWriter<ResultMessage> storer = new StreamWriter<>( future, storage, ResultMessage.class );
        this.clientStub.fetchResults( fetchMessage, storer );
        try {
            future.get(); //Waits until completion
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "RPC Error while writing YCSB Results" );
            throw new RuntimeException( e );
        }
    }


    /**
     * Write all results to a streamwriter
     *
     * @param writer each {@link ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MUSQLEResultTuple} will be written to this writer
     */
    public synchronized void writeMusqleResults( StreamWriter<MUSQLEResultTuple> writer, FetchResultsMessage fetchMessage ) {
        SettableFuture<Boolean> future = SettableFuture.create();
        this.clientStub.fetchResults( fetchMessage, new StreamObserver<ResultMessage>() {

            @Override
            public void onNext( ResultMessage resultMessage ) {
                for ( MUSQLEResultTuple tuple : resultMessage.getMusqleResultMessage().getResultsList() ) {
                    writer.onNext( tuple );
                }
            }


            @Override
            public void onError( Throwable throwable ) {
                future.setException( throwable );
            }


            @Override
            public void onCompleted() {
                future.set( true );
            }
        } );
        getFuture( future );
    }


    /**
     * Tells the worker to start measuring system metrics
     */
    public synchronized void launchWorkerMonitor( MonitorWorkerMessage workerMessage ) {
        logger.trace( "Launching worker monitor @ {}", workerMessage.getUrl() );
        SettableFuture<AckMessage> future = SettableFuture.create();
        this.clientStub.monitorWorker( workerMessage, new LastObserver<>( future ) );
        getFuture( future );
    }


    public synchronized ProgressMessage reportProgress( ProgressRequestMessage requestMessage ) {
        SettableFuture<ProgressMessage> future = SettableFuture.create();
        this.clientStub.progressReport( requestMessage, new LastObserver<>( future ) );
        return getFuture( future );
    }


    /**
     * @param job uses measurementoptions stored here
     */
    public synchronized void launchWorkerMonitor( PolyphenyJobCdl job ) {
        MonitorWorkerMessage workerMessage = ProtoObjectFactory.createMonitorWorkerMessage( this.getHost(), job.getEvaluation().getOptions().getMeasurementOptions() );
        launchWorkerMonitor( workerMessage );
    }


    private synchronized void writeWorkerMonitorResult( StreamWriter<NetdataMeasurement> writer, FetchMonitorWorkerMessage fetchMessage ) {
        SettableFuture<Boolean> future = SettableFuture.create();
        this.clientStub.fetchMonitorResults( fetchMessage, new StreamObserver<WorkerMonitorResult>() {

            @Override
            public void onNext( WorkerMonitorResult workerMonitorResult ) {
                for ( NetdataMeasurement tuple : workerMonitorResult.getMeasurementsList() ) {
                    writer.onNext( tuple );
                }
            }


            @Override
            public void onError( Throwable throwable ) {
                future.setException( throwable );
            }


            @Override
            public void onCompleted() {
                future.set( true );
            }
        } );
        getFuture( future );
    }


    /**
     * {@link #writeWorkerMonitorResult(StreamWriter, String, long, long)} using {@link #getHost()} as host
     */
    public void writeWorkerMonitorResult( StreamWriter<NetdataMeasurement> writer, long start, long stop ) {
        this.writeWorkerMonitorResult( writer, this.getHost(), start, stop );
    }


    /**
     * @param writer Where each tuple will be written to
     * @param host From where the results are
     * @param start lower bound for measurements
     * @param stop upper bound for measurements
     */
    public void writeWorkerMonitorResult( StreamWriter<NetdataMeasurement> writer, String host, long start, long stop ) {
        FetchMonitorWorkerMessage workerMessage = FetchMonitorWorkerMessage( host, start, stop );
        this.writeWorkerMonitorResult( writer, workerMessage );
    }


    /**
     * Launches a Monitor for the DBMS stored in the job
     */
    public void launchDBMonitor( PolyphenyJobCdl job ) {
        MonitorWorkerMessage workerMessage = createMonitorWorkerMessage( job.getEvaluation().getDbms().getHost(), job.getEvaluation().getOptions().getMeasurementOptions() );
        this.launchWorkerMonitor( workerMessage );
    }


    private <T> T getFuture( SettableFuture<T> future ) {
        try {
            return future.get();
        } catch ( InterruptedException | ExecutionException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Close connection to worker
     */
    public void close() {
        this.channel.shutdown();
    }


    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }


    public String getHost() {
        return host;
    }


    /**
     * This is needed for RPC-Calls. See {@link #launchWorker(LaunchWorkerMessage)} for example usage.
     */
    class LastObserver<T> implements StreamObserver<T> {

        private final SettableFuture<T> future;
        private T last = null;


        LastObserver( final SettableFuture<T> future ) {
            this.future = future;
        }


        @Override
        public void onCompleted() {
            future.set( this.last );
        }


        @Override
        public void onError( Throwable e ) {
            logger.error( e );
            future.setException( e );
        }


        @Override
        public void onNext( T t ) {
            this.last = t;
        }

    }

}
