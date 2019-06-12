package ch.unibas.dmi.dbis.polyphenydb.client.rpc.worker;


import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.okACK;

import ch.unibas.dmi.dbis.polyphenydb.client.config.Config;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.ClientWorkerGrpc.ClientWorkerImplBase;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.AckMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.FetchMonitorWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.FetchResultsMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MonitorWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ProgressMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ProgressRequestMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.StopWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.WorkerMonitorResult;
import ch.unibas.dmi.dbis.polyphenydb.client.monitoring.NetdataMonitor;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Worker;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.musqle.worker.MusqleWorker;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.tpcc.worker.TPCCWorker;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.tpch.worker.TPCHWorker;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.ycsb.worker.YCSBWorker;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * RPC Implementation or the clientworker service. This runs on Worker Machines which accept orders from master. Try to hand requests off to subclasses ASAP so that this file doesn't get too bloated.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 * @tpchversion 2.17.1
 */
public class ClientWorkerImpl extends ClientWorkerImplBase {

    private static final Logger logger = LogManager.getLogger();
    private Map<PolyClientGRPC.Scenario, Worker> workers = new HashMap<>();
    private Map<String, NetdataMonitor> monitors = new HashMap<>();


    @Override
    public void launchWorker( LaunchWorkerMessage request, StreamObserver<AckMessage> responseObserver ) {
        try {
            logger.debug( "Received RPC Request to launch worker" );
            Worker worker;
            logger.debug( "Fetching worker" );
            worker = workers.get( request.getScenario() );
            logger.debug( "Worker fetched" );
            if ( worker != null ) {
                logger.debug( "There is already a worker in the map" );
                if ( worker.isRunning() ) {
                    responseObserver.onError( new RuntimeException( "There is already a running worker on this node" ) );
                    return;
                }
                logger.debug( "That worker was stopped" );
            }
            switch ( request.getScenario() ) {
                case SCENARIOTPCC:
                    worker = new TPCCWorker( request );
                    break;
                case SCENARIOTPCH:
                    worker = new TPCHWorker( request );
                    break;
                case SCENARIOYCSB:
                    worker = new YCSBWorker( request );
                    break;
                case SCENARIOMUSQLE:
                    worker = new MusqleWorker( request );
                    break;
                default:
                    logger.debug( "Scenario " + request.getScenario() + " not supported" );
                    responseObserver.onError( new IllegalArgumentException( "Scenario " + request.getScenario() + " not supported" ) );
                    return;
            }
            logger.debug( "Starting worker" );
            worker.start();
            workers.put( request.getScenario(), worker );
            responseObserver.onNext( ProtoObjectFactory.okACK() );
            responseObserver.onCompleted();
            logger.debug( "Leaving RPC Request to launch worker" );
        } catch ( Throwable t ) {
            t.printStackTrace();
            responseObserver.onError( t );
        }
    }


    @Override
    public void progressReport( ProgressRequestMessage request, StreamObserver<ProgressMessage> responseObserver ) {
        logger.trace( "Received RPC Request for progress report" );
        Worker worker = workers.get( request.getScenario() );
        if ( worker == null ) {
            String error = "Worker for scenario " + request.getScenario() + " not found, progress can not be reported";
            responseObserver.onError( new IllegalArgumentException( error ) );
            logger.info( error );
            return;
        }
        responseObserver.onNext( worker.progress() );
        responseObserver.onCompleted();
        logger.trace( "Leaving RPC Request for progress report" );
    }


    @Override
    public void stopWorker( StopWorkerMessage request, StreamObserver<AckMessage> responseObserver ) {
        logger.debug( "Received RPC Request to stop worker" );
        Worker worker = workers.get( request.getScenario() );
        if ( worker == null ) {
            String error = "Worker for scenario " + request.getScenario() + " not found, can not stop";
            responseObserver.onError( new IllegalArgumentException( error ) );
            logger.info( error );
            return;
        }
        worker.abort();
        responseObserver.onNext( ProtoObjectFactory.okACK() );
        responseObserver.onCompleted();
        logger.debug( "Leaving RPC Request to stop worker" );
    }


    @Override
    public void fetchResults( FetchResultsMessage request, StreamObserver<ResultMessage> responseObserver ) {
        logger.debug( "Received RPC Request to fetch results" );
        Worker worker = workers.get( request.getScenario() );
        if ( worker == null ) {
            responseObserver.onError( new IllegalArgumentException( "Worker for scenario " + request.getScenario() + " not found" ) );
            return;
        }
        worker.sendResults( responseObserver, request );
        responseObserver.onCompleted();
        logger.debug( "Leaving RPC Request to fetch results" );
    }


    @Override
    public void monitorWorker( MonitorWorkerMessage request, StreamObserver<AckMessage> responseObserver ) {
        logger.debug( "Received RPC Request to monitor worker" );
        try {
            File storageFolder = new File( Config.DEFAULT_WORKER_STORAGE_LOCATION, "netdata" );
            storageFolder.mkdirs();
            monitors.put( request.getUrl(), new NetdataMonitor( request.getUrl(), new File( storageFolder, "netdata-measurements-" + request.getUrl() + ".json" ), request.getOptionsList() ) );
        } catch ( IOException e ) {
            logger.error( e );
            responseObserver.onError( e );
            return;
        }
        new Thread( monitors.get( request.getUrl() ) ).start();
        responseObserver.onNext( okACK() );
        responseObserver.onCompleted();
        logger.debug( "Leaving RPC Request to monitor worker" );
    }


    @Override
    public void fetchMonitorResults( FetchMonitorWorkerMessage request, StreamObserver<WorkerMonitorResult> responseObserver ) {
        logger.debug( "Received RPC Request to fetch monitor results" );
        if ( !monitors.containsKey( request.getUrl() ) ) {
            logger.warn( "No monitor running for url {} on this worker. Aborting request", request.getUrl() );
            responseObserver.onError( new RuntimeException( "No Monitor for given url on this worker" ) );
            return;
        }
        try {
            monitors.get( request.getUrl() ).sendMeasurements( responseObserver );
        } catch ( IOException e ) {
            logger.error( e );
            responseObserver.onError( e );
            return;
        }
        responseObserver.onCompleted();
        logger.debug( "Leaving RPC Request to fetch monitor results" );
    }
}
