package org.polypheny.client.rpc.worker;


import static org.polypheny.client.rpc.ProtoObjectFactory.okACK;

import io.grpc.stub.StreamObserver;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.config.Config;
import org.polypheny.client.grpc.ClientWorkerGrpc.ClientWorkerImplBase;
import org.polypheny.client.grpc.PolyClientGRPC;
import org.polypheny.client.grpc.PolyClientGRPC.AckMessage;
import org.polypheny.client.grpc.PolyClientGRPC.FetchMonitorWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.FetchResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.MonitorWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.ProgressMessage;
import org.polypheny.client.grpc.PolyClientGRPC.ProgressRequestMessage;
import org.polypheny.client.grpc.PolyClientGRPC.ResultMessage;
import org.polypheny.client.grpc.PolyClientGRPC.StopWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.WorkerMonitorResult;
import org.polypheny.client.monitoring.NetdataMonitor;
import org.polypheny.client.rpc.ProtoObjectFactory;
import org.polypheny.client.scenarios.Worker;
import org.polypheny.client.scenarios.musqle.worker.MusqleWorker;
import org.polypheny.client.scenarios.tpcc.worker.TPCCWorker;
import org.polypheny.client.scenarios.tpch.worker.TPCHWorker;
import org.polypheny.client.scenarios.ycsb.worker.YCSBWorker;


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
        File storageFolder = new File( Config.DEFAULT_WORKER_STORAGE_LOCATION, "netdata" );
        storageFolder.mkdirs();
        monitors.put( request.getUrl(), new NetdataMonitor( request.getUrl(), new File( storageFolder, "netdata-measurements-" + request.getUrl() + ".json" ), request.getOptionsList() ) );
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
        monitors.get( request.getUrl() ).sendMeasurements( responseObserver );
        responseObserver.onCompleted();
        logger.debug( "Leaving RPC Request to fetch monitor results" );
    }
}
