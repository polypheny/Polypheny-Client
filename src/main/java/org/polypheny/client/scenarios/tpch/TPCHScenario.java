package org.polypheny.client.scenarios.tpch;


import com.google.common.util.concurrent.SettableFuture;
import java.io.File;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Scanner;
import java.util.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.analysis.tpch.TPCHAnalysis;
import org.polypheny.client.chronos.DummyProgressListener;
import org.polypheny.client.chronos.ProgressListener;
import org.polypheny.client.config.Config;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.tpch.IcarusTpchBenchmarker;
import org.polypheny.client.db.tpch.PostgresTpchBenchmarker;
import org.polypheny.client.db.tpch.TPCHBenchmarker;
import org.polypheny.client.grpc.PolyClientGRPC;
import org.polypheny.client.grpc.PolyClientGRPC.DBMSSystem;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;
import org.polypheny.client.job.PolyphenyJobCdl;
import org.polypheny.client.job.evaluation.PolyphenyCdlParams;
import org.polypheny.client.monitoring.NetdataMonitorAdmin;
import org.polypheny.client.rpc.ProtoObjectFactory;
import org.polypheny.client.rpc.master.WorkerStub;
import org.polypheny.client.scenarios.Scenario;
import org.polypheny.client.storage.StreamWriter;


/**
 * a TPC-H Scenario. Holds all relevant Parameters. <p>
 *
 * @author Manuel Huerbin
 * @tpchversion 2.17.1
 */
public class TPCHScenario implements Scenario {

    private static final Logger logger = LogManager.getLogger();
    private final ProgressListener progressListener;
    private PolyphenyJobCdl job;
    private File inputDir;
    private File outputDir;
    private Vector<WorkerStub> workers = new Vector<>();
    private StreamWriter<TPCHResultTuple> resultWriter;


    public TPCHScenario( PolyphenyJobCdl cdl, File inputDir, File outputDir, ProgressListener progressListener ) {
        this.job = cdl;
        this.inputDir = inputDir;
        this.progressListener = progressListener;
        boolean mkInput = inputDir.mkdirs();
        boolean mkOutput = outputDir.mkdirs();
        this.outputDir = outputDir;
        if ( mkInput || mkOutput ) {
            logger.debug( "Some I/O directories were not created. Input {}, Output {}", mkInput, mkOutput );
        }
        getResultFolderPath().mkdirs();
        getStorageFolderPath().mkdirs();
        File storage = new File( getStorageFolderPath(), "allresults.json" );
        resultWriter = new StreamWriter<>( SettableFuture.create(), storage, TPCHResultTuple.class );
    }


    /**
     * Uses I/O Folders from the {@link PolyphenyJobCdl}
     */
    public TPCHScenario( PolyphenyJobCdl polyphenyJobCdl ) {
        this( polyphenyJobCdl, new File( polyphenyJobCdl.getEvaluation().getSetup().getInputFolderPath() ), new File( polyphenyJobCdl.getEvaluation().getSetup().getOutputFolderPath() ), new DummyProgressListener() );
    }


    private File getResultFolderPath() {
        return Paths.get( outputDir.getPath(), "results", "tpch", job.getEvaluation().getOptions().getSystem().toString() ).toFile();
    }


    @Override
    public Properties execute() {
        TPCHBenchmarker benchmarker = generateBenchmarker();

        safeSetup( benchmarker );

        if ( Config.EXECUTE_POWER_TEST ) {
            powerTest( benchmarker );
        }

        launchWorkers(); //throughput test

        NetdataMonitorAdmin.launchMonitors( workers, job );

        long start = java.lang.System.currentTimeMillis();
        long elapsedTime = 0;
        long SLEEP_TIME = 10_000;
        //Sleep for benchmark duration
        while ( elapsedTime < job.getEvaluation().getParams().getMeasurementTime() ) {
            try {
                double progress = (float) elapsedTime / job.getEvaluation().getParams().getMeasurementTime();
                progressListener.setBenchmarkProgress( progress );
                logger.debug( "Sleeping for {} ms", SLEEP_TIME );
                Thread.sleep( SLEEP_TIME );
            } catch ( InterruptedException e ) {
                logger.error( e );
            }
            elapsedTime += SLEEP_TIME;
        }
        //Abort benchmark
        abort();
        long stop = java.lang.System.currentTimeMillis();

        logger.debug( "Fetching Results" );

        fetchResults( start, stop );
        TPCHAnalysis analysis = new TPCHAnalysis( getStorageFolderPath(), getResultFolderPath() );
        analysis.analyze();
        NetdataMonitorAdmin.fetchWorkerMonitorResults( getResultFolderPath(), workers, job, start, stop );
        return analysis.getProperties();
    }


    private File getStorageFolderPath() {
        return Paths.get( Config.DEFAULT_WORKER_STORAGE_LOCATION, "results", "tpch", job.getEvaluation().getOptions().getSystem().toString() ).toFile();
    }


    public void powerTest( TPCHBenchmarker benchmarker ) {
        logger.info( "Executing Power test" );
        //power test (5.3.3, page 95) with RF1 - QueryStream00 - RF2
        //RF1
        if ( Config.EXECUTE_REFRESH_FUNCTIONS ) {
            logger.info( "Executing RF1" );
            TPCHResultTuple tupleRF1 = benchmarker.genericRefreshFunctionExecutor( 23 );
            logTransaction( tupleRF1 );
        }
        //QueryStream00
        for ( int i = 1; i <= 22; i++ ) {
            logger.info( "Executing query {}", i );
            TPCHResultTuple tupleQ = benchmarker.genericQueryExecutor( i );
            logTransaction( tupleQ );
        }
        if ( Config.EXECUTE_REFRESH_FUNCTIONS ) {
            logger.info( "Executing RF2" );
            //RF2
            TPCHResultTuple tupleRF2 = benchmarker.genericRefreshFunctionExecutor( 24 );
            logTransaction( tupleRF2 );
        }
        logger.debug( "Power test finished" );
    }


    /**
     * Safety block to prevent accidental overwriting of tables
     */
    public void safeSetup( TPCHBenchmarker benchmarker ) {
        /*if ( job.getEvaluation().getOptions().getSystem().equals( DBMSSystem.SYSTEMICARUS ) ) {
            try {
                logger.debug( "Restarting Icarus..." );
                Unirest.setTimeouts( 0, 0 );
                String response = Unirest.get( "http://10.34.58.114:9090/control/stop" ).asString().getBody();

                logger.debug( response );
                response = Unirest.get( "http://10.34.58.114:9090/control/start" ).asString().getBody();

                logger.debug( response );

                logger.debug( "icarus restarted" );

            } catch ( UnirestException e ) {
                throw new RuntimeException( e );
            }
        }*/

        if ( job.getEvaluation().getSetup().getPopulate() ) {
            Scanner keyboard = new Scanner( java.lang.System.in );
            logger.warn( "Type 42 to confirm you want to populate the db" );
            int myint = keyboard.nextInt();
            if ( myint != 42 ) {
                logger.warn( "Aborting benchmark" );
                java.lang.System.exit( 1 );
            }

            //load test (5.1.1.2, page 92)
            if ( job.getEvaluation().getSetup().getCreateSchema() ) {
                logger.debug( "Creating schema..." );
                benchmarker.createTables(); //Create Tables
            } else {
                logger.debug( "Not creating schema... " );
            }
            benchmarker.populateDatabase(); //Populate DB
        }
    }


    /**
     * @param start lower bound for result-timestamp
     * @param stop upper bound for result-timestamp
     */
    private void fetchResults( long start, long stop ) {
        File resultsFolder = getStorageFolderPath();
        if ( !resultsFolder.mkdirs() ) {
            logger.trace( "results-Folder not created" );
        }
        for ( WorkerStub workerStub : workers ) {
            logger.debug( "Fetching results for worker {}", workerStub.getHost() );
            workerStub.writeTPCHResults( this.resultWriter, ProtoObjectFactory.createFetchTPCHMessage( start, stop ) );
        }
        this.resultWriter.onCompleted();
    }


    /**
     * Launches the TPC-H Workers, using {@link PolyphenyCdlParams#getWorkerURLs()}.
     */
    private void launchWorkers() {
        int noWorkers = job.getEvaluation().getParams().getWorkers();
        logger.debug( "Launching {} workers", noWorkers );
        String[] workerURLs = job.getEvaluation().getParams().getWorkerURLs();

        if ( noWorkers != workerURLs.length ) {
            throw new IllegalArgumentException(
                    "the number of workers in the job is not equal to the number of URLs specified" );
        }

        for ( int i = 0; i < workerURLs.length; i++ ) {
            String workerURL = workerURLs[i];
            //Create workerStub
            WorkerStub worker = new WorkerStub( workerURL );
            workers.add( worker );

            //Generate WorkerMessage
            LaunchWorkerMessage workerMessage = ProtoObjectFactory.TPCHWorkerMessage( job.getEvaluation().getDbms().getHost(), job.getEvaluation().getDbms().getPort(), job.getEvaluation().getDbms().getDatabase(), job.getEvaluation().getDbms().getUsername(), job.getEvaluation().getDbms().getPassword(),
                    job.getEvaluation().getOptions().getSystem(), job.getEvaluation().getOptions().getAccessMethod(), i == 0, job.getEvaluation().getOptions().getTpchScalefactor(), job.getEvaluation().getOptions().getTpchStreams() );
            worker.launchWorker( workerMessage );
        }
    }


    /**
     * Creates a {@link TPCHBenchmarker} from the {@link PolyphenyJobCdl} depending on chosen parameters. The {@link TPCHBenchmarker} then can be used to generate the DB-Schema or populate the DB.
     */
    private TPCHBenchmarker generateBenchmarker() {
        DBMSSystem dbms = job.getEvaluation().getOptions().getSystem();
        switch ( dbms ) {
            case SYSTEMPOSTGRESQL:
                try {
                    return new PostgresTpchBenchmarker( job.getEvaluation().getOptions().getAccessMethod(), job.getEvaluation().getDbms().getHost(), job.getEvaluation().getDbms().getPort(), job.getEvaluation().getDbms().getDatabase(), job.getEvaluation().getDbms().getUsername(),
                            job.getEvaluation().getDbms().getPassword(), job.getEvaluation().getOptions().getTpchScalefactor() );
                } catch ( ConnectionException e ) {
                    logger.fatal( "Could not create Postgres-benchmarker @ master" );
                    throw new RuntimeException( e );
                }
            case SYSTEMICARUS:
                return new IcarusTpchBenchmarker( job );
            default:
                throw new UnsupportedOperationException(
                        "System " + dbms + " is not supported by the TPC-H Benchmark" );
        }
    }


    @Override
    public void abort() {
        logger.warn( "Aborting TPCH Scenario" );
        for ( WorkerStub worker : workers ) {
            worker.stopWorker( ProtoObjectFactory.StopWorkerMessage( PolyClientGRPC.Scenario.SCENARIOTPCH ) );
        }
    }


    /**
     * Stores the transaction
     *
     * @param tuple {@link TPCHResultTuple} which stores all relevant information about a transaction
     */
    private void logTransaction( TPCHResultTuple tuple ) {
        if ( tuple.getQueryID() == 0 ) {
            logger.info( "Ignoring unsupported query" );
            return;
        }
        resultWriter.onNext( tuple );
        logger.info( "Query {} with transaction {} took {} ms", tuple.getQueryID(), tuple.getTransactionType(), tuple.getResponseTime() );
    }
}
