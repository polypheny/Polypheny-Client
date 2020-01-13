package org.polypheny.client.scenarios.tpcc;


import static org.polypheny.client.grpc.PolyClientGRPC.Scenario.SCENARIOTPCC;

import com.google.common.util.concurrent.SettableFuture;
import java.io.File;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.chronos.DummyProgressListener;
import org.polypheny.client.chronos.ProgressListener;
import org.polypheny.client.config.Config;
import org.polypheny.client.config.TPCCConfig;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.tpcc.IcarusTpccBenchmarker;
import org.polypheny.client.db.tpcc.PolyphenyDbTpccBenchmarker;
import org.polypheny.client.db.tpcc.PostgresTpccBenchmarker;
import org.polypheny.client.db.tpcc.TPCCBenchmarker;
import org.polypheny.client.generator.tpcc.TPCCGenerator;
import org.polypheny.client.grpc.PolyClientGRPC.DBMSSystem;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.Pair;
import org.polypheny.client.grpc.PolyClientGRPC.ProgressRequestMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;
import org.polypheny.client.job.PolyphenyJobCdl;
import org.polypheny.client.job.evaluation.PolyphenyCdlParams;
import org.polypheny.client.main.analysis.TPCCAnalysis;
import org.polypheny.client.monitoring.NetdataMonitorAdmin;
import org.polypheny.client.rpc.ProtoObjectFactory;
import org.polypheny.client.rpc.master.WorkerStub;
import org.polypheny.client.scenarios.Scenario;
import org.polypheny.client.storage.StreamWriter;


/**
 * TPC-C Scenario. You can think of this class as the 'master' in the distributed setup.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class TPCCScenario implements Scenario {

    private static final Logger logger = LogManager.getLogger();

    private static final int CC_LAST_MEASUREMENT = TPCCGenerator.getCLast( TPCCGenerator.getcRun() );
    private static final int COL_I_ID_MEASUREMENT = TPCCGenerator.getCOLIID( TPCCGenerator.getcRun() );
    private static final int CC_ID_MEASUREMENT = TPCCGenerator.getCID( TPCCGenerator.getcRun() );
    private final ProgressListener progressListener;
    private PolyphenyJobCdl job;
    private File inputDir;
    private File outputDir;
    private Vector<WorkerStub> workers = new Vector<>();


    public TPCCScenario( PolyphenyJobCdl cdl, File inputDir, File outputDir, ProgressListener progressListener ) {
        this.job = cdl;
        this.inputDir = inputDir;
        this.progressListener = progressListener;
        boolean mkInput = inputDir.mkdirs();
        boolean mkOutput = outputDir.mkdirs();
        this.outputDir = outputDir;
        if ( mkInput || mkOutput ) {
            logger.debug( "Some I/O directories were not created. Input {}, Output {}", mkInput, mkOutput );
        }
    }


    /**
     * Uses I/O Folders from the {@link PolyphenyJobCdl}
     */
    public TPCCScenario( PolyphenyJobCdl polyphenyJobCdl ) {
        this( polyphenyJobCdl, new File( polyphenyJobCdl.getEvaluation().getSetup().getInputFolderPath() ), new File( polyphenyJobCdl.getEvaluation().getSetup().getOutputFolderPath() ), new DummyProgressListener() );
    }


    @Override
    public Properties execute() {
        logger.info( "Executing TPCC Scenario" );
        TPCCBenchmarker benchmarker = generateBenchmarker();

        logger.info( "Generated benchmarker {}", benchmarker.getClass().getSimpleName() );
        safeSetup( benchmarker );

        logger.info( "Launching worker" );
        launchWorkers();
        logger.info( "Workers launched" );

        NetdataMonitorAdmin.launchMonitors( workers, job );
        logger.info( "Monitors launched" );

        long start = java.lang.System.currentTimeMillis();
        long elapsedTime = 0;
        long SLEEP_TIME = 10_000;
        //Sleep for benchmark duration
        while ( elapsedTime < job.getEvaluation().getParams().getMeasurementTime() ) {
            try {
                double progress = (float) elapsedTime / job.getEvaluation().getParams().getMeasurementTime();
                progressListener.setBenchmarkProgress( progress );
                workers.forEach( w -> logger.info( "Progress for worker {} was {}", w.getHost(), w.reportProgress( ProgressRequestMessage.newBuilder().setScenario( SCENARIOTPCC ).build() ) ) );
                logger.debug( "Sleeping for {} ms", SLEEP_TIME );
                Thread.sleep( SLEEP_TIME );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }
            elapsedTime += SLEEP_TIME;
        }
        logger.info( "Aborting becnhmark" );
        abort();
        long stop = java.lang.System.currentTimeMillis();

        logger.info( "Fetching results " );
        fetchResults( start, stop );
        Properties properties = processResults();
        logger.info( "Fetching HW Monitor results" );
        NetdataMonitorAdmin.fetchWorkerMonitorResults( getResultFolderPath(), workers, job, start, stop );
        return properties;
    }


    /**
     * Safety block to prevent accidental overwriting of tables
     */
    private void safeSetup( TPCCBenchmarker benchmarker ) {
//        if ( job.getEvaluation().getOptions().getSystem().equals( DBMSSystem.SYSTEMICARUS ) ) {
//            try {
//                logger.debug( "Restarting Icarus..." );
//                Unirest.setTimeouts( 0, 0 );
//                String response = Unirest.get( "http://10.34.58.114:9090/control/stop" ).asString().getBody();
//
//                logger.debug( response );
//                response = Unirest.get( "http://10.34.58.114:9090/control/start" ).asString().getBody();
//
//                logger.debug( response );
//
//                logger.debug( "icarus restarted" );
//
//            } catch ( UnirestException e ) {
//                throw new RuntimeException( e );
//            }
//        }

        if ( job.getEvaluation().getSetup().getPopulate() ) {
            /*Scanner keyboard = new Scanner( java.lang.System.in );
            logger.warn( "Type 42 to confirm you want to populate the db" );
            int myint = keyboard.nextInt();
            if ( myint != 42 ) {
                logger.warn( "Aborting benchmark" );
                java.lang.System.exit( 1 );
            }*/
            if ( job.getEvaluation().getSetup().getCreateSchema() ) {
                logger.debug( "Creating schema..." );
                benchmarker.createTables( progressListener ); //Create Tables
            } else {
                logger.debug( "Not creating schema... " );
            }
            benchmarker.populateDatabase( progressListener ); //Populate DB
        }

    }


    private Properties processResults() {
        TPCCAnalysis analysis = new TPCCAnalysis( getMasterStorageFolderPath(), getResultFolderPath(), job.getEvaluation().getParams().getMeasurementTime() );
        analysis.analyze();
        return analysis.getProperties();
    }


    private File getResultFolderPath() {
        return Paths.get( outputDir.getPath(), "results", "tpcc", job.getEvaluation().getOptions().getSystem().toString() ).toFile();
    }


    private File getWorkerStorageFolderPath() {
        return Paths.get( Config.DEFAULT_WORKER_STORAGE_LOCATION, "results", "tpcc", job.getEvaluation().getOptions().getSystem().toString() ).toFile();
    }


    private File getMasterStorageFolderPath() {
        return Paths.get( Config.DEFAULT_MASTER_STORAGE_LOCATION, "results", "tpcc", job.getEvaluation().getOptions().getSystem().toString() ).toFile();
    }


    /**
     * @param start lower bound for result-timestamp
     * @param stop upper bound for result-timestamp
     */
    private void fetchResults( long start, long stop ) {
        File resultsFolder = getMasterStorageFolderPath();
        if ( !resultsFolder.mkdirs() ) {
            logger.trace( "results-Folder not created" );
        }
        StreamWriter<TPCCResultTuple> writer = new StreamWriter<>( SettableFuture.create(), new File( getMasterStorageFolderPath(), "allresults.json" ), TPCCResultTuple.class );
        for ( WorkerStub workerStub : workers ) {
            logger.debug( "Fetching results for worker {}", workerStub.getHost() );
            workerStub.writeTPCCResults( writer, ProtoObjectFactory.createFetchTPCCMessage( start, stop ) );
        }
        writer.onCompleted();
    }


    /**
     * Launches the TPC-C Workers, using {@link PolyphenyCdlParams#getWorkerURLs()}. The last worker will get handled separately since the warehouses he gets need to be the remaining ones.
     */
    private void launchWorkers() {
        int noWorkers = job.getEvaluation().getParams().getWorkers();
        logger.debug( "Launching {} workers", noWorkers );

        String[] workerURLs = job.getEvaluation().getParams().getWorkerURLs();

        if ( noWorkers != workerURLs.length ) {
            throw new IllegalArgumentException(
                    "the number of workers in the job is not equal to the number of URLs specified" );
        }

        int noWarehouses = job.getEvaluation().getOptions().getTpccWarehouses();
        logger.trace( "Benchmarking for {} warehouses", noWarehouses );
        int warehousePerWorker = noWarehouses / noWorkers;
        logger.trace( "Assigning {} warehouses per worker", warehousePerWorker );

        //The last one needs to take the rest, so we only go to length - 1
        for ( int i = 0; i < workerURLs.length - 1; i++ ) {
            String workerURL = workerURLs[i];
            //Create workerStub
            WorkerStub worker = new WorkerStub( workerURL );
            workers.add( worker );

            Pair pair = ProtoObjectFactory.Pair( (i * warehousePerWorker) + 1, ((i + 1) * warehousePerWorker) + 1 );
            logger.trace( "Worker @ URL {} with index {} gets {}", workerURL, i, pair );

            generateWorkerMessage( noWarehouses, worker, pair );
            //TODO Sleep parameters
        }
        launchLastWorker( workerURLs, warehousePerWorker );
    }


    private void generateWorkerMessage( int noWarehouses, WorkerStub worker, Pair pair ) {
        LaunchWorkerMessage workerMessage = ProtoObjectFactory.TPCCWorkerMessage( job.getEvaluation().getDbms().getHost(), job.getEvaluation().getDbms().getPort(), job.getEvaluation().getDbms().getDatabase(), job.getEvaluation().getDbms().getUsername(), job.getEvaluation().getDbms().getPassword(),
                job.getEvaluation().getOptions().getSystem(), job.getEvaluation().getOptions().getAccessMethod(), pair, TPCCConfig.TERMINALS_PER_DISTRICT, COL_I_ID_MEASUREMENT, CC_LAST_MEASUREMENT, CC_ID_MEASUREMENT, noWarehouses,
                job.getEvaluation().getOptions().getTpccTerminalThink(), Optional.of( 1_000L ), Optional.empty() );  //1= terminals per district
        worker.launchWorker( workerMessage );
    }


    /**
     * Since the number of warehouses is not guaranteed to be a multiple of the number of workers, the last worker needs to be generated independently
     */
    private void launchLastWorker( String[] workerURLs, int warehousePerWorker ) {
        int noWarehouses = job.getEvaluation().getOptions().getTpccWarehouses();
        String workerURL = workerURLs[workerURLs.length - 1];
        WorkerStub worker = new WorkerStub( workerURL.split( ":" )[0], Integer.parseInt( workerURL.split( ":" )[1] ) );
        workers.add( worker );

        Pair pair = ProtoObjectFactory.Pair( (workerURLs.length - 1) * warehousePerWorker + 1, noWarehouses + 1 );
        logger.trace( "Worker @ URL {} with index {} gets {}", workerURL, workerURLs.length - 1, pair );

        //Generate WorkerMessage
        generateWorkerMessage( noWarehouses, worker, pair );
    }


    /**
     * Creates a {@link TPCCBenchmarker} from the {@link PolyphenyJobCdl} depending on chosen parameters. The {@link TPCCBenchmarker} then can be used to generate the DB-Schema or populate the DB.
     */
    private TPCCBenchmarker generateBenchmarker() {
        DBMSSystem dbms = job.getEvaluation().getOptions().getSystem();
        switch ( dbms ) {
            case SYSTEMPOSTGRESQL:
                try {
                    return new PostgresTpccBenchmarker( job );
                } catch ( ConnectionException e ) {
                    logger.fatal( "Could not create Postgres-benchmarker @ master" );
                    throw new RuntimeException( e );
                }
            case SYSTEMICARUS:
                return new IcarusTpccBenchmarker( job );
            case SYSTEMPOLYPHENY:
                try {
                    return new PolyphenyDbTpccBenchmarker( job );
                } catch ( ConnectionException e ) {
                    logger.fatal( "Could not create PolyphenyDb-benchmarker @ master" );
                    throw new RuntimeException( e );
                }
            default:
                throw new UnsupportedOperationException( "System " + dbms + " is not supported by the TPC-C Benchmark" );
        }
    }


    @Override
    public void abort() {
        logger.warn( "Aborting TPCC Scenario" );
        for ( WorkerStub worker : workers ) {
            worker.stopWorker( ProtoObjectFactory.StopWorkerMessage( SCENARIOTPCC ) );
        }
    }
}
