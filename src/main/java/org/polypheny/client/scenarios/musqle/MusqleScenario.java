package org.polypheny.client.scenarios.musqle;


import com.google.common.util.concurrent.SettableFuture;
import java.io.File;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.analysis.musqle.MusqleAnalysis;
import org.polypheny.client.chronos.DummyProgressListener;
import org.polypheny.client.chronos.ProgressListener;
import org.polypheny.client.config.Config;
import org.polypheny.client.db.musqle.IcarusMusqleBenchmarker;
import org.polypheny.client.db.musqle.MusqleBenchmarker;
import org.polypheny.client.grpc.PolyClientGRPC;
import org.polypheny.client.grpc.PolyClientGRPC.DBMSSystem;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import org.polypheny.client.job.PolyphenyJobCdl;
import org.polypheny.client.job.evaluation.PolyphenyCdlParams;
import org.polypheny.client.monitoring.NetdataMonitorAdmin;
import org.polypheny.client.rpc.ProtoObjectFactory;
import org.polypheny.client.rpc.master.WorkerStub;
import org.polypheny.client.scenarios.Scenario;
import org.polypheny.client.storage.StreamWriter;


/**
 * MuSQLE Scenario. You can think of this class as the 'master' in the distributed setup.
 *
 * @author Marco Vogt
 */
public class MusqleScenario implements Scenario {

    private static final Logger logger = LogManager.getLogger();
    private final ProgressListener progressListener;
    private PolyphenyJobCdl job;
    private File inputDir;
    private File outputDir;
    private Vector<WorkerStub> workers = new Vector<>();
    private StreamWriter<MUSQLEResultTuple> resultWriter;


    public MusqleScenario( PolyphenyJobCdl cdl, File inputDir, File outputDir, ProgressListener progressListener ) {
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
        resultWriter = new StreamWriter<>( SettableFuture.create(), storage, MUSQLEResultTuple.class );
    }


    /**
     * Uses I/O Folders from the {@link PolyphenyJobCdl}
     */
    public MusqleScenario( PolyphenyJobCdl polyphenyJobCdl ) {
        this( polyphenyJobCdl, new File( polyphenyJobCdl.getEvaluation().getSetup().getInputFolderPath() ), new File( polyphenyJobCdl.getEvaluation().getSetup().getOutputFolderPath() ), new DummyProgressListener() );
    }


    private File getResultFolderPath() {
        return Paths.get( outputDir.getPath(), "results", "tpch", job.getEvaluation().getOptions().getSystem().toString() ).toFile();
    }


    @Override
    public Properties execute() {
        MusqleBenchmarker benchmarker = generateBenchmarker();

        if ( Config.EXECUTE_POWER_TEST ) {
            powerTest( benchmarker );
        }

        launchWorkers(); //throughput test

        NetdataMonitorAdmin.launchMonitors( workers, job );

        long start = System.currentTimeMillis();
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
        long stop = System.currentTimeMillis();

        logger.debug( "Fetching Results" );

        fetchResults( start, stop );
        MusqleAnalysis analysis = new MusqleAnalysis( getStorageFolderPath(), getResultFolderPath() );
        analysis.analyze();
        NetdataMonitorAdmin.fetchWorkerMonitorResults( getResultFolderPath(), workers, job, start, stop );
        return analysis.getProperties();
    }


    private File getStorageFolderPath() {
        return Paths.get( Config.DEFAULT_WORKER_STORAGE_LOCATION, "results", "musqle", job.getEvaluation().getOptions().getSystem().toString() ).toFile();
    }


    public void powerTest( MusqleBenchmarker benchmarker ) {
        logger.info( "Executing Power test" );
        //QueryStream00
        for ( int i = 1; i <= 18; i++ ) {
            logger.info( "Executing query {}", i );
            MUSQLEResultTuple tupleQ = benchmarker.genericQueryExecutor( i );
            logTransaction( tupleQ );
        }
        logger.info( "Power test finished" );
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
            workerStub.writeMusqleResults( this.resultWriter, ProtoObjectFactory.createFetchMusqleMessage( start, stop ) );
        }
        this.resultWriter.onCompleted();
    }


    /**
     * Launches the MuSQLE Workers, using {@link PolyphenyCdlParams#getWorkerURLs()}.
     */
    private void launchWorkers() {
        int noWorkers = job.getEvaluation().getParams().getWorkers();
        logger.debug( "Launching {} workers", noWorkers );
        String[] workerURLs = job.getEvaluation().getParams().getWorkerURLs();

        if ( noWorkers != workerURLs.length ) {
            throw new IllegalArgumentException(
                    "the number of workers in the job is not equal to the number of URLs specified" );
        }

        for ( String workerURL : workerURLs ) {
            //Create workerStub
            WorkerStub worker = new WorkerStub( workerURL );
            workers.add( worker );

            //Generate WorkerMessage
            LaunchWorkerMessage workerMessage = ProtoObjectFactory.MusqleWorkerMessage(
                    job.getEvaluation().getDbms().getHost(),
                    job.getEvaluation().getDbms().getPort(),
                    job.getEvaluation().getDbms().getDatabase(),
                    job.getEvaluation().getDbms().getUsername(),
                    job.getEvaluation().getDbms().getPassword(),
                    job.getEvaluation().getOptions().getSystem(),
                    job.getEvaluation().getOptions().getAccessMethod(),
                    job.getEvaluation().getOptions().getTpchScalefactor(),
                    job.getEvaluation().getOptions().getMusqleStreams()
            );
            worker.launchWorker( workerMessage );
        }
    }


    /**
     * Creates a {@link MusqleBenchmarker} from the {@link PolyphenyJobCdl} depending on chosen parameters. The {@link MusqleBenchmarker} then can be used to generate the DB-Schema or populate the DB.
     */
    private MusqleBenchmarker generateBenchmarker() {
        DBMSSystem dbms = job.getEvaluation().getOptions().getSystem();
        if ( dbms == DBMSSystem.SYSTEMICARUS ) {
            return new IcarusMusqleBenchmarker( job );
        }
        throw new UnsupportedOperationException( "System " + dbms + " is not supported by the MuSQLE Benchmark" );
    }


    @Override
    public void abort() {
        logger.warn( "Aborting MuSQLE Scenario" );
        for ( WorkerStub worker : workers ) {
            worker.stopWorker( ProtoObjectFactory.StopWorkerMessage( PolyClientGRPC.Scenario.SCENARIOMUSQLE ) );
        }
    }


    /**
     * Stores the transaction
     *
     * @param tuple {@link MUSQLEResultTuple} which stores all relevant information about a transaction
     */
    private void logTransaction( MUSQLEResultTuple tuple ) {
        if ( tuple.getQueryID() == 0 ) {
            logger.info( "Ignoring unsupported query" );
            return;
        }
        resultWriter.onNext( tuple );
        logger.info( "Query {} with transaction {} took {} ms", tuple.getQueryID(), tuple.getTransactionType(), tuple.getResponseTime() );
    }
}
