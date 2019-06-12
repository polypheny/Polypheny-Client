package ch.unibas.dmi.dbis.polyphenydb.client.scenarios.ycsb;


import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.FetchYCSBResultsMessage;
import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.StopWorkerMessage;
import static ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory.createMonitorWorkerMessage;

import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.job.PolyphenyJobCdl;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.master.WorkerStub;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Scenario;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author silvan on 13.07.17.
 */
public class YCSBScenario implements Scenario {

    private static Logger logger = LogManager.getLogger();
    private final PolyphenyJobCdl job;
    private final File inputDir;
    private final File outputDir;
    private Vector<WorkerStub> workers = new Vector<>();


    public YCSBScenario( PolyphenyJobCdl cdl,
            File inputDir, File outputDir ) {
        this.job = cdl;
        this.inputDir = inputDir;
        boolean mkInput = inputDir.mkdirs();
        boolean mkOutput = outputDir.mkdirs();
        this.outputDir = outputDir;
        if ( mkInput || mkOutput ) {
            logger.debug( "Some I/O directories were not created. Input {}, Output {}", mkInput, mkOutput );
        }
    }


    public YCSBScenario( PolyphenyJobCdl polyphenyJobCdl ) {
        this( polyphenyJobCdl, new File( polyphenyJobCdl.getEvaluation().getSetup().getInputFolderPath() ),
                new File( polyphenyJobCdl.getEvaluation().getSetup().getOutputFolderPath() ) );
    }


    @Override
    public Properties execute() {
        logger.debug( "Executing YCSB Benchmark" );
        long start = System.currentTimeMillis();
        launchWorkers();

        try {
            Thread.sleep( 30_000 );
        } catch ( InterruptedException ignored ) {
        }

        abort();
        long stop = System.currentTimeMillis();

        // Fetch Results
        File resultsFolder = new File( outputDir, "results" );
        resultsFolder = new File( resultsFolder, "ycsb" );
        resultsFolder.mkdirs();
        for ( WorkerStub workerStub : workers ) {
            File storageFile = new File( resultsFolder, workerStub.getHost() + "_results.json" );
            try {
                workerStub.writeYCSBResults( storageFile, FetchYCSBResultsMessage( start, stop ) );
            } catch ( IOException e ) {
                logger.error( e );  //We just log the error and continue for other workers
            }
        }
        return new Properties();
    }


    private void launchWorkers() {
        logger.debug( "Launching Workers... " );

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
            LaunchWorkerMessage workerMessage = ProtoObjectFactory
                    .createYCSBWorkerMessage( job.getEvaluation().getDbms().getHost(), job.getEvaluation().getDbms().getPort(), "", "", "", job.getEvaluation().getOptions().getSystem(), job.getEvaluation().getOptions().getAccessMethod(), job.getEvaluation().getOptions().getYcsbPropertiesMap() );
            worker.launchWorker( workerMessage );
        }

        //Monitor Worker Hardware
        for ( WorkerStub worker : workers ) {
            worker.launchWorkerMonitor( createMonitorWorkerMessage( worker.getHost(), job.getEvaluation().getOptions().getMeasurementOptions() ) );
        }

        //Monitor DB Hardware @ Worker 0
        workers.get( 0 ).launchWorkerMonitor( createMonitorWorkerMessage( job.getEvaluation().getDbms().getHost(), job.getEvaluation().getOptions().getMeasurementOptions() ) );
    }


    @Override
    public void abort() {
        logger.warn( "Aborting YCSB Scenario" );
        for ( WorkerStub worker : workers ) {
            worker.stopWorker( StopWorkerMessage( PolyClientGRPC.Scenario.SCENARIOYCSB ) );
        }
    }
}
