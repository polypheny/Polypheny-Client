package org.polypheny.client.monitoring;


import com.google.common.util.concurrent.SettableFuture;
import java.io.File;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.grpc.PolyClientGRPC.NetdataMeasurement;
import org.polypheny.client.job.PolyphenyJobCdl;
import org.polypheny.client.rpc.master.WorkerStub;
import org.polypheny.client.storage.StreamWriter;


public class NetdataMonitorAdmin {

    private static final Logger logger = LogManager.getLogger();


    public static void launchMonitors( List<WorkerStub> workers, PolyphenyJobCdl job ) {
        //Monitor Worker Hardware
        for ( WorkerStub worker : workers ) {
            worker.launchWorkerMonitor( job );
        }

        //Monitor DB Hardware @ Worker 0
        workers.get( 0 ).launchDBMonitor( job );
    }


    private static File fileForWorker( File origin, String url ) {
        return new File( origin, "netdata_" + url + ".json" );
    }


    public static void fetchWorkerMonitorResults( File resultsFolder, List<WorkerStub> workers, PolyphenyJobCdl job, long start, long stop ) {

        if ( !resultsFolder.mkdirs() ) {
            logger.trace( "results-Folder not created" );
        }
        for ( WorkerStub workerStub : workers ) {
            StreamWriter<NetdataMeasurement> writer = new StreamWriter<>( SettableFuture.create(), fileForWorker( resultsFolder, workerStub.getHost() ), NetdataMeasurement.class );
            workerStub.writeWorkerMonitorResult( writer, start, stop );
            writer.onCompleted();
        }

        String host = job.getEvaluation().getDbms().getHost();
        StreamWriter<NetdataMeasurement> dbWriter = new StreamWriter<>( SettableFuture.create(), fileForWorker( resultsFolder, host ), NetdataMeasurement.class );
        workers.get( 0 ).writeWorkerMonitorResult( dbWriter, host, start, stop );
        dbWriter.onCompleted();
    }
}
