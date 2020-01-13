package org.polypheny.client.scenarios.ycsb;


import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import java.io.File;
import java.io.OutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.config.Config;
import org.polypheny.client.grpc.PolyClientGRPC.YCSBResultTuple;
import org.polypheny.client.storage.StreamWriter;


/**
 * Custom YCSBMeasurementsExporter which writes to a JSON-File.
 *
 * @author silvan on 13.07.17.
 */
public class YCSBMeasurementsExporter implements MeasurementsExporter {

    private static StreamWriter<YCSBResultTuple> resultWriter;

    private static Logger logger = LogManager.getLogger();


    static {
        File storageFolder = new File( Config.DEFAULT_WORKER_STORAGE_LOCATION, "ycsb" );
        storageFolder.mkdirs();
        File storage = new File( storageFolder, "measurements.json" );
        resultWriter = new StreamWriter<>( SettableFuture.create(), storage, YCSBResultTuple.class );
    }


    public YCSBMeasurementsExporter( OutputStream os ) {
        logger.info( "OuputStream {}", os.toString() );
    }


    @Override
    public void write( String metric, String measurement, int i ) {
        resultWriter.onNext(
                YCSBResultTuple.newBuilder().setMetricName( metric ).setMeasurementName( measurement )
                        .setMeasurement( i ).build() );
    }


    @Override
    public void write( String metric, String measurement, double d ) {
        resultWriter.onNext(
                YCSBResultTuple.newBuilder().setMetricName( metric ).setMeasurementName( measurement )
                        .setMeasurement( d ).build() );
    }


    @Override
    public void close() {
        resultWriter.onCompleted();
    }
}
