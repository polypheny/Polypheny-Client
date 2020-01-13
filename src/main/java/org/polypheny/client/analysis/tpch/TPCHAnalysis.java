package org.polypheny.client.analysis.tpch;


import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;
import org.polypheny.client.storage.JsonStreamReader;
import org.polypheny.client.storage.StorageGson;


/**
 * Main analysis logic in {@link #analyze()}. Contains a {@link #main(String[])} method for analyzing results after the benchmark has already been executed.
 *
 * @author silvan on 26.07.17.
 */
public class TPCHAnalysis {

    public static Logger logger = LogManager.getLogger();
    private final File inputPath;
    private final File outputPath;
    List<TPCHAnalyzer> fullAnalyzers = new ArrayList<>();
    List<TPCHAnalyzer> visualizationAnalyzers = new ArrayList<>();


    public TPCHAnalysis( File inputPath, File outputPath ) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        inputPath.mkdirs();
        outputPath.mkdirs();
    }


    public static void main( String[] args ) {
        TPCHAnalysis analysis = new TPCHAnalysis( new File( "storage/results/tpch/SYSTEMICARUS" ), new File( "output/results/tpch/SYSTEMICARUS" ) );
        analysis.analyze();
    }


    public void analyze() {
        visualizationAnalyzers.add( new AverageTransactionResponse() );
        visualizationAnalyzers.add( new TransactionResponseTimeFull() );

        File storageFile = new File( inputPath, "allresults.json" );
        JsonStreamReader<TPCHResultTuple> reader = new JsonStreamReader<>( storageFile, TPCHResultTuple.class, StorageGson.getGson() );
        reader.start();

        while ( reader.hasNext() ) {
            for ( TPCHResultTuple tuple : reader.readFromStream( 100 ) ) {
                if ( tuple.getAborted() ) {
                    continue;
                }
                visualizationAnalyzers.forEach( tpchAnalyzer -> tpchAnalyzer.process( tuple ) );
                fullAnalyzers.forEach( tpchAnalyzer -> tpchAnalyzer.process( tuple ) );
            }
        }

        JsonObject element = new JsonObject();
        visualizationAnalyzers.forEach( tpccAnalyzer -> element.add( tpccAnalyzer.getClass().getSimpleName(), tpccAnalyzer.getResults() ) );

        fullAnalyzers.forEach( tpccAnalyzer -> element.add( tpccAnalyzer.getClass().getSimpleName(), tpccAnalyzer.getResults() ) );
        File resultJSON = new File( outputPath, "analysis.json" );
        try {
            JsonWriter writer = new JsonWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( resultJSON ) ), StandardCharsets.UTF_8 ) );
            writer.jsonValue( element.toString() );
            writer.close();
        } catch ( IOException e ) {
            logger.error( e );
        }

        logger.trace( element );
    }


    public Properties getProperties() {
        JsonObject element = new JsonObject();
        visualizationAnalyzers.forEach( tpchAnalyzer -> element.add( tpchAnalyzer.getClass().getSimpleName(), tpchAnalyzer.getResults() ) );
        Properties props = new Properties();
        props.put( "results", element );
        return props;
    }
}
