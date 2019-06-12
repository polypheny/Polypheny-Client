package ch.unibas.dmi.dbis.polyphenydb.client.analysis.musqle;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.JsonStreamReader;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StorageGson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Main analysis logic in {@link #analyze()}. Contains a {@link #main(String[])} method for analyzing results after the benchmark has already been executed.
 *
 * @author silvan on 26.07.17.
 */
public class MusqleAnalysis {

    public static Logger logger = LogManager.getLogger();
    private final File inputPath;
    private final File outputPath;
    List<MusqleAnalyzer> fullAnalyzers = new ArrayList<>();
    List<MusqleAnalyzer> visualizationAnalyzers = new ArrayList<>();


    public MusqleAnalysis( File inputPath, File outputPath ) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        inputPath.mkdirs();
        outputPath.mkdirs();
    }


    public static void main( String[] args ) {
        MusqleAnalysis analysis = new MusqleAnalysis( new File( "storage/results/tpch/SYSTEMICARUS" ), new File( "output/results/tpch/SYSTEMICARUS" ) );
        analysis.analyze();
    }


    public void analyze() {
        visualizationAnalyzers.add( new AverageTransactionResponse() );
        visualizationAnalyzers.add( new TransactionResponseTimeFull() );

        File storageFile = new File( inputPath, "allresults.json" );
        JsonStreamReader<MUSQLEResultTuple> reader = new JsonStreamReader<>( storageFile, MUSQLEResultTuple.class, StorageGson.getGson() );
        reader.start();

        while ( reader.hasNext() ) {
            for ( MUSQLEResultTuple tuple : reader.readFromStream( 100 ) ) {
                if ( tuple.getAborted() ) {
                    continue;
                }
                visualizationAnalyzers.forEach( musqleAnalyzer -> musqleAnalyzer.process( tuple ) );
                fullAnalyzers.forEach( musqleAnalyzer -> musqleAnalyzer.process( tuple ) );
            }
        }

        JsonObject element = new JsonObject();
        visualizationAnalyzers.forEach( musqleAnalyzer -> {
            element.add( musqleAnalyzer.getClass().getSimpleName(), musqleAnalyzer.getResults() );
        } );

        fullAnalyzers.forEach( musqleAnalyzer -> {
            element.add( musqleAnalyzer.getClass().getSimpleName(), musqleAnalyzer.getResults() );
        } );
        File resultJSON = new File( outputPath, "analysis.json" );
        try {
            JsonWriter writer = new JsonWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( resultJSON ) ), "UTF-8" ) );
            writer.jsonValue( element.toString() );
            writer.close();
        } catch ( IOException e ) {
            logger.error( e );
        }

        logger.trace( element );
    }


    public Properties getProperties() {
        JsonObject element = new JsonObject();
        visualizationAnalyzers.forEach( musqleAnalyzer -> element.add( musqleAnalyzer.getClass().getSimpleName(), musqleAnalyzer.getResults() ) );
        Properties props = new Properties();
        props.put( "results", element );
        return props;
    }
}
