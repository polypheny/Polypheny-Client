package ch.unibas.dmi.dbis.polyphenydb.client.main.analysis;


import ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc.AverageNumberOfQueries;
import ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc.AverageTransactionResponse;
import ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc.ResponseTimePerQuery;
import ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc.ResponseTimePerQueryType;
import ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc.TPCCAnalyzer;
import ch.unibas.dmi.dbis.polyphenydb.client.analysis.tpcc.TransactionResponseTimeFull;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCTransactionType;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.JsonStreamReader;
import ch.unibas.dmi.dbis.polyphenydb.client.storage.StorageGson;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
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


/**
 * Main analysis logic in {@link #analyze()}. Used for post-processing of all results
 */
@Command(name = "analysis-tpcc", description = "Analyze the final, aggregated, TPC-C results.")
public class TPCCAnalysis implements Runnable {

    public static Logger logger = LogManager.getLogger();
    private File inputPath;
    private File outputPath;
    private int totalExecutionTime;
    private List<TPCCAnalyzer> fullAnalyzers = new ArrayList<>();
    private List<TPCCAnalyzer> visualizationAnalyzers = new ArrayList<>();

    @Option(title = "Input Folder", name = { "--input" }, description = "Folder where the results are located")
    @Required
    private String input;

    @Option(title = "Output Folder", name = { "--output" }, description = "Where results should be written to. Defaults to input folder.")
    private String output;

    @Option(title = "Execution Time", name = { "--time" }, description = "How long the benchmark ran for")
    private int executionTime = 60_000;


    public TPCCAnalysis( File inputPath, File outputPath, int totalExecutionTime ) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.totalExecutionTime = totalExecutionTime;

    }


    //No-args constructor for command
    public TPCCAnalysis() {
    }


    @Override
    public void run() {
        if ( output == null ) {
            output = input;
        }
        TPCCAnalysis analysis = new TPCCAnalysis( new File( input ), new File( output ), executionTime );
        analysis.analyze();
    }


    public void analyze() {
        inputPath.mkdirs();
        outputPath.mkdirs();
        int newOrderCount = 0;

        visualizationAnalyzers.add( new AverageTransactionResponse() );
        fullAnalyzers.add( new AverageNumberOfQueries() );
        fullAnalyzers.add( new ResponseTimePerQuery() );
        visualizationAnalyzers.add( new ResponseTimePerQueryType() );
        visualizationAnalyzers.add( new TransactionResponseTimeFull() );

        File storageFile = new File( getInputPath(), "allresults.json" );
        JsonStreamReader<TPCCResultTuple> reader = new JsonStreamReader<>( storageFile, TPCCResultTuple.class, StorageGson.getGson() );
        reader.start();

        while ( reader.hasNext() ) {
            //Handle each tuple
            for ( TPCCResultTuple tuple : reader.readFromStream( 100 ) ) {
                if ( tuple.getAborted() ) {
                    continue;
                }
                fullAnalyzers.forEach( tpccAnalyzer -> tpccAnalyzer.process( tuple ) );
                visualizationAnalyzers.forEach( tpccAnalyzer -> tpccAnalyzer.process( tuple ) );

                if ( tuple.getTransactionType().equals( TPCCTransactionType.TPCCTRANSACTIONNEWORDER ) ) {
                    newOrderCount++;
                }
            }
        }

        int elapsedTimeInMinutes = totalExecutionTime / 60_000;
        logger.info( "tmpC {}", newOrderCount / elapsedTimeInMinutes );

        JsonObject element = new JsonObject();
        fullAnalyzers.forEach( tpccAnalyzer -> element.add( tpccAnalyzer.getClass().getSimpleName(), tpccAnalyzer.getResults() ) );
        visualizationAnalyzers.forEach( tpccAnalyzer -> element.add( tpccAnalyzer.getClass().getSimpleName(), tpccAnalyzer.getResults() ) );

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


    private File getInputPath() {
        return inputPath;
    }


    public Properties getProperties() {
        JsonObject element = new JsonObject();
        visualizationAnalyzers.forEach( tpccAnalyzer -> element.add( tpccAnalyzer.getClass().getSimpleName(), tpccAnalyzer.getResults() ) );
        Properties props = new Properties();
        props.put( "results", element );
        return props;
    }

}
