package org.polypheny.client.chronos;


import ch.unibas.dmi.dbis.chronos.agent.ChronosJob;
import java.io.File;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.scenarios.Scenario;
import org.polypheny.client.scenarios.ScenarioFactory;


/**
 * Manages one {@link ChronosJob}, including abort() and execute(). Holds the currently executing {@link Scenario}. Also our internal abstraction point for chronos.
 *
 * @author silvan on 24.02.17.
 */
public class ChronosExecutor {

    private final ChronosJob chronosJob;
    private final File inputDir;
    private final File outputDir;
    private final ChronosProgressListener progressListener;
    private Logger logger = LogManager.getLogger( "PolyphenyDB" );
    private Scenario scenario;


    /**
     * parameters directly mirror those of chronos
     *
     * @param progressListener Used to send progress to chronos.
     */
    ChronosExecutor( ChronosJob chronosJob, File inputDir, File outputDir,
            ChronosProgressListener progressListener ) {
        this.chronosJob = chronosJob;
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.progressListener = progressListener;
        this.scenario = ScenarioFactory
                .createScenario( this );
    }


    /**
     * Handles Shutdown of the executor. Aborts the {@link Scenario}
     */
    public void abort() {
        scenario.abort();
        logger.error( "Aborting of a ChronosJob is currently not implemented in the architecture" );
    }


    /**
     * Executes the {@link Scenario}
     *
     * @return output of the {@link Scenario}. Sent back to chronos
     */
    public Properties execute() {
        logger.info( "Starting execution" );
        progressListener.setBenchmarkProgress( 0d );
        Properties result = scenario.execute();
        progressListener.setBenchmarkProgress( 1d );
        System.out.println( "Execution done!" );
        return result;
    }


    public ChronosJob getChronosJob() {
        return chronosJob;
    }


    public File getInputDir() {
        return inputDir;
    }


    public File getOutputDir() {
        return outputDir;
    }


    public ChronosProgressListener getProgressListener() {
        return progressListener;
    }

}
