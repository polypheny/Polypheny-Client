package ch.unibas.dmi.dbis.polyphenydb.client.chronos;


import ch.unibas.dmi.dbis.chronos.agent.ChronosJob;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * ProgressListener for setting progress of an evaluation job
 *
 * @author silvan on 24.02.17.
 */
public class ChronosProgressListener implements ProgressListener {

    private final ChronosJob job;
    private final ChronosAgent agent;
    private Logger logger = LogManager.getLogger();


    /**
     * @param job {@link ChronosJob} you want to set the progress of
     * @param agent The agent which manages the job
     */
    ChronosProgressListener( ChronosJob job, ChronosAgent agent ) {
        this.job = job;
        this.agent = agent;
    }


    /**
     * Notifies the {@link ChronosAgent} with the progress of the currently tracked {@link ChronosJob}
     *
     * @param progress Between 0 and 1
     */
    public void setBenchmarkProgress( Double progress ) {
        agent.setProgress( job, progress );
    }


    @Override
    public void reportPopulationProgress( Optional<Double> progress, Optional<String> progressMessage ) {
        logger.debug( "Population progress: {}, message: {}", progress, progressMessage );
    }
}
