package ch.unibas.dmi.dbis.polyphenydb.client.chronos;


import java.util.Optional;
import org.apache.commons.math3.util.Precision;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Dummy-progresslistener which just logs the progress as it receives it
 *
 * @author silvan on 25.07.17.
 */
public class DummyProgressListener implements ProgressListener {

    private static final Logger logger = LogManager.getLogger();


    @Override
    public void setBenchmarkProgress( Double progress ) {
        logger.info( "Progress {}", Precision.round( progress, 2 ) );
    }


    @Override
    public void reportPopulationProgress( Optional<Double> progress, Optional<String> progressMessage ) {
        logger.info( "Progress during population: {}, progress Message: {}", Precision.round( progress.orElse( 0d ), 2 ), progressMessage );
    }
}
