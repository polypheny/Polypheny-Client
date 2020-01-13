package org.polypheny.client.chronos;


import java.util.Optional;


/**
 * Simple Progress-Listener Interface which allows us to switch a {@link ChronosProgressListener} and {@link DummyProgressListener} if we're not using chronos
 *
 * @author silvan on 25.07.17.
 */
public interface ProgressListener {

    /**
     * @param progress Between 0 and 1
     */
    void setBenchmarkProgress( Double progress );

    void reportPopulationProgress( Optional<Double> progress, Optional<String> progressMessage );

}
