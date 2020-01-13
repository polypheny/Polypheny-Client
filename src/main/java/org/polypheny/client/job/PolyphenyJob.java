package org.polypheny.client.job;


import ch.unibas.dmi.dbis.chronos.agent.ChronosJob;
import java.io.File;
import java.io.StringReader;
import java.util.Collections;
import java.util.NoSuchElementException;
import javax.xml.bind.JAXB;
import org.polypheny.client.chronos.ChronosAgent;
import org.polypheny.client.chronos.ChronosExecutor;


/**
 * Our Abstraction for a {@link ChronosJob}. Also stores the {@link PolyphenyJobCdl} which contains parsed XML. <p>
 *
 * @author Silvan Heller
 */
public class PolyphenyJob extends ChronosJob {

    private PolyphenyJobCdl cdl;
    private ChronosExecutor executor;


    /**
     * Also parses the XML of the {@link ChronosJob} into a {@link PolyphenyJobCdl}
     *
     * @param executor {@link ChronosExecutor}
     */
    public PolyphenyJob( ChronosExecutor executor ) {
        super( executor.getChronosJob() );
        this.executor = executor;
        this.cdl = JAXB
                .unmarshal( new StringReader( executor.getChronosJob().cdl ), PolyphenyJobCdl.class );
        checkCdl( this.cdl );
    }


    /**
     * Verify the integrity of a {@link PolyphenyJobCdl}
     *
     * @param cdl the {@link PolyphenyJobCdl} to be verified
     * @throws IllegalArgumentException if the {@link PolyphenyJobCdl} is invalid
     */
    private static void checkCdl( final PolyphenyJobCdl cdl ) throws IllegalArgumentException {
        try {
            cdl.getEvaluation().getSetup();
            if ( cdl.getEvaluation().getChronosSystem() == null
                    || !Collections.singletonList( ChronosAgent.SUPPORTED_SYSTEMS )
                    .contains( cdl.getEvaluation().getChronosSystem() ) ) {
                throw new IllegalArgumentException(
                        "evaluation.system is either null or not equal to \"" + ChronosAgent.SUPPORTED_SYSTEMS
                                + "\". It is: \"" + cdl.getEvaluation().getChronosSystem() + "\"" );
            }
        } catch ( NoSuchElementException ex ) {
            throw new IllegalArgumentException( ex );
        }
    }


    /**
     * @return the {@link PolyphenyJobCdl} of this job
     */
    public PolyphenyJobCdl getCdl() {
        return cdl;
    }


    /**
     * @return Input Directory of the {@link ChronosExecutor}
     */
    public File getInputDir() {
        return executor.getInputDir();
    }


    public File getOutputDir() {
        return executor.getOutputDir();
    }


    public ChronosExecutor getExecutor() {
        return executor;
    }
}
