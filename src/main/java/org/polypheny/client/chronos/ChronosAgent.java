package org.polypheny.client.chronos;


import ch.unibas.dmi.dbis.chronos.agent.AbstractChronosAgent;
import ch.unibas.dmi.dbis.chronos.agent.ChronosJob;
import ch.unibas.dmi.dbis.chronos.agent.ExecutionException;
import java.io.File;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Implements the {@link AbstractChronosAgent}. Entry point to fetch jobs from Chronos. Be aware that you have to be inside the VPN to upload files to chronos.dmi.unibas.ch
 *
 * The properties-object is passed between the phases
 *
 * @author Silvan Heller
 */
public class ChronosAgent extends AbstractChronosAgent {

    /**
     * We currently only support polyphenydb-evaluation
     */
    public static final String SUPPORTED_SYSTEMS = "polyphenydb";
    public static final Logger logger = LogManager.getLogger();

    /**
     * Keep track of running jobs
     */
    private HashMap<Integer, ChronosExecutor> runningJobs = new HashMap<>();


    /**
     * Connects to default port 80
     *
     * @param ipAddressOrHostname IP in format x.x.x.x or qualified hostname
     * @throws UnknownHostException host not known
     */
    public ChronosAgent( String ipAddressOrHostname ) throws UnknownHostException {
        super( ipAddressOrHostname, 80, false, true );
    }


    /**
     * Aborts a job
     *
     * @param abortedJob job the be aborted
     */
    @Override
    protected void aborted( ChronosJob abortedJob ) {
        ChronosExecutor chronosExecutor = runningJobs.get( abortedJob.id );

        if ( chronosExecutor != null ) {
            chronosExecutor.abort();
        }
    }


    @Override
    protected void failed( ChronosJob chronosJob ) {
        logger.error( "Chronos-Job failed! {} ", chronosJob.toString() );
    }


    @Override
    protected String[] getSupportedSystemNames() {
        return new String[]{ ChronosAgent.SUPPORTED_SYSTEMS };
    }


    @Override
    protected Object prepare( ChronosJob chronosJob, File inputDir, File outputDir, Properties properties, Object o ) {
        logger.warn( "No preparation yet for this agent" );
        logger.warn( "You should populate your DB already here?" );
        return o;
    }


    @Override
    protected Object warmUp( ChronosJob chronosJob, File inputDir, File outputDir, Properties properties, Object o ) {
        logger.debug( "skipping warmup" );
        return o;
    }


    @Override
    protected Object execute( ChronosJob chronosJob, File inputDir, File outputDir, Properties properties, Object o ) throws ExecutionException {
        try {
            ChronosProgressListener progressListener = new ChronosProgressListener( chronosJob, this );
            ChronosExecutor executor = new ChronosExecutor( chronosJob, inputDir, outputDir, progressListener );
            runningJobs.put( chronosJob.id, executor );
            Properties result = executor.execute();
            properties.putAll( result );
            logger.debug( "Leaving Executor" );
            runningJobs.remove( chronosJob.id );
            return o;
        } catch ( RuntimeException e ) {
            throw new ExecutionException( e );
        }
    }


    @Override
    protected Object analyze( ChronosJob chronosJob, File inputDir, File outputDir, Properties properties, Object o ) {
        logger.warn( "No analysis step implemented for ChronosAgent" );
        logger.warn( "We should put results in the properties-object / IO folder and then do analysis here" );
        logger.debug( "Not passing any object to clean" );
        return o;
    }


    @Override
    protected Object clean( ChronosJob chronosJob, File inputDir, File outputDir, Properties properties, Object o ) {
        logger.warn( "No cleanup step implemented in chronos agent" );
        return o;
    }


    /**
     * Runs a {@link ChronosExecutor} with a {@link ChronosProgressListener}
     *
     * @return Properties which will be uploaded to chronos
     */
    protected Properties execute( ChronosJob chronosJob, File inputDir, File outputDir ) {
        ChronosProgressListener progressListener = new ChronosProgressListener( chronosJob, this );
        ChronosExecutor executor = new ChronosExecutor( chronosJob, inputDir, outputDir, progressListener );
        runningJobs.put( chronosJob.id, executor );
        Properties results = executor.execute();
        System.out.println( "Left executor" );
        runningJobs.remove( chronosJob.id );
        return results;
    }


    /**
     * Set the progress of a {@link ChronosJob}
     *
     * @param job {@link ChronosJob} you want to set the progress of
     * @param progress percentage between 0 and 1
     */
    public void setProgress( ChronosJob job, double progress ) {
        this.setProgress( job, (byte) Math.floor( progress * 100 ) );
    }
}
