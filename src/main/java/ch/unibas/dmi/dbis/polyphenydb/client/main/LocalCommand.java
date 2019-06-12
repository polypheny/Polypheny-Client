package ch.unibas.dmi.dbis.polyphenydb.client.main;


import ch.unibas.dmi.dbis.polyphenydb.client.job.PolyphenyJobCdl;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Scenario;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.ScenarioFactory;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import javax.xml.bind.JAXB;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@Command(name = "local", description = "For testing without chronos. Executes against a specified Job-XML.")
public class LocalCommand extends AbstractCommand {

    @Option(title = "Populate Database", name = { "--populate" }, description = "Use this flag if you want to initalize and fill the Database tables as well")
    private boolean populate;

    @Option(title = "Properties", name = { "--properties" }, description = "Relative path to the file which contains the Chronos Job-XML.")
    private String propertiesFile;

    /**
     * Not initialized so we can give the logfile a name
     */
    private Logger logger;


    public LocalCommand() {
        // make sure that there is a non-arg constructor
    }


    @Override
    public void run() {
        System.setProperty( "logFilename", "local" );
        org.apache.logging.log4j.core.LoggerContext ctx = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext( false );
        ctx.reconfigure();
        logger = LogManager.getLogger();
        PolyphenyJobCdl cdl = new PolyphenyJobCdl();
        logger.info( "Starting master without contacting chronos" );
        if ( propertiesFile != null ) {
            try {
                logger.info( "Using xml located @ {}", propertiesFile );
                cdl = JAXB.unmarshal( new StringReader( FileUtils.readFileToString( new File( propertiesFile ), Charset.defaultCharset() ) ), PolyphenyJobCdl.class );
                logger.debug( cdl.toString() );
            } catch ( IOException e ) {
                logger.error( e );
                System.exit( 1 );
            }
        }
        if ( populate ) {
            cdl.getEvaluation().getSetup().setPopulate( populate );
            logger.info( "Starting Local execution and populating Database" );
        }
        Scenario scenario = ScenarioFactory.createScenario( cdl );
        scenario.execute();
    }
}
