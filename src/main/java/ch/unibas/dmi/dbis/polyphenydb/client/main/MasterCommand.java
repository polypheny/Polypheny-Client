package ch.unibas.dmi.dbis.polyphenydb.client.main;


import ch.unibas.dmi.dbis.polyphenydb.client.chronos.ChronosAgent;
import com.github.rvesse.airline.annotations.Command;
import java.net.UnknownHostException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Main entry point for our client-master. See the command-description for details and {@link ChronosAgent} for more details.
 */
@Command(
        name = "master",
        description = "Entry-point for the Polypheny-client. Runs on the Master Node. Responsible for contacting chronos, fetching a job and executing it distributed.")
public class MasterCommand extends AbstractCommand {

    private Logger logger = LogManager.getLogger( "PolyphenyDB" );


    public MasterCommand() {
        // make sure that there is a non-arg constructor
    }


    @Override
    public void run() {
        System.out.println( "Hello from Master" );
        try {
            ChronosAgent agent = new ChronosAgent( "chronos.dmi.unibas.ch" );
            agent.run();
        } catch ( UnknownHostException e ) {
            logger.error( e );
        }
    }
}
