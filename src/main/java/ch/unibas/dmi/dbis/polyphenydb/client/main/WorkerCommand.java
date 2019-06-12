package ch.unibas.dmi.dbis.polyphenydb.client.main;


import ch.unibas.dmi.dbis.polyphenydb.client.config.Config;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.worker.ClientWorkerImpl;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.worker.ClientWorkerServer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * See the description of the command. For everything implementation-related, see {@link ClientWorkerImpl}.
 */
@Command(name = "worker",
        description = "Entrypoint for the distributed Workers. Runs an RPC Thread which is responsible for taking orders from master. You do not specify a master URL as ")
public class WorkerCommand extends AbstractCommand {

    /**
     * Not initialized as we want to give the logfiles a specific name depending on the command which is run.
     */
    private Logger logger;

    @Option(title = "Port", name = { "-p", "--port" }, description = "The port on which the worker listens for tasks.")
    private int port = Config.DEFAULT_WORKER_PORT;

    private ClientWorkerServer server;


    public WorkerCommand() {
        // make sure that there is a non-arg constructor
    }


    @Override
    public void run() {
        try {
            System.setProperty( "logFilename", "worker" );
            org.apache.logging.log4j.core.LoggerContext ctx = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext( false );
            ctx.reconfigure();
            logger = LogManager.getLogger();
            logger.info( "Launching ClientWorker on port {}", port );
            server = new ClientWorkerServer( port );
            server.start();
            server.blockUntilShutdown();
        } catch ( Exception e ) {
            //Try to not arrive here :)
            logger.error( "Fatal error, exiting." );
            e.printStackTrace();
        }
    }


    public ClientWorkerServer getServer() {
        return server;
    }
}
