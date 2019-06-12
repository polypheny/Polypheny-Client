package ch.unibas.dmi.dbis.polyphenydb.client.rpc.worker;


import ch.unibas.dmi.dbis.polyphenydb.client.grpc.ClientWorkerGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * RPC Server for the ClientWorkerImpl. Intended to be launched on the workers. Uses the GRPC-Implementation {@link ClientWorkerImpl} for the service {@link ClientWorkerGrpc}.
 *
 * @author Silvan Heller
 */
public class ClientWorkerServer {

    private static final Logger LOGGER = LogManager.getLogger();

    private int port;
    private Server server;


    /**
     * @param port Port the server should listen on
     */
    public ClientWorkerServer( int port ) {
        this( ServerBuilder.forPort( port ), port );
    }


    /**
     * Internal Constructor. You should not have to specify your own server builder. Use {@link ClientWorkerServer#ClientWorkerServer(int)} instead.
     */
    private ClientWorkerServer( ServerBuilder<?> serverBuilder, int port ) {
        this.port = port;
        server = serverBuilder.addService( new ClientWorkerImpl() ).build();
    }


    /**
     * Start the service.
     *
     * @throws IOException for runtime errors of the server
     */
    public void start() throws IOException {
        server.start();
        LOGGER.info( "Server started, listening on {}", port );
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            // Use stderr here since the logger may has been reset by its JVM shutdown hook.
            System.err.println( "*** shutting down gRPC server since JVM is shutting down" );
            ClientWorkerServer.this.stop();
            System.err.println( "*** server shut down" );
        } ) );
    }


    /**
     * Stop serving requests and shutdown resources.
     */
    public void stop() {
        if ( server != null ) {
            server.shutdown();
        }
    }


    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if ( server != null ) {
            server.awaitTermination();
        }
    }
}
