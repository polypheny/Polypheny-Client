package ch.unibas.dmi.dbis.polyphenydb.client.db.musqle;


import ch.unibas.dmi.dbis.polyphenydb.client.db.access.ConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.DBConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.RESTConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import ch.unibas.dmi.dbis.polyphenydb.client.job.PolyphenyJobCdl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Marco Vogt
 */
public class IcarusMusqleBenchmarker extends MusqleBenchmarker {

    private final static Logger logger = LogManager.getLogger();
    private final double SCALE_FACTOR;
    private DBConnector connector;


    public IcarusMusqleBenchmarker( PolyphenyJobCdl cdl ) {
        this( cdl.getEvaluation().getOptions().getTpchScalefactor(), cdl.getEvaluation().getDbms().getHost(), cdl.getEvaluation().getDbms().getPort() );
    }


    public IcarusMusqleBenchmarker( LaunchWorkerMessage workerMessage ) {
        this( workerMessage.getTpchWorkerMessage().getSCALEFACTOR(), workerMessage.getDbInfo().getDbHost(), workerMessage.getDbInfo().getDbPort() );
    }


    public IcarusMusqleBenchmarker( double SCALE_FACTOR, String host, int port ) {
        super( SCALE_FACTOR );
        this.SCALE_FACTOR = SCALE_FACTOR;
        this.connector = new RESTConnector( host, port );
    }


    @Override
    public void abort() {
        try {
            connector.abortTransaction();
        } catch ( ConnectionException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public MUSQLEResultTuple genericQueryExecutor( int queryID ) {
        logger.trace( "Executing Query {}", queryID );
        String query = QGenIcarus.createQuery( SCALE_FACTOR, queryID );
        return super.genericQueryExecutor( queryID, connector, query );
    }

}
