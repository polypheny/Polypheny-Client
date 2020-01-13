package org.polypheny.client.db.musqle;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.access.RESTConnector;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import org.polypheny.client.job.PolyphenyJobCdl;


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
