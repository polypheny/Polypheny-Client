package org.polypheny.client.db.musqle;


import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.MusqleTransactionType;
import org.polypheny.client.rpc.ProtoObjectFactory;


/**
 * This is the interface that any system to be benchmarked should implement.
 *
 * @author Marco Vogt
 */
public abstract class MusqleBenchmarker {

    // scale factor (SF) must be either 1, 10, 30, 100, 300, 1000, 3000, 10000, 30000 or 100000
    private double SCALE_FACTOR;
    private Logger logger = LogManager.getLogger();


    public MusqleBenchmarker( final double SCALE_FACTOR ) {
        this.SCALE_FACTOR = SCALE_FACTOR;
    }


    /**
     * Aborts the currently running benchmark, closing all associated resources. Must not throw exceptions.
     */
    public abstract void abort();


    /**
     * Fire Query against Database.
     */
    public abstract MUSQLEResultTuple genericQueryExecutor( final int queryID );


    public MusqleTransactionType getTypeForQueryID( int queryID ) {
        switch ( queryID ) {
            case 1:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q01;
            case 2:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q02;
            case 3:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q03;
            case 4:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q04;
            case 5:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q05;
            case 6:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q06;
            case 7:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q07;
            case 8:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q08;
            case 9:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q09;
            case 10:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q10;
            case 11:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q11;
            case 12:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q12;
            case 13:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q13;
            case 14:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q14;
            case 15:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q15;
            case 16:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q16;
            case 17:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q17;
            case 18:
                return MusqleTransactionType.MUSQLE_TRANSACTION_Q18;
            default:
                return MusqleTransactionType.MUSQLE_TRANSACTION_UNDEFINED;
        }
    }


    public MUSQLEResultTuple genericQueryExecutor( int queryID, DBConnector connector, String query ) {
        MusqleTransactionType transactionType = getTypeForQueryID( queryID );

        long start;
        long stop;
        logger.trace( "Starting Q" + queryID + " transaction" );
        start = System.currentTimeMillis();
        connector.startTransaction();
        try ( ResultSet resultSet = connector.executeQuery( query ) ) {
            if ( !resultSet.next() ) {
                logger.error( "Could not move resultset for query {}", query );
            }
            connector.commitTransaction();
            stop = System.currentTimeMillis();
            logger.trace( "Finished Q" + queryID + " transaction. Elapsed time: {} ms", (stop - start) );
        } catch ( SQLException e ) {
            //We just log exceptions. For example Q11 throws an expected exception for VoltDB
            logger.error( "ConnectionException while Q" + queryID + " transaction. Logging and continuing. \n" );
            try {
                connector.abortTransaction();
            } catch ( ConnectionException ignored ) {
                //Ignore
            }
            logger.debug( "Query = {} \n \n", query );
            if ( e.getMessage().length() > 1000 ) {
                logger.error( cropErrorMessage( e.getMessage() ) );
            } else {
                logger.error( e );
            }
            return ProtoObjectFactory.MusqleResultTuple( start, (System.currentTimeMillis() - start), transactionType, queryID, true, query );
            //abort();
            //throw new RuntimeException( e );
        }
        logger.trace( "Finished Q{} with text {}", queryID, query );
        return ProtoObjectFactory.MusqleResultTuple( start, (stop - start), transactionType, queryID, false, query );
    }


    public String cropErrorMessage( String str ) {
        if ( str != null && str.length() > 1000 ) {
            String first = str.substring( 0, 500 );
            String last = str.substring( str.length() - 500, str.length() );
            str = "Cropped error message: " + first + "    ...    " + last;
        }
        return str;
    }
}
