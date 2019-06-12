package ch.unibas.dmi.dbis.polyphenydb.client.db.access;


import java.sql.SQLException;


/**
 * Used for all Exceptions which are related to DB-Errors
 *
 * @author silvan on 27.03.17.
 */
public class ConnectionException extends SQLException {

    ConnectionException( Exception e ) {
        super( e );
    }


    ConnectionException( String errorMessage ) {
        super( errorMessage );
    }
}
