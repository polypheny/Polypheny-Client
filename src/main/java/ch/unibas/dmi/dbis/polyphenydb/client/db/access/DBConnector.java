package ch.unibas.dmi.dbis.polyphenydb.client.db.access;


import java.io.File;
import java.sql.ResultSet;


/**
 * Our Interface for accessing Databases.
 */
public interface DBConnector {

    void startTransaction();

    void commitTransaction() throws ConnectionException;

    void abortTransaction() throws ConnectionException;

    /**
     * Executes a query, giving back the results.
     *
     * @param query Query-string
     * @return Depends on the {@link DBConnector}. YOU AS THE CALLER OF THE FUNCTION ARE RESPONSIBLE FOR CLOSING THE RESULTSET WITH {@link ResultSet#close()}
     */
    ResultSet executeQuery( String query ) throws ConnectionException;

    /**
     * Execute a Statement. The key difference to {@link #executeQuery(String)} is that you do not expect a result here.
     */
    void executeStatement( String statement ) throws ConnectionException;

    /**
     * Execute a statement from a file, returning no results.
     */
    void executeScript( File file ) throws ConnectionException;

    /**
     * Closes all relevant resources
     */
    void close();
}

