package org.polypheny.client.db.access;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.ScriptRunner;


/**
 * Our JDBC-Wrapper. AutoCommit is set to false.
 */
public class PolyphenyDbJdbcConnector implements DBConnector {

    private final Statement executeQueryStatement;
    private Logger logger = LogManager.getLogger();
    private Connection conn;


    /**
     * @param dbHost Hostname without JDBC-Prefix (example: postgresql://10.34.58.105)
     * @param sslEnabled boolean lowercase as String (example: "false")
     */
    public PolyphenyDbJdbcConnector( String dbHost, int port, String dbName, String user, String password, String sslEnabled ) throws ConnectionException {
        try {
            Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
        } catch ( ClassNotFoundException e ) {
            logger.error( "Polypheny-DB Driver not found", e );
        }
        String url = "jdbc:polypheny://" + dbHost + ":" + port + "/" + dbName + "?prepareThreshold=0";
        logger.debug( "Connecting to database @ {}", url );

        Properties props = new Properties();
        props.setProperty( "username", user );
        props.setProperty( "password", password );
        //props.setProperty( "ssl", sslEnabled );
        props.setProperty( "serialization", "PROTOBUF" );
        conn = null;
        try {
            conn = DriverManager.getConnection( url, props );
            conn.setAutoCommit( false );

            executeQueryStatement = conn.createStatement();
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        }
    }


    @Override
    public void startTransaction() {
        logger.trace( " You cannot start transactions with JDBC..." );
    }


    @Override
    public void commitTransaction() throws ConnectionException {
        try {
            conn.commit();
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        }
    }


    @Override
    public void abortTransaction() throws ConnectionException {
        try {
            conn.rollback();
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        }
    }


    @Override
    public ResultSet executeQuery( String query ) throws ConnectionException {
        try {
            return executeQueryStatement.executeQuery( query );
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        }
        // We can't close the preparedStatement here since that would also close the associated resultset.
    }


    @Override
    public void executeStatement( String statement ) throws ConnectionException {
        try {
            executeQueryStatement.execute( statement );
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        }
    }


    @Override
    public void executeScript( File file ) throws ConnectionException {
        try {
            ScriptRunner runner = new ScriptRunner( conn, conn.getAutoCommit(), true );
            runner.runScript( new BufferedReader( new FileReader( file.getPath() ) ) );
        } catch ( IOException | SQLException e ) {
            throw new ConnectionException( e );
        }
    }


    @Override
    public void close() {
        if ( conn != null ) {
            try {
                conn.close();
            } catch ( SQLException e ) { /* ignored */}
        }
        if ( executeQueryStatement != null ) {
            try {
                executeQueryStatement.close();
            } catch ( SQLException e ) { /* ignored */}
        }
    }


    public void setAutoCommit( boolean autoCommit ) throws SQLException {
        conn.setAutoCommit( autoCommit );
    }
}
