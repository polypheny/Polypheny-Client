package ch.unibas.dmi.dbis.polyphenydb.client.db.access;


import ch.unibas.dmi.dbis.polyphenydb.client.db.ScriptRunner;
import ch.unibas.dmi.dbis.polysqlparser.PolySqlParserException;
import ch.unibas.dmi.dbis.polysqlparser.parser.PolySqlParserConfig;
import ch.unibas.dmi.dbis.polysqlparser.parser.PolySqlParserUtil;
import ch.unibas.dmi.dbis.polysqlparser.statement.Statement;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class JDBCConnector implements DBConnector {

    private static Connection staticConnection;
    private final PolySqlParserConfig polySqlParserConfig;
    private final boolean supportsMultipleConcurrentTransactions;
    ReentrantLock staticConnectionLock = new ReentrantLock();
    private Logger logger = LogManager.getLogger();
    private Connection connection;


    /**
     * @param sslEnabled boolean lowercase as String (example: "false")
     */
    public JDBCConnector( String url, String user, String password, String sslEnabled, String driver, PolySqlParserConfig polySqlParserConfig, boolean supportsMultipleConcurrentTransactions ) throws ConnectionException {
        this.polySqlParserConfig = polySqlParserConfig;
        this.supportsMultipleConcurrentTransactions = supportsMultipleConcurrentTransactions;

        if ( supportsMultipleConcurrentTransactions ) {
            connection = establishConnection( url, user, password, sslEnabled, driver );
            setAutoCommit( false );
        } else {
            if ( staticConnection == null ) {
                staticConnection = establishConnection( url, user, password, sslEnabled, driver );
                setAutoCommit( false );
            }
        }

    }


    private Connection establishConnection( String url, String user, String password, String sslEnabled, String driver ) throws ConnectionException {
        try {
            Class.forName( driver );
        } catch ( ClassNotFoundException e ) {
            logger.error( "Driver not found: " + driver, e );
        }
        logger.trace( "Connecting to database @ {}", url );

        Properties props = new Properties();
        props.setProperty( "user", user );
        props.setProperty( "password", password );
        props.setProperty( "ssl", sslEnabled );
        Connection connection = null;
        try {
            connection = DriverManager.getConnection( url, props );
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        }
        return connection;
    }


    @Override
    public void startTransaction() {
        if ( !supportsMultipleConcurrentTransactions ) {
            staticConnectionLock.lock();
        }
    }


    @Override
    public void commitTransaction() throws ConnectionException {
        Connection conn = getConnection();
        try {
            conn.commit();
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        } finally {
            if ( !supportsMultipleConcurrentTransactions ) {
                staticConnectionLock.unlock();
            }
        }

    }


    @Override
    public void abortTransaction() throws ConnectionException {
        Connection conn = getConnection();
        try {
            conn.rollback();
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        } finally {
            if ( !supportsMultipleConcurrentTransactions ) {
                staticConnectionLock.unlock();
            }
        }

    }


    @Override
    public ResultSet executeQuery( String query ) throws ConnectionException {
        Connection conn = getConnection();
        try {
            Statement statement = PolySqlParserUtil.parse( query );
            return conn.prepareStatement( statement.getEscapedSql( polySqlParserConfig ) ).executeQuery();
        } catch ( SQLException | PolySqlParserException e ) {
            throw new ConnectionException( e );
        }
        // We can't close the preparedStatement here since that would also close the associated resultset.
    }


    @Override
    public void executeStatement( String query ) throws ConnectionException {
        Connection conn = getConnection();
        PreparedStatement preparedStatement = null;
        try {
            Statement statement = PolySqlParserUtil.parse( query );
            preparedStatement = conn.prepareStatement( statement.getEscapedSql( polySqlParserConfig ) );
            preparedStatement.execute();
        } catch ( SQLException | PolySqlParserException e ) {
            throw new ConnectionException( e );
        } finally {
            if ( preparedStatement != null ) {
                try {
                    preparedStatement.close();
                } catch ( SQLException e ) {/* Ignore */}
            }
        }
    }


    @Override
    public void executeScript( File file ) throws ConnectionException {
        Connection conn = getConnection();
        try {
            ScriptRunner runner = new ScriptRunner( conn, conn.getAutoCommit(), true );
            runner.runScript( new BufferedReader( new FileReader( file.getPath() ) ) );
        } catch ( IOException | SQLException e ) {
            throw new ConnectionException( e );
        }
    }


    @Override
    public void close() {
        Connection conn = getConnection();
        if ( conn != null ) {
            try {
                conn.close();
            } catch ( SQLException e ) { /* ignored */}
        }
    }


    public void setAutoCommit( boolean autoCommit ) throws ConnectionException {
        try {
            getConnection().setAutoCommit( autoCommit );
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        }
    }


    protected Connection getConnection() {
        if ( supportsMultipleConcurrentTransactions ) {
            return connection;
        } else {
            return staticConnection;
        }
    }
}
