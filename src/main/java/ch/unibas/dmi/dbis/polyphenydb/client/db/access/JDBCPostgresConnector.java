package ch.unibas.dmi.dbis.polyphenydb.client.db.access;


import ch.unibas.dmi.dbis.polysqlparser.parser.PolySqlParserConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class JDBCPostgresConnector extends JDBCConnector {

    private static final String escapeTableCharacter = "\"";
    private static final String escapeColumnCharacter = "\"";
    private static final String escapeAliasCharacter = "\"";
    private static final String escapeStringCharacter = "'";
    private static final boolean supportsILike = true;
    private static final boolean convertIdentifiersToUppercase = false;

    private static final boolean supportsMultipleConcurrentTransactions = true;


    public JDBCPostgresConnector( String dbHost, int port, String dbName, String user, String password ) throws ConnectionException {
        super( "jdbc:postgresql://" + dbHost + ":" + port + "/" + dbName,
                user,
                password,
                "false",
                "org.postgresql.Driver",
                new PolySqlParserConfig(
                        escapeTableCharacter,
                        escapeColumnCharacter,
                        escapeStringCharacter,
                        escapeAliasCharacter,
                        supportsILike,
                        convertIdentifiersToUppercase
                ),
                supportsMultipleConcurrentTransactions
        );
    }


    @Override
    public ResultSet executeQuery( String query ) throws ConnectionException {
        try {
            Connection conn = getConnection();
            return conn.prepareStatement( query ).executeQuery();
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        }
        // We can't close the preparedStatement here since that would also close the associated resultset.
    }


    @Override
    public void executeStatement( String statement ) throws ConnectionException {
        PreparedStatement preparedStatement = null;
        try {
            Connection conn = getConnection();
            preparedStatement = conn.prepareStatement( statement );
            preparedStatement.execute();
        } catch ( SQLException e ) {
            throw new ConnectionException( e );
        } finally {
            if ( preparedStatement != null ) {
                try {
                    preparedStatement.close();
                } catch ( SQLException e ) {/* Ignore */}
            }
        }
    }

}
