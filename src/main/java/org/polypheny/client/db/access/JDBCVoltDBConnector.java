package org.polypheny.client.db.access;


import ch.unibas.dmi.dbis.polysqlparser.parser.PolySqlParserConfig;


public class JDBCVoltDBConnector extends JDBCConnector {

    private static final String escapeTableCharacter = "\"";
    private static final String escapeColumnCharacter = "\"";
    private static final String escapeAliasCharacter = "";
    private static final String escapeStringCharacter = "'";
    private static final boolean supportsILike = false;
    private static final boolean convertIdentifiersToUppercase = true;

    private static final boolean supportsMultipleConcurrentTransactions = true;


    public JDBCVoltDBConnector( String dbHost, int port, String dbName, String user, String password ) throws ConnectionException {
        super( "jdbc:voltdb://" + dbHost + ":" + port,
                user,
                password,
                "false",
                "org.voltdb.jdbc.Driver",
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
    public void setAutoCommit( boolean autoCommit ) {
        // VoltDb does not support transactions, do nothing
    }


    @Override
    public void commitTransaction() {
        // VoltDb does not support transactions, do nothing
    }


    @Override
    public void abortTransaction() {
        // VoltDb does not support transactions, do nothing
    }


}
