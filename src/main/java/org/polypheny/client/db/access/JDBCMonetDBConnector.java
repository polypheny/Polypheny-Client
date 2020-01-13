package org.polypheny.client.db.access;


import ch.unibas.dmi.dbis.polysqlparser.parser.PolySqlParserConfig;


public class JDBCMonetDBConnector extends JDBCConnector {

    private static final String escapeTableCharacter = "\"";
    private static final String escapeColumnCharacter = "\"";
    private static final String escapeAliasCharacter = "\"";
    private static final String escapeStringCharacter = "'";
    private static final boolean supportsILike = true;
    private static final boolean convertIdentifiersToUppercase = false;

    private static final boolean supportsMultipleConcurrentTransactions = true;


    public JDBCMonetDBConnector( String dbHost, int port, String dbName, String user, String password ) throws ConnectionException {
        super( "jdbc:monetdb://" + dbHost + ":" + port + "/" + dbName,
                user,
                password,
                "false",
                "nl.cwi.monetdb.jdbc.MonetDriver",
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


}
