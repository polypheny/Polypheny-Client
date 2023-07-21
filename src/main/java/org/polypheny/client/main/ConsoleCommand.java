/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.polypheny.client.main;


import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.io.Console;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
@Command(name = "console", description = "Opens a console to query a Polypheny-DB instance.")
public class ConsoleCommand extends AbstractCommand {


    private static Logger log;

    // Weather to print lines between the rows in a result set
    private static final boolean PRINT_INNER_LINES = false;

    @Arguments(title = { "hostname" }, description = "Polypheny-DB hostname or IP address")
    private String hostname = "localhost";

    @Option(title = "Username", name = { "-u", "--username" })
    private String username = "pa";


    public void run() {
        log = LoggerFactory.getLogger( ConsoleCommand.class );
        final StopWatch stopWatch = new StopWatch();

        final PrintWriter writer;
        final PrintWriter errorWriter;
        final Scanner reader;
        final Console c = System.console();
        if ( c == null ) {
            reader = new Scanner( System.in );
            writer = new PrintWriter( System.out );
            errorWriter = new PrintWriter( System.err );
            if ( verbose ) {
                writer.println( "*** Using System.out ***" );
                errorWriter.println( "*** Using System.err ***" );
            }
        } else {
            reader = new Scanner( c.reader() );
            writer = new PrintWriter( c.writer() );
            errorWriter = new PrintWriter( c.writer() );
            if ( verbose ) {
                writer.println( "*** Using System.console() ***" );
                errorWriter.println( "*** Using System.console() ***" );
            }
        }

        try {
            Class.forName( "org.polypheny.jdbc.Driver" );
            //Class.forName( "org.apache.calcite.avatica.remote.Driver" );
        } catch ( ClassNotFoundException e ) {
            if ( log.isErrorEnabled() ) {
                log.error( "Could not load Polypheny-DB's driver", e );
            }
            writer.println( "Driver not found. Terminating ..." );
            return;
        }

        final Properties connectionProperties = new Properties();
        connectionProperties.setProperty( "user", "pa" );
        connectionProperties.setProperty( "serialization", "PROTOBUF" );
        final String url = "jdbc:polypheny:http://localhost:20591";
        try ( final Connection connection = DriverManager.getConnection( url, connectionProperties ) ) {
            connection.setAutoCommit( false );
            try ( final Statement statement = connection.createStatement() ) {

                String line = "";

                writer.print( "> " );
                writer.flush();

                while ( (line = reader.nextLine()) != null ) {
                    if ( line.isEmpty() ) {
                        writer.print( "> " );
                        writer.flush();
                        continue;
                    }

                    if ( line.endsWith( ";" ) ) {
                        line = line.substring( 0, line.length() - 1 );
                    }

                    stopWatch.start();
                    if ( line.equalsIgnoreCase( "exit" ) ) {
                        break;
                    } else if ( line.toLowerCase().startsWith( "set" ) || line.toLowerCase().startsWith( "get" ) ) {
                        if ( line.toLowerCase().startsWith( "get fetchsize" ) || line.toLowerCase().startsWith( "get fetch size" ) ) {
                            writer.println( "FetchSize = " + statement.getFetchSize() );
                        } else if ( line.toLowerCase().startsWith( "set fetchsize" ) || line.toLowerCase().startsWith( "set fetch size" ) ) {
                            if ( line.contains( "=" ) ) {
                                statement.setFetchSize( Integer.parseInt( line.substring( line.indexOf( '=' ) + 1 ).trim() ) );
                                writer.println( "FetchSize is now: " + statement.getFetchSize() );
                            } else {
                                writer.println( "Missing '='" );
                            }
                        } else if ( line.toLowerCase().startsWith( "get autocommit" ) || line.toLowerCase().startsWith( "get auto commit" ) ) {
                            writer.println( "AutoCommit = " + connection.getAutoCommit() );
                        } else if ( line.toLowerCase().startsWith( "set autocommit" ) || line.toLowerCase().startsWith( "set auto commit" ) ) {
                            if ( line.contains( "=" ) ) {
                                connection.setAutoCommit( Boolean.parseBoolean( line.substring( line.indexOf( '=' ) + 1 ).trim() ) );
                                writer.println( "AutoCommit is now: " + connection.getAutoCommit() );
                            } else {
                                writer.println( "Missing '='" );
                            }
                        }
                    } else if ( line.startsWith( "!" ) ) {
                        if ( line.toLowerCase().startsWith( "!databases" ) ) {
                            ResultSet rs = connection.getMetaData().getCatalogs();
                            writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                            rs.close();
                        } else if ( line.toLowerCase().startsWith( "!schemas" ) ) {
                            final String[] params = line.split( " " );
                            ResultSet rs;
                            if ( params.length == 2 ) {
                                rs = connection.getMetaData().getSchemas( null, params[1] );
                            } else {
                                rs = connection.getMetaData().getSchemas( null, null );
                            }
                            writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                            rs.close();
                        } else if ( line.toLowerCase().startsWith( "!tables" ) ) {
                            final String[] params = line.split( " " );
                            ResultSet rs;
                            if ( params.length == 2 ) {
                                rs = connection.getMetaData().getTables( null, params[1], "%", null );
                            } else if ( params.length == 3 ) {
                                rs = connection.getMetaData().getTables( null, params[1], params[2], null );
                            } else {
                                rs = connection.getMetaData().getTables( null, null, "%", null );
                            }
                            writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                            rs.close();
                        } else if ( line.toLowerCase().startsWith( "!columns" ) ) {
                            final String[] params = line.split( " " );
                            ResultSet rs;
                            if ( params.length == 2 ) {
                                rs = connection.getMetaData().getColumns( null, params[1], "%", null );
                            } else if ( params.length == 3 ) {
                                rs = connection.getMetaData().getColumns( null, params[1], params[2], null );
                            } else {
                                rs = connection.getMetaData().getColumns( null, null, "%", null );
                            }
                            writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                            rs.close();
                        } else if ( line.toLowerCase().startsWith( "!tabletypes" ) ) {
                            ResultSet rs = connection.getMetaData().getTableTypes();
                            writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                            rs.close();
                        } else if ( line.toLowerCase().startsWith( "!types" ) ) {
                            ResultSet rs = connection.getMetaData().getTypeInfo();
                            writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                            rs.close();
                        } else if ( line.toLowerCase().startsWith( "!primarykey" ) ) {
                            final String[] params = line.split( " " );
                            if ( params.length == 4 ) {
                                ResultSet rs;
                                rs = connection.getMetaData().getPrimaryKeys( params[1], params[2], params[3] );
                                writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                                rs.close();
                            } else {
                                writer.println( "You need to specify the name of the table: [ > !primarykey DATABASE_NAME SCHEMA_NAME TABLE_NAME ]" );
                            }
                        } else if ( line.toLowerCase().startsWith( "!importedkeys" ) ) {
                            final String[] params = line.split( " " );
                            if ( params.length == 4 ) {
                                ResultSet rs;
                                rs = connection.getMetaData().getImportedKeys( params[1], params[2], params[3] );
                                writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                                rs.close();
                            } else {
                                writer.println( "You need to specify the name of the table: [ > !importedkeys DATABASE_NAME SCHEMA_NAME TABLE_NAME ]" );
                            }
                        } else if ( line.toLowerCase().startsWith( "!exportedkeys" ) ) {
                            final String[] params = line.split( " " );
                            if ( params.length == 4 ) {
                                ResultSet rs;
                                rs = connection.getMetaData().getExportedKeys( params[1], params[2], params[3] );
                                writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                                rs.close();
                            } else {
                                writer.println( "You need to specify the name of the table: [ > !exportedkeys DATABASE_NAME SCHEMA_NAME TABLE_NAME ]" );
                            }
                        } else if ( line.toLowerCase().startsWith( "!indexinfo" ) ) {
                            final String[] params = line.split( " " );
                            if ( params.length == 5 ) {
                                ResultSet rs;
                                rs = connection.getMetaData().getIndexInfo( params[1], params[2], params[3], Boolean.parseBoolean( params[4] ), false );
                                writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                                rs.close();
                            } else {
                                writer.println( "You need to specify the name of the table: [ > !getIndexInfo DATABASE_NAME SCHEMA_NAME TABLE_NAME UNIQUE ]" );
                            }
                        } else if ( line.toLowerCase().startsWith( "!commit" ) ) {
                            connection.commit();
                            writer.println( "Successfully committed changes!" );
                        } else if ( line.toLowerCase().startsWith( "!rollback" ) ) {
                            connection.rollback();
                            writer.println( "Successfully rollbacked changes!" );
                        } else {
                            writer.println( "Unknown command: " + line.toLowerCase() );
                        }
                    } else if ( line.toLowerCase().startsWith( "add to batch : " ) ) {
                        final String sql = line.substring( line.indexOf( ':' ) + 1 ).trim();
                        statement.addBatch( sql );
                    } else if ( line.toLowerCase().startsWith( "execute batch" ) ) {
                        stopWatch.start();
                        final int[] result = statement.executeBatch();
                        stopWatch.stop();
                        writer.println( "NUMBER OF ROWS AFFECTED: " + Arrays.toString( result ) );
                    } else {
                        try {
                            if ( statement.execute( line ) ) {
                                stopWatch.stop();
                                final ResultSet rs = statement.getResultSet();

                                if ( line.toLowerCase().startsWith( "explain plan " ) ) {
                                    writer.println( processExplainPlanResultSet( rs ) );
                                } else {
                                    writer.println( processResultSet( rs, Integer.MAX_VALUE, DEFAULT_MAX_DATA_LENGTH ) );
                                }
                                rs.close();
                            } else {
                                stopWatch.stop();
                                writer.println( "NUMBER OF ROWS AFFECTED: " + statement.getUpdateCount() );
                            }

                        } catch ( SQLException ex ) {
                            if ( log.isInfoEnabled() ) {
                                log.info( "", ex );
                            }
                            errorWriter.println( ex.getMessage() + "\n" );
                            errorWriter.flush();
                        }
                    }

                    writer.println( "* [CLIENT] Total execution time: " + stopWatch );
                    stopWatch.reset();

                    writer.print( "> " );
                    writer.flush();
                }
            }
        } catch ( Throwable t ) {
            if ( log.isErrorEnabled() ) {
                log.error( "Uncaught Throwable.", t );
            }
        }
    }


    public static final int DEFAULT_MAX_ROWS = 25;
    public static final int DEFAULT_MAX_DATA_LENGTH = 25;
    private static final String CROP_STRING = "[...]";
    private static final String NULL_STRING = "NULL";
    private static final String BINARY_STRING = "BINARY";


    public static String processResultSet( ResultSet rs, int maxRows, int maxLength ) {
        StringBuilder sb = new StringBuilder();

        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int totalColumns = rsmd.getColumnCount();
            Column[] columns = new Column[totalColumns];

            // get labels
            for ( int i = 0; i < totalColumns; i++ ) {
                columns[i] = new Column();
                columns[i].addData( rsmd.getColumnLabel( i + 1 ), maxLength );
            }

            // get Data
            int totalPrintRows = 0;
            int totalRows = 0;
            while ( rs.next() ) {
                totalRows++;
                if ( totalPrintRows < maxRows ) {
                    totalPrintRows++;
                    for ( int columnIndex = 0; columnIndex < totalColumns; columnIndex++ ) {
                        final String stringValue = rs.getString( columnIndex + 1 );

                        switch ( rsmd.getColumnType( columnIndex + 1 ) ) {
                            case Types.BINARY:
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                            case Types.BLOB:
                                columns[columnIndex].addData( BINARY_STRING, maxLength );
                                break;

                            default:
                                columns[columnIndex].addData( stringValue == null ? NULL_STRING : stringValue, maxLength );
                                break;
                        }
                    }
                }
            }

            // build table
            String horizontalLine = getHorizontalLine( columns );
            for ( int rowIndex = 0; rowIndex <= totalPrintRows; rowIndex++ ) {
                if ( PRINT_INNER_LINES || rowIndex < 2 ) {
                    sb.append( horizontalLine );
                }
                for ( int columnIndex = 0; columnIndex < totalColumns; columnIndex++ ) {
                    String line = columns[columnIndex].getData( rowIndex );
                    sb.append( String.format( "| %" + columns[columnIndex].maxLength + "s ", line ) );
                }
                sb.append( "|\n" );
            }
            sb.append( horizontalLine );
            sb.append( "Printed " + totalPrintRows + " rows out of " + totalRows + " rows\n" );
        } catch ( SQLException e ) {
            if ( log.isErrorEnabled() ) {
                log.error( "", e );
            }
        }

        return sb.toString();
    }


    private String processExplainPlanResultSet( ResultSet rs ) {
        StringBuilder sb = new StringBuilder();

        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            if ( rsmd.getColumnCount() != 1 ) {
                // Error
            }
            if ( !rs.next() ) {
                // Error
            }

            // get Plan
            final String plan = rs.getString( 1 );
            final String[] planLines = plan.split( "[\\r\\n]+" );

            // Get max line length
            int maxLineLength = 12;
            for ( String planLine : planLines ) {
                if ( planLine.length() > maxLineLength ) {
                    maxLineLength = planLine.length();
                }
            }
            maxLineLength += 4; // Add space

            // Print Plan
            sb.append( getHorizontalLine( maxLineLength ) );
            sb.append( printLine( "QUERY PLAN", maxLineLength ) );
            sb.append( getHorizontalLine( maxLineLength ) );
            for ( String planLine : planLines ) {
                sb.append( printLine( planLine, maxLineLength ) );
            }
            sb.append( getHorizontalLine( maxLineLength ) );
        } catch ( SQLException e ) {
            if ( log.isErrorEnabled() ) {
                log.error( "", e );
            }
        }
        return sb.toString();
    }


    private static String getHorizontalLine( final Column[] columns ) {
        StringBuilder sb = new StringBuilder();

        for ( Column column : columns ) {
            sb.append( "+" );
            for ( int j = 0; j < column.maxLength + 2; j++ ) {
                sb.append( "-" );
            }
        }
        sb.append( "+\n" );

        return sb.toString();
    }


    private static String getHorizontalLine( final int length ) {
        StringBuilder sb = new StringBuilder();

        sb.append( "+" );
        for ( int i = 0; i < length; i++ ) {
            sb.append( "-" );
        }
        sb.append( "+\n" );

        return sb.toString();
    }


    private static String printLine( final String text, final int lineLength ) {
        StringBuilder sb = new StringBuilder();

        sb.append( "| " );
        sb.append( text );
        for ( int i = text.length(); i < lineLength - 2; i++ ) {
            sb.append( " " );
        }
        sb.append( " |\n" );

        return sb.toString();
    }


    private static class Column {

        private int maxLength = 0;
        private ArrayList<String> data = new ArrayList<>();


        void addData( final String dataStr, final int maxLength ) {
            String s = dataStr;
            if ( dataStr.length() > maxLength ) {
                s = dataStr.substring( 0, maxLength );
                s += CROP_STRING;
            }
            if ( this.maxLength < s.length() ) {
                this.maxLength = s.length();
            }
            data.add( s );
        }


        private String getData( int row ) {
            return data.get( row );
        }
    }
}
