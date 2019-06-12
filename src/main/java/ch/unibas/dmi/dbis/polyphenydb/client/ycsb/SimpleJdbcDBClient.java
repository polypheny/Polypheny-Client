/*
 * Copyright (c) 2010 - 2016 Yahoo! Inc., 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the
 * License. See accompanying LICENSE file.
 */

package ch.unibas.dmi.dbis.polyphenydb.client.ycsb;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;


/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced with YCSB. This class extends {@link DB} and implements the database interface used by YCSB client.
 *
 * <br> Each client will have its own instance of this class. This client is not thread safe.
 *
 * <br> This interface expects a schema <key> <field1> <field2> <field3> ... All attributes are of type VARCHAR. All accesses are through the primary key. Therefore, only one index on the primary key is needed.
 */
public class SimpleJdbcDBClient extends DB {

    /**
     * The class to use as the jdbc driver.
     */
    public static final String DRIVER_CLASS = "db.driver";

    /**
     * The URL to connect to the database.
     */
    public static final String CONNECTION_URL = "db.url";

    /**
     * The user name to use to connect to the database.
     */
    public static final String CONNECTION_USER = "db.user";

    /**
     * The password to use for establishing the connection.
     */
    public static final String CONNECTION_PASSWD = "db.passwd";

    /**
     * The batch size for batched inserts. Set to >0 to use batching
     */
    public static final String DB_BATCH_SIZE = "db.batchsize";

    /**
     * The JDBC fetch size hinted to the driver.
     */
    public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

    /**
     * The JDBC connection auto-commit property for the driver.
     */
    public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

    public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

    /**
     * The name of the property for the number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY = "fieldcount";

    /**
     * Default number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

    /**
     * Representing a NULL value.
     */
    public static final String NULL_VALUE = "NULL";

    /**
     * The primary key in the user table.
     */
    public static final String PRIMARY_KEY = "YCSB_KEY";

    /**
     * The field name prefix in the table.
     */
    public static final String COLUMN_PREFIX = "FIELD";


    private static final String DEFAULT_PROP = "";
    private ArrayList<Connection> conns;
    private Connection conn;
    private boolean initialized = false;
    private Properties props;
    private int jdbcFetchSize;
    private int batchSize;
    private boolean autoCommit;
    private boolean batchUpdates;
    private long numRowsInBatch = 0;


    /**
     * Returns parsed int value from the properties if set, otherwise returns -1.
     */
    private static int getIntProperty( Properties props, String key ) throws DBException {
        String valueStr = props.getProperty( key );
        if ( valueStr != null ) {
            try {
                return Integer.parseInt( valueStr );
            } catch ( NumberFormatException nfe ) {
                System.err.println( "Invalid " + key + " specified: " + valueStr );
                throw new DBException( nfe );
            }
        }
        return -1;
    }


    /**
     * Returns parsed boolean value from the properties if set, otherwise returns defaultVal.
     */
    private static boolean getBoolProperty( Properties props, String key, boolean defaultVal ) {
        String valueStr = props.getProperty( key );
        if ( valueStr != null ) {
            return Boolean.parseBoolean( valueStr );
        }
        return defaultVal;
    }


    /**
     * For the given key, returns what shard contains data for this key.
     *
     * @param key Data key to do operation on
     * @return Shard index
     */
    private int getShardIndexByKey( String key ) {
        int ret = Math.abs( key.hashCode() ) % conns.size();
        return ret;
    }


    /**
     * For the given key, returns Connection object that holds connection to the shard that contains this key.
     *
     * @param key Data key to get information for
     * @return Connection object
     */
    private Connection getShardConnectionByKey( String key ) {
        return conns.get( getShardIndexByKey( key ) );
    }


    private void cleanupAllConnections() throws SQLException {
        for ( Connection conn : conns ) {
            if ( !autoCommit ) {
                conn.commit();
            }
            conn.close();
        }
    }


    @Override
    public void init() throws DBException {
        if ( initialized ) {
            System.err.println( "Client connection already initialized." );
            return;
        }
        props = getProperties();
        String urls = props.getProperty( CONNECTION_URL, DEFAULT_PROP );
        String user = props.getProperty( CONNECTION_USER, DEFAULT_PROP );
        String passwd = props.getProperty( CONNECTION_PASSWD, DEFAULT_PROP );
        String driver = props.getProperty( DRIVER_CLASS );

        this.jdbcFetchSize = getIntProperty( props, JDBC_FETCH_SIZE );
        this.batchSize = getIntProperty( props, DB_BATCH_SIZE );

        this.autoCommit = getBoolProperty( props, JDBC_AUTO_COMMIT, true );
        this.batchUpdates = getBoolProperty( props, JDBC_BATCH_UPDATES, false );

        try {
            if ( driver != null ) {
                Class.forName( driver );
            }
            int shardCount = 0;
            conns = new ArrayList<>( 3 );
            final String[] urlArr = urls.split( "," );
            for ( String url : urlArr ) {
                System.out.println( "Adding shard node URL: " + url );
                Connection conn = DriverManager.getConnection( url, user, passwd );

                // Since there is no explicit commit method in the DB interface, all
                // operations should auto commit, except when explicitly told not to
                // (this is necessary in cases such as for PostgreSQL when running a
                // scan workload with fetchSize)
                conn.setAutoCommit( autoCommit );

                shardCount++;
                conns.add( conn );
            }

            conn = conns.get( 0 );

            System.out.println( "Using shards: " + shardCount + ", batchSize:" + batchSize + ", fetchSize: " + jdbcFetchSize );

        } catch ( ClassNotFoundException e ) {
            System.err.println( "Error in initializing the JDBS driver: " + e );
            throw new DBException( e );
        } catch ( SQLException e ) {
            System.err.println( "Error in database operation: " + e );
            throw new DBException( e );
        } catch ( NumberFormatException e ) {
            System.err.println( "Invalid value for fieldcount property. " + e );
            throw new DBException( e );
        }

        initialized = true;
    }


    @Override
    public void cleanup() throws DBException {
        try {
            cleanupAllConnections();
        } catch ( SQLException e ) {
            System.err.println( "Error in closing the connection. " + e );
            throw new DBException( e );
        }
    }


    @Override
    public Status read( String tableName, String key, Set<String> fields, HashMap<String, ByteIterator> result ) {
        try ( final Statement readStatement = conn.createStatement() ) {
            final StringBuilder sql = new StringBuilder( "SELECT * FROM " );
            sql.append( tableName );
            sql.append( " WHERE " );
            sql.append( PRIMARY_KEY );
            sql.append( " = '" );
            sql.append( key );
            sql.append( "'" );

            try ( final ResultSet resultSet = readStatement.executeQuery( sql.toString() ) ) {
                final ResultSetMetaData metaData = resultSet.getMetaData();

                if ( !resultSet.next() ) {
                    resultSet.close();
                    return Status.NOT_FOUND;
                }

                if ( result == null ) {
                    result = new HashMap<>();
                }

                if ( fields == null || fields.isEmpty() ) {
                    // select *
                    for ( int ci = 1; ci <= metaData.getColumnCount(); ++ci ) {
                        final String label = metaData.getColumnLabel( ci );
                        final String value = resultSet.getString( ci );
                        result.put( label, new StringByteIterator( value ) );
                    }
                } else {
                    // select ...
                    for ( String field : fields ) {
                        final String value = resultSet.getString( field );
                        result.put( field, new StringByteIterator( value ) );
                    }
                }
            }

            return Status.OK;
        } catch ( SQLException e ) {
            System.err.println( "Error in processing read of table " + tableName + ": " + e );
            return Status.ERROR;
        }
    }


    @Override
    public Status scan( String tableName, String startKey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result ) {
        try ( final Statement scanStatement = conn.createStatement() ) {
            final StringBuilder sql = new StringBuilder( "SELECT * FROM " );
            sql.append( tableName );
            sql.append( " WHERE " );
            sql.append( PRIMARY_KEY );
            sql.append( " >= '" );
            sql.append( startKey );
            sql.append( "' ORDER BY " );
            sql.append( PRIMARY_KEY );
            sql.append( " LIMIT " );
            sql.append( recordcount );

            try ( final ResultSet resultSet = scanStatement.executeQuery( sql.toString() ) ) {
                final ResultSetMetaData metaData = resultSet.getMetaData();

                if ( result == null ) {
                    result = new Vector<>();
                }

                for ( int i = 0; i < recordcount && resultSet.next(); i++ ) {
                    HashMap<String, ByteIterator> values = new HashMap<>();
                    if ( fields == null || fields.isEmpty() ) {
                        for ( int ci = 1; ci <= metaData.getColumnCount(); ++ci ) {
                            final String label = metaData.getColumnLabel( ci );
                            final String value = resultSet.getString( ci );
                            values.put( label, new StringByteIterator( value ) );
                        }
                    } else {
                        for ( String field : fields ) {
                            String value = resultSet.getString( field );
                            values.put( field, new StringByteIterator( value ) );
                        }
                        result.add( values );
                    }
                }
            }

            return Status.OK;
        } catch ( SQLException e ) {
            System.err.println( "Error in processing scan of table: " + tableName + ": " + e );
            return Status.ERROR;
        }
    }


    @Override
    public Status update( String tableName, String key, HashMap<String, ByteIterator> values ) {
        try ( final Statement updateStatement = conn.createStatement() ) {
            final StringBuilder sql = new StringBuilder( "UPDATE " );
            sql.append( tableName );
            sql.append( " SET " );
            int i = 0;
            for ( Entry<String, ByteIterator> v : values.entrySet() ) {
                sql.append( v.getKey() );
                sql.append( "='" );
                sql.append( v.getValue().toString().replaceAll( "'", "" ).replaceAll( "\"", "" ) );
                if ( i++ < values.size() - 1 ) {
                    sql.append( "', " );
                } else {
                    sql.append( "'" );
                }
            }
            sql.append( " WHERE " );
            sql.append( PRIMARY_KEY );
            sql.append( " = '" );
            sql.append( key );
            sql.append( "'" );

            final int result = updateStatement.executeUpdate( sql.toString() );

            if ( result == 1 ) {
                return Status.OK;
            } else {
                return Status.UNEXPECTED_STATE;
            }
        } catch ( SQLException e ) {
            System.err.println( "Error in processing update to table: " + tableName + ": " + e );
            return Status.ERROR;
        }
    }


    @Override
    public Status insert( String tableName, String key, HashMap<String, ByteIterator> values ) {
        try ( final Statement insertStatement = conn.createStatement() ) {
            final StringBuilder sql = new StringBuilder( "INSERT INTO " );
            sql.append( tableName );
            sql.append( " ( " );
            for ( Entry<String, ByteIterator> v : values.entrySet() ) {
                sql.append( v.getKey() );
                sql.append( ", " );
            }
            sql.append( PRIMARY_KEY );
            sql.append( " ) VALUES ( '" );
            for ( Entry<String, ByteIterator> v : values.entrySet() ) {
                sql.append( v.getValue().toString().replaceAll( "'", "" ).replaceAll( "\"", "" ) );
                sql.append( "', '" );
            }
            sql.append( key );
            sql.append( "' )" );

            // Normal update
            final int result = insertStatement.executeUpdate( sql.toString() );

            if ( result == 1 ) {
                return Status.OK;
            } else {
                return Status.UNEXPECTED_STATE;
            }
        } catch ( SQLException e ) {
            System.err.println( "Error in processing insert to table: " + tableName + ": " + e );
            return Status.ERROR;
        }
    }


    @Override
    public Status delete( String tableName, String key ) {
        try ( final Statement deleteStatement = conn.createStatement() ) {
            final StringBuilder sql = new StringBuilder( "DELETE FROM " );
            sql.append( tableName );
            sql.append( " WHERE " );
            sql.append( PRIMARY_KEY );
            sql.append( " = '" );
            sql.append( key );
            sql.append( "'" );

            final int result = deleteStatement.executeUpdate( sql.toString() );

            if ( result == 1 ) {
                return Status.OK;
            } else {
                return Status.UNEXPECTED_STATE;
            }
        } catch ( SQLException e ) {
            System.err.println( "Error in processing delete to table: " + tableName + ": " + e );
            return Status.ERROR;
        }
    }
}
