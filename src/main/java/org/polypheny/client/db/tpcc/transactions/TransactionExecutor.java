package org.polypheny.client.db.tpcc.transactions;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.db.exceptions.TransactionAbortedException;
import org.polypheny.client.db.utils.CheckedResultSetFunction;
import org.polypheny.client.db.utils.DatabaseAccessFunction;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCQueryTuple;
import org.polypheny.client.rpc.ProtoObjectFactory;


/**
 * Top-level Transaction Executor. Contains core Logic to reduce code-duplication. If you disagree with anything this class does, just @Override the function.
 *
 * @author silvan on 19.07.17.
 */
public abstract class TransactionExecutor {

    public static final Logger logger = LogManager.getLogger();
    private final DBConnector connector;
    List<TPCCQueryTuple> queries = new ArrayList<>();


    TransactionExecutor( DBConnector connector ) {
        this.connector = connector;
    }


    /**
     * Executes AND logs a given query. Allows you to apply a function to the resultset.
     *
     * @param function The ResultSet will already have one next() call applied to it, so you start at the first row.
     * @param query The query you want executed
     * @param level which loglevel you want the query logged at
     * @return the result of your function
     */
    public <T> T executeQuery( CheckedResultSetFunction<ResultSet, T> function, String query, QueryType type, Level level ) {
        logger.log( level, query );
        long start = System.currentTimeMillis();
        try ( ResultSet resultSet = connector.executeQuery( query ) ) {
            if ( !resultSet.next() ) {
                logger.error( "ResultSet could not be moved for query \n{} \n", query );
                throw new TransactionAbortedException();
            }
            T result = function.apply( resultSet );
            long stop = System.currentTimeMillis();
            logQuery( ProtoObjectFactory.TPCCQueryTuple( query, stop - start, type ) );
            return result;
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * This method is intended to log functions which access a database and return their query as a string.
     *
     * @return result of the function call.
     */
    protected String executeAndLogFunction( DatabaseAccessFunction fun, QueryType queryType ) {
        try {
            long start = System.currentTimeMillis();
            String query = fun.execute();
            long stop = System.currentTimeMillis();
            logQuery( ProtoObjectFactory.TPCCQueryTuple( query, stop - start, queryType ) );
            return query;
        } catch ( ConnectionException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * By default, logs with trace-level {@link #executeQuery(CheckedResultSetFunction, String, QueryType, Level)}
     */
    public <T> T executeQuery( CheckedResultSetFunction<ResultSet, T> function, String query, QueryType type ) {
        return executeQuery( function, query, type, Level.TRACE );
    }


    protected void executeAndLogStatement( String statement, QueryType type ) {
        executeAndLogStatement( statement, type, Level.TRACE );
    }


    protected void executeAndLogStatement( String statement, QueryType type, Level level ) {
        logger.log( level, statement );
        long start = System.currentTimeMillis();
        try {
            connector.executeStatement( statement );
            long stop = System.currentTimeMillis();
            logQuery( ProtoObjectFactory.TPCCQueryTuple( statement, stop - start, type ) );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Logs an executed query. This method is intended to be used by the implementing class each time it executes a query to the DB.
     *
     * @param query the query and information about it which is to be logged.
     */
    protected void logQuery( TPCCQueryTuple query ) {
        queries.add( query );
    }


    /**
     * The database transaction is committed.
     */
    protected void commitTransaction() {
        try {
            connector.commitTransaction();
        } catch ( ConnectionException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * A database transaction is started
     */
    protected void startTransaction() {
        connector.startTransaction();
    }


    /**
     * Rollback of the transaction
     */
    void rollback() {
        try {
            connector.abortTransaction();
        } catch ( ConnectionException e ) {
            throw new RuntimeException( e );
        }
    }
}
