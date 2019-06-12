package ch.unibas.dmi.dbis.polyphenydb.client.db.utils;


import java.sql.SQLException;


/**
 * A functional interface for lambdas which take throw SQLExceptions
 *
 * @author silvan on 19.07.17.
 */
@FunctionalInterface
public interface CheckedResultSetFunction<ResultSet, T> {

    T apply( ResultSet resultSet ) throws SQLException;
}
