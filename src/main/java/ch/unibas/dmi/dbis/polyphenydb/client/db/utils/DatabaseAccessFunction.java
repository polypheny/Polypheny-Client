package ch.unibas.dmi.dbis.polyphenydb.client.db.utils;


import ch.unibas.dmi.dbis.polyphenydb.client.db.access.ConnectionException;


/**
 * Functional Interface for queries running against a database.
 *
 * @author silvan on 10.04.18.
 */
@FunctionalInterface
public interface DatabaseAccessFunction {

    String execute() throws ConnectionException;

}
