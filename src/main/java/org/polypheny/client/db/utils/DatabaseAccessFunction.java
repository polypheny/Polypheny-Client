package org.polypheny.client.db.utils;


import org.polypheny.client.db.access.ConnectionException;


/**
 * Functional Interface for queries running against a database.
 *
 * @author silvan on 10.04.18.
 */
@FunctionalInterface
public interface DatabaseAccessFunction {

    String execute() throws ConnectionException;

}
