package org.polypheny.client.db.exceptions;


/**
 * Extends {@link RuntimeException} so it doesn't have to be added to all method descriptions. Thrown when a transaction is aborted.
 *
 * @author silvan on 24.07.17.
 */
public class TransactionAbortedException extends RuntimeException {

    public TransactionAbortedException( Exception e ) {
        super( e );
    }


    public TransactionAbortedException() {

    }
}
