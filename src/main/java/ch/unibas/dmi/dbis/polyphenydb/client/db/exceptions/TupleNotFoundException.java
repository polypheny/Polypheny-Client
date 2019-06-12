package ch.unibas.dmi.dbis.polyphenydb.client.db.exceptions;


/**
 * This exception should be thrown when a tuple is not found, i.e. an invalid id is specified. An Example would be 2.4.1.5 of the TPC-C Specification
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class TupleNotFoundException extends Exception {

}
