package org.polypheny.client.db.tpch;


import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.config.TPCHConfig;
import org.polypheny.client.db.access.ConnectionException;
import org.polypheny.client.db.access.DBConnector;
import org.polypheny.client.generator.tpch.objects.Customer;
import org.polypheny.client.generator.tpch.objects.LineItem;
import org.polypheny.client.generator.tpch.objects.Nation;
import org.polypheny.client.generator.tpch.objects.Order;
import org.polypheny.client.generator.tpch.objects.Part;
import org.polypheny.client.generator.tpch.objects.PartSupplier;
import org.polypheny.client.generator.tpch.objects.Region;
import org.polypheny.client.generator.tpch.objects.Supplier;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHTransactionType;
import org.polypheny.client.rpc.ProtoObjectFactory;


/**
 * This is the interface that any system to be benchmarked should implement.
 *
 * @author Manuel Huerbin
 * @tpchversion 2.17.1
 */
public abstract class TPCHBenchmarker {

    // scale factor (SF) must be either 1, 10, 30, 100, 300, 1000, 3000, 10000, 30000 or 100000
    private double SCALE_FACTOR;
    private Logger logger = LogManager.getLogger();


    public TPCHBenchmarker( final double SCALE_FACTOR ) {
        this.SCALE_FACTOR = SCALE_FACTOR;
    }


    /**
     * Create the DB Schema according to section 1.2
     */
    public abstract void createTables();


    public void populateDatabase() {
        try {
            logger.trace( "populating database" );
            final double scaleFactor = SCALE_FACTOR;
            final int part = TPCHConfig.PART;
            final int partCount = TPCHConfig.PART_COUNT;

            // populate table 'tpch_part'
            writeParts( scaleFactor, part, partCount );

            // populate table 'tpch_region'
            writeRegions();

            // populate table 'tpch_nation'
            writeNations();

            // populate table 'tpch_supplier'
            writeSuppliers( scaleFactor, part, partCount );

            // populate table 'tpch_partsupp'
            writePartSuppliers( scaleFactor, part, partCount );

            // populate table 'tpch_customer'
            writeCustomers( scaleFactor, part, partCount );

            // populate table "tpch_orders"
            writeOrders( scaleFactor, part, partCount );

            // populate table 'tpch_lineitem'
            writeLineItems( scaleFactor, part, partCount );
        } catch ( ConnectionException e ) {
            logger.error( "ConnectionException while populating Database. Exiting." );
            abort();
            throw new RuntimeException( e );
        }
        logger.info( "Finished populating database" );
    }


    /**
     * Aborts the currently running benchmark, closing all associated resources. Must not throw exceptions.
     */
    public abstract void abort();


    /**
     * Writes multiple {@link Part}s to the Database.
     */
    protected abstract void writeParts( double scaleFactor, int part, int partCount ) throws ConnectionException;


    /**
     * Writes multiple {@link Supplier}s to the Database.
     */
    protected abstract void writeSuppliers( double scaleFactor, int part, int partCount ) throws ConnectionException;


    /**
     * Writes multiple {@link PartSupplier}s to the Database.
     */
    protected abstract void writePartSuppliers( double scaleFactor, int part, int partCount ) throws ConnectionException;


    /**
     * Writes multiple {@link Customer}s to the Database.
     */
    protected abstract void writeCustomers( double scaleFactor, int part, int partCount ) throws ConnectionException;


    /**
     * Writes multiple {@link Order}s to the Database.
     */
    protected abstract void writeOrders( double scaleFactor, int part, int partCount ) throws ConnectionException;


    /**
     * Writes multiple {@link LineItem}s to the Database.
     */
    protected abstract void writeLineItems( double scaleFactor, int part, int partCount ) throws ConnectionException;


    /**
     * Writes multiple (default) {@link Nation}s to the Database.
     */
    protected abstract void writeNations() throws ConnectionException;


    /**
     * Writes multiple (default) {@link Region}s to the Database.
     */
    protected abstract void writeRegions() throws ConnectionException;


    /**
     * Fire Query against Database.
     */
    public abstract TPCHResultTuple genericQueryExecutor( final int queryID );


    /**
     * Fire Refresh Function against Database.
     */
    public abstract TPCHResultTuple genericRefreshFunctionExecutor( final int refreshFunctionID );


    public TPCHTransactionType getTypeForQueryID( int queryID ) {
        TPCHTransactionType transactionType = TPCHTransactionType.TPCHTRANSACTIONUNDEFINED;
        switch ( queryID ) {
            case 1:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONPRICINGSUMMARYREPORT;
                break;
            case 2:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONMINIMUMCOSTSUPPLIER;
                break;
            case 3:
                transactionType = TPCHTransactionType.TPCHTRANSCATIONSHIPPINGPRIORITY;
                break;
            case 4:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONORDERPRIORITYCHECKING;
                break;
            case 5:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONLOCALSUPPLIERCOLUME;
                break;
            case 6:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONFORECASTINGREVENUECHANGE;
                break;
            case 7:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONVOLUMESHIPPING;
                break;
            case 8:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONNATIONALMARETSHARE;
                break;
            case 9:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONPRODUCTTYPEPROFITMEASURE;
                break;
            case 10:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONRETURNEDITEMREPORTING;
                break;
            case 11:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONIMPORTANTSTOCKIDENTIFICATION;
                break;
            case 12:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONSHIPPINGMODESANDORDERPRIORITY;
                break;
            case 13:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONCUSTOMERDISTRIBUTION;
                break;
            case 14:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONPROMOTIONEFFECT;
                break;
            case 15:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONTOPSUPPLIER;
                break;
            case 16:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONPARTSSUPPLIERRELATIONSHIP;
                break;
            case 17:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONSMALLQUANTITYORDERREVENUE;
                break;
            case 18:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONLARGEVOLUMECUSTOMER;
                break;
            case 19:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONDISCOUNTEDREVENUE;
                break;
            case 20:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONPOTENTIALPARTPROMOTION;
                break;
            case 21:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONSUPPLIERSWHOKEPTORDERSWAITING;
                break;
            case 22:
                transactionType = TPCHTransactionType.TPCHTRANSACTIONGLOBALSALESOPPORTUNITY;
        }
        return transactionType;
    }


    public TPCHResultTuple genericQueryExecutor( int queryID, DBConnector connector, String query ) {
        TPCHTransactionType transactionType = getTypeForQueryID( queryID );

        if ( query.contains( "select 1 from category;" ) ) {
            logger.info( "Query {} is disabled", queryID );
            //Ugly hack to disable queries
            return TPCHResultTuple.getDefaultInstance();
        }
        long start;
        long stop;
        logger.trace( "Starting Q" + queryID + " transaction" );
        start = System.currentTimeMillis();
        connector.startTransaction();
        try ( ResultSet resultSet = connector.executeQuery( query ) ) {
            if ( !resultSet.next() ) {
                logger.error( "Could not move resultset for query {}", query );
            }
            connector.commitTransaction();
            stop = System.currentTimeMillis();
            logger.trace( "Finished Q" + queryID + " transaction. Elapsed time: {} ms", (stop - start) );
        } catch ( SQLException e ) {
            //We just log exceptions. For example Q11 throws an expected exception for VoltDB
            logger.error( "ConnectionException while Q" + queryID + " transaction. Logging and continuing. \n" );
            try {
                connector.abortTransaction();
            } catch ( ConnectionException ignored ) {
                //Ignore
            }
            logger.debug( "Query = {} \n \n", query );
            logger.error( e );
            return ProtoObjectFactory.TPCHResultTuple( start, (System.currentTimeMillis() - start), transactionType, queryID, true, query );
            //abort();
            //throw new RuntimeException( e );
        }
        logger.trace( "Finished Q{} with text {}", queryID, query );
        return ProtoObjectFactory.TPCHResultTuple( start, (stop - start), transactionType, queryID, false, query );
    }
}
