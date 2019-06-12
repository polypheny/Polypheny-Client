package ch.unibas.dmi.dbis.polyphenydb.client.scenarios.tpcc.worker;


import ch.unibas.dmi.dbis.polyphenydb.client.db.access.ConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.IcarusTpccBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.PolyphenyDbTpccBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.PostgresTpccBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.TPCCBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.DBInfo;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.FetchResultsMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ProgressMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.ResultMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCTransactionType;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.TPCCWorkerMessage;
import ch.unibas.dmi.dbis.polyphenydb.client.rpc.ProtoObjectFactory;
import ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Worker;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The thread which each worker of the master-worker architecture executes. A {@link TPCCWorker} gets assigned a series of warehouses and launches {@link Terminal} for each of those. Terminals are responsible for bombarding the DBMS with queries
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class TPCCWorker implements Worker {


    private static final Logger logger = LogManager.getLogger();

    private final LaunchWorkerMessage workerMessage;
    //Keep track of transaction count for 'random'-generation
    private AtomicInteger newOrderTransactions = new AtomicInteger( 0 );
    private AtomicInteger paymentTransactions = new AtomicInteger( 0 );
    private AtomicInteger orderStatusTransactions = new AtomicInteger( 0 );
    private AtomicInteger deliveryTransactions = new AtomicInteger( 0 );
    private AtomicInteger stockTransactions = new AtomicInteger( 0 );
    private AtomicInteger queryID = new AtomicInteger( 1 );
    //Maps TerminalID to terminal
    private List<Terminal> terminals;
    private boolean running = false;


    public TPCCWorker( LaunchWorkerMessage workerMessage ) {
        this.workerMessage = workerMessage;
        this.terminals = new ArrayList<>();
        logger.info( "This TPCCWorker is responsible for the warehouseIDs from {} to {} (exclusive upper bound)", getWorkerMessage().getWarehouses().getLower(), getWorkerMessage().getWarehouses().getUpper() );
    }


    /**
     * Starts the TPC-C Worker. Launches {@link TPCCWorkerMessage#getTerminalPerDistrict()} {@link Terminal}s per District. Launch from lower to upper bound (lower inclusive, upper exclusive)
     */
    @Override
    public void start() {
        running = true;
        logger.info( "Starting TPC-C Terminals" );
        for ( int warehouseID = getWorkerMessage().getWarehouses().getLower(); warehouseID < getWorkerMessage().getWarehouses().getUpper(); warehouseID++ ) {
            for ( int districtID = 1; districtID <= 10;
                    districtID++ ) {
                logger.trace( "Starting terminals for district {} and warehouse {}", districtID, warehouseID );
                //Create Terminals and start them
                for ( int terminalIdx = 0; terminalIdx < getWorkerMessage().getTerminalPerDistrict(); terminalIdx++ ) {
                    Terminal terminal = new Terminal( this, districtID, warehouseID );
                    logger.trace( "Starting terminal for warehouse {} and district {}", warehouseID, districtID );
                    terminals.add( terminal );
                    new Thread( terminal ).start();
                }
            }
            logger.debug( "Started terminals for warehouse {}", warehouseID );
        }
        logger.info( "All Terminals started" );
        new Thread( () -> {
            int total = 0;
            while ( running ) {
                try {
                    logger.debug( "{} seconds passed, {} queries executed", total / 1_000, queryID.get() );
                    total += 10_000;
                    Thread.sleep( 10_000 );
                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        } ).start();
    }


    @Override
    public void abort() {
        running = false;
        for ( Terminal terminal : terminals ) {
            terminal.stop();
        }
    }


    @Override
    public void sendResults( StreamObserver<ResultMessage> responseObserver, FetchResultsMessage request ) {
        for ( Terminal terminal : terminals ) {
            terminal.sendResults( responseObserver, request );
        }
    }


    public TPCCWorkerMessage getWorkerMessage() {
        return workerMessage.getTpccWorkerMessage();
    }


    private DBInfo getDBInfo() {
        return workerMessage.getDbInfo();
    }


    /**
     * Parses the {@link TPCCWorkerMessage} to determine the kind of benchmarker which should be used. Each Terminal gets its own benchmarker so each terminal gets its own connection to the target DBMS.
     *
     * @param terminal the {@link Terminal} which wants to get a benchmarker
     * @return a {@link TPCCBenchmarker} which can be used to run queries against the System
     */
    TPCCBenchmarker createBenchmarker( Terminal terminal ) {
        switch ( getDBInfo().getSystem() ) {
            case SYSTEMPOSTGRESQL:
                try {
                    return new PostgresTpccBenchmarker( this.workerMessage );
                } catch ( ConnectionException e ) {
                    logger.fatal( "could not create a benchmarker for terminal {}", terminal.toString() );
                    throw new RuntimeException( e );
                }
            case SYSTEMICARUS:
                return new IcarusTpccBenchmarker( this.workerMessage );
            case SYSTEMPOLYPHENY:
                try {
                    return new PolyphenyDbTpccBenchmarker( this.workerMessage );
                } catch ( ConnectionException e ) {
                    logger.fatal( "could not create a benchmarker for terminal {}", terminal.toString() );
                    throw new RuntimeException( e );
                }
            default:
                logger.error( "System {} not supported", getDBInfo().getSystem() );
                throw new UnsupportedOperationException();
        }
    }


    /**
     * Sums all Transaction counts
     */
    private int getTotalTransactionCount() {
        return this.newOrderTransactions.get() + this.paymentTransactions.get() + this.deliveryTransactions.get() + this.orderStatusTransactions.get() + this.stockTransactions.get();
    }


    /**
     * Chooses a queryType according to a weighted distribution specified in 5.2.3 Corresponds to Step 1 in 5.2.2. First checks if a transactionCount falls below required minimum, then uses {@link Random#nextDouble()} to generate a new Transaction
     *
     * @return {@link TPCCTransactionType} which should be performed next by this user.
     */
    TPCCTransactionType selectTransactionType() {
        if ( paymentTransactions.get() / 0.43 < getTotalTransactionCount() ) {
            paymentTransactions.getAndIncrement();
            return TPCCTransactionType.TPCCTRANSACTIONPAYMENT;
        }
        if ( deliveryTransactions.get() / 0.04 < getTotalTransactionCount() ) {
            deliveryTransactions.getAndIncrement();
            return TPCCTransactionType.TPCCTRANSACTIONDELIVERY;
        }
        if ( orderStatusTransactions.get() / 0.04 < getTotalTransactionCount() ) {
            orderStatusTransactions.getAndIncrement();
            return TPCCTransactionType.TPCCTRANSACTIONORDERSTATUS;
        }
        if ( stockTransactions.get() / 0.04 < getTotalTransactionCount() ) {
            stockTransactions.getAndIncrement();
            return TPCCTransactionType.TPCCTRANSACTIONSTOCK;
        }
        Double random = new Random().nextDouble();
        if ( random < 0.43 ) {
            paymentTransactions.getAndIncrement();
            return TPCCTransactionType.TPCCTRANSACTIONPAYMENT;
        }
        if ( random < 0.47 ) {
            deliveryTransactions.getAndIncrement();
            return TPCCTransactionType.TPCCTRANSACTIONDELIVERY;
        }
        if ( random < 0.51 ) {
            orderStatusTransactions.getAndIncrement();
            return TPCCTransactionType.TPCCTRANSACTIONORDERSTATUS;
        }
        if ( random < 0.55 ) {
            stockTransactions.getAndIncrement();
            return TPCCTransactionType.TPCCTRANSACTIONSTOCK;
        }
        newOrderTransactions.getAndIncrement();
        return TPCCTransactionType.TPCCTRANSACTIONNEWORDER;
    }


    /**
     * 'Generates' a query ID in the sense that it gets and increments the atomic integer
     *
     * @return an ID which is guaranteed to be unique for this worker and this run.
     */
    int generateQueryID() {
        return queryID.getAndIncrement();
    }


    @Override
    public boolean isRunning() {
        return running;
    }


    @Override
    public ProgressMessage progress() {
        return ProtoObjectFactory.ProgressMessage( !running, queryID.get() );
    }
}
