package org.polypheny.client.scenarios.tpcc.worker;


import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.io.File;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.config.Config;
import org.polypheny.client.db.tpcc.TPCCBenchmarker;
import org.polypheny.client.grpc.PolyClientGRPC.FetchResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.ResultMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultMessage.Builder;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCTransactionType;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCWorkerMessage.ThinkModeCase;
import org.polypheny.client.storage.JsonStreamReader;
import org.polypheny.client.storage.StorageGson;
import org.polypheny.client.storage.StreamWriter;


/**
 * Polypheny-Client abstraction for the TPC-C Terminal. Is controlled by a {@link TPCCWorker}.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class Terminal implements Runnable {

    private static final Logger logger = LogManager.getLogger();
    private TPCCWorker worker;
    private int districtID;
    private int warehouseID;
    private volatile boolean running;
    private TPCCBenchmarker benchmarker;
    private StreamWriter<TPCCResultTuple> resultWriter;
    private JsonStreamReader<TPCCResultTuple> resultReader;


    public Terminal( TPCCWorker worker, int districtID, int warehouseID ) {
        this.worker = worker;
        this.districtID = districtID;
        this.warehouseID = warehouseID;
        this.running = false;
        this.benchmarker = worker.createBenchmarker( this );
        File storageFolder = new File( new File( Config.DEFAULT_WORKER_STORAGE_LOCATION ), warehouseID + "" );
        if ( !storageFolder.mkdirs() ) {
            logger.trace( "Storage Folder {} was not created", storageFolder.getPath() );
        }
        File storage = new File( storageFolder, districtID + ".json" );
        resultWriter = new StreamWriter<>( SettableFuture.create(), storage, TPCCResultTuple.class );
        resultReader = new JsonStreamReader<>( storage, TPCCResultTuple.class,
                StorageGson.getGson() );
    }


    /**
     * Starts this Terminal according to TPC-C Specifications
     */
    @Override
    public void run() {
        running = true;
        while ( running ) {
            //This is in line with TPC-C Terminal behavior
            TPCCTransactionType transactionType = worker.selectTransactionType();
            int queryID = worker.generateQueryID();
            TPCCResultTuple tuple = performTransaction( transactionType, queryID );
            logTransaction( tuple );
            if ( worker.getWorkerMessage().getTPCCTERMINALTHINK() ) {
                think();
            }
        }
        benchmarker.abort();
    }


    /**
     * Stops execution
     */
    public void stop() {
        resultWriter.onCompleted();
        this.running = false;
    }


    public boolean isRunning() {
        return running;
    }


    /**
     * Stores the transaction
     *
     * @param tuple {@link TPCCResultTuple} which stores all relevant information about a transaction
     */
    private void logTransaction( TPCCResultTuple tuple ) {
        if ( tuple.getQueryID() == 0 ) {
            logger.trace( "Ignoring unsupported query" );
            return;
        }
        resultWriter.onNext( tuple );
        logger.trace( "Query {} with transaction {} took {} ms", tuple.getQueryID(),
                tuple.getTransactionType(), tuple.getResponseTime() );
    }


    /**
     * Executes a TPC-C Transaction
     *
     * @param transactionType {@link TPCCTransactionType} to be executed
     * @param queryID unqiue query ID (at least for the {@link TPCCWorker} this {@link Terminal} is assigned to)
     * @return information about the transaction which was executed
     */
    private TPCCResultTuple performTransaction( TPCCTransactionType transactionType, int queryID ) {
        switch ( transactionType ) {
            case TPCCTRANSACTIONNEWORDER:
                return benchmarker.newOrderTransaction( warehouseID, queryID, worker.getWorkerMessage().getCOLIID() );
            case TPCCTRANSACTIONPAYMENT:
                return benchmarker.paymentTransaction( queryID, warehouseID, worker.getWorkerMessage().getCCLAST(), worker.getWorkerMessage().getCCID() );
            case TPCCTRANSACTIONDELIVERY:
                return benchmarker.deliveryTransaction( queryID, warehouseID );
            case TPCCTRANSACTIONORDERSTATUS:
                return benchmarker.orderStatusTransaction( queryID, worker.getWorkerMessage().getCCLAST(), worker.getWorkerMessage().getCCID(), warehouseID );
            case TPCCTRANSACTIONSTOCK:
                return benchmarker.stockLevelTransaction( queryID, warehouseID, districtID );
            default:
                logger.error( "Transactiontype {} not supported", transactionType );
                throw new UnsupportedOperationException();
        }
    }


    /**
     * Waits according to Step 7 in 5.2.2
     */
    private void think() {
        try {
            if ( worker.getWorkerMessage().getThinkModeCase() == ThinkModeCase.CONSTANTSLEEP ) {
                Thread.sleep( worker.getWorkerMessage().getConstantSleep() );
            }
            if ( worker.getWorkerMessage().getThinkModeCase() == ThinkModeCase.UNIFORMSLEEP ) {
                Thread.sleep( RandomUtils.nextLong( worker.getWorkerMessage().getUniformSleep().getLower(), worker.getWorkerMessage().getUniformSleep().getUpper() ) );
            }
        } catch ( InterruptedException e ) {
            logger.fatal( "Thinking time of terminal at district {} and warehouse {} interrupted",
                    districtID, warehouseID );
        }
    }


    /**
     * @param responseObserver The stored results will be sent to this StreamObserver
     * @param request Information about which results you want fetched
     */
    public void sendResults( StreamObserver<ResultMessage> responseObserver, FetchResultsMessage request ) {
        resultReader.reset();
        Builder builder = TPCCResultMessage.newBuilder();

        resultReader.start();

        int counter = 0;
        while ( resultReader.hasNext() ) {
            //Iterate in batches of 100
            for ( TPCCResultTuple tuple : resultReader.readFromStream( 100 ) ) {
                if ( request.getStartTime() < tuple.getStartTimestamp() && request.getStopTime() > tuple.getStartTimestamp() ) {
                    counter++;
                    builder.addResults( tuple );
                    if ( counter % 100 == 0 ) {
                        responseObserver.onNext( ResultMessage.newBuilder().setTpccResultMessage( builder.build() ).build() );
                        builder.clear();
                    }
                }
            }
        }
        responseObserver.onNext( ResultMessage.newBuilder().setTpccResultMessage( builder.build() ).build() );
    }


    @Override
    public String toString() {
        return "Terminal{" +
                "districtID=" + districtID +
                ", warehouseID=" + warehouseID +
                '}';
    }
}
