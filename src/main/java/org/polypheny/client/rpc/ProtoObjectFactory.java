package org.polypheny.client.rpc;


import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.polypheny.client.generator.tpcc.TPCCGenerator;
import org.polypheny.client.grpc.PolyClientGRPC.AccessMethod;
import org.polypheny.client.grpc.PolyClientGRPC.AckMessage;
import org.polypheny.client.grpc.PolyClientGRPC.AckMessage.Code;
import org.polypheny.client.grpc.PolyClientGRPC.DBInfo;
import org.polypheny.client.grpc.PolyClientGRPC.DBMSSystem;
import org.polypheny.client.grpc.PolyClientGRPC.FetchMUSQLEResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.FetchMonitorWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.FetchResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.FetchTPCCResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.FetchTPCHResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.FetchYCSBResultsMessage;
import org.polypheny.client.grpc.PolyClientGRPC.LaunchWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.MUSQLEWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.MonitorWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.MusqleTransactionType;
import org.polypheny.client.grpc.PolyClientGRPC.Pair;
import org.polypheny.client.grpc.PolyClientGRPC.ProgressMessage;
import org.polypheny.client.grpc.PolyClientGRPC.QueryType;
import org.polypheny.client.grpc.PolyClientGRPC.Scenario;
import org.polypheny.client.grpc.PolyClientGRPC.StopWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCQueryTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCTransactionType;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.TPCCWorkerMessage.Builder;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHResultTuple;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHTransactionType;
import org.polypheny.client.grpc.PolyClientGRPC.TPCHWorkerMessage;
import org.polypheny.client.grpc.PolyClientGRPC.YCSBWorkerMessage;


/**
 * Wraps GRPC-Object Construction because builder-code is ugly. Use static method imports in your own classes to beautify code.
 *
 * @author Silvan Heller
 */
public class ProtoObjectFactory {

    /**
     * @param startTime timestamp when the query started
     * @param responseTime how long the transaction took to execute
     * @param transactionType which transaction was executed
     * @param queryID internal ID of the query
     * @param aborted if the transaction was aborted.
     * @param queries complete list of queries in the order they were executed
     */
    public static TPCCResultTuple TPCCResultTuple( long startTime, float responseTime, TPCCTransactionType transactionType, int queryID, boolean aborted, List<TPCCQueryTuple> queries ) {
        return TPCCResultTuple.newBuilder().setAborted( aborted ).setQueryID( queryID ).setTransactionType( transactionType ).setResponseTime( responseTime ).setStartTimestamp( startTime ).addAllQueries( queries ).build();
    }


    /**
     * @param query querytext
     * @param time execution time
     */
    public static TPCCQueryTuple TPCCQueryTuple( String query, long time, QueryType queryType ) {
        return TPCCQueryTuple.newBuilder().setQuery( query ).setExecutionTime( time ).setQueryType( queryType ).build();
    }


    /**
     * @param dbHost Where the DB is located
     * @param port port for the DB
     * @param database database name if there is one
     * @param username username for the DBMS if there is one
     * @param password password for the DBMS if there is one
     * @param system Which system is to be benchmarked
     * @param accessMethod accessmethod to be used
     * @param warehouses upper and lower bound for the warehouseIndex
     * @param terminalsPerDistrict how many terminals per district should be launched
     * @param COL_I_ID See {@link TPCCGenerator#getCLast(int)} documentation
     * @param CC_LAST See {@link TPCCGenerator#getCLast(int)} documentation
     * @param CC_ID See {@link TPCCGenerator#getCLast(int)} documentation
     * @param NUMBER_OF_CONFIGURED_WAREHOUSES how many warehouses there are in total.
     * @param TPCC_TERMINAL_THINK whether terminals should wait after performing a query or not
     */
    public static LaunchWorkerMessage TPCCWorkerMessage( String dbHost, int port, String database, String username, String password, DBMSSystem system, AccessMethod accessMethod, Pair warehouses, int terminalsPerDistrict, int COL_I_ID, int CC_LAST, int CC_ID, int NUMBER_OF_CONFIGURED_WAREHOUSES,
            boolean TPCC_TERMINAL_THINK, Optional<Long> constantSleep, Optional<Pair> sleepBound ) {
        DBInfo dbInfo = DBInfo.newBuilder().setDbHost( dbHost ).setDbPort( port ).setDatabase( database ).setUsername( username ).setPassword( password ).setSystem( system ).setAccessMethod( accessMethod ).build();
        Builder workerMessage = TPCCWorkerMessage.newBuilder().setTerminalPerDistrict( terminalsPerDistrict ).setWarehouses( warehouses ).setCCID( CC_ID ).setCCLAST( CC_LAST ).setCOLIID( COL_I_ID ).setNUMBEROFCONFIGUREDWAREHOUSES( NUMBER_OF_CONFIGURED_WAREHOUSES )
                .setTPCCTERMINALTHINK( TPCC_TERMINAL_THINK );
        constantSleep.ifPresent( workerMessage::setConstantSleep );
        sleepBound.ifPresent( workerMessage::setUniformSleep );
        return LaunchWorkerMessage.newBuilder().setDbInfo( dbInfo ).setScenario( Scenario.SCENARIOTPCC ).setTpccWorkerMessage( workerMessage ).build();
    }


    /**
     * @param startTime timestamp when the query started
     * @param responseTime how long the transaction took to execute
     * @param transactionType which transaction was executed
     * @param queryID internal ID of the query
     * @param aborted if the transaction was aborted.
     */
    public static TPCHResultTuple TPCHResultTuple( long startTime, float responseTime, TPCHTransactionType transactionType, int queryID, boolean aborted, String query ) {
        return TPCHResultTuple.newBuilder().setAborted( aborted ).setQueryID( queryID ).setTransactionType( transactionType ).setResponseTime( responseTime ).setStartTimestamp( startTime ).setQuery( query ).build();
    }


    /**
     * @param dbHost Where the DB is located
     * @param port port for the DB
     * @param system Which system is to be benchmarked
     * @param accessMethod accessmethod to be used
     * @param streams how many streams should be executed
     */
    public static LaunchWorkerMessage TPCHWorkerMessage( String dbHost, int port,
            String database, String username, String password, DBMSSystem system, AccessMethod accessMethod, boolean executorRefreshStream, double SCALE_FACTOR, int streams ) {
        DBInfo dbInfo = DBInfo.newBuilder().setDbHost( dbHost ).setDbPort( port ).setDatabase( database ).setUsername( username ).setPassword( password ).setSystem( system ).setAccessMethod( accessMethod ).build();
        TPCHWorkerMessage workerMessage = TPCHWorkerMessage.newBuilder().setExecuteRefreshStream( executorRefreshStream ).setSCALEFACTOR( SCALE_FACTOR ).setStreams( streams ).build();
        return LaunchWorkerMessage.newBuilder().setDbInfo( dbInfo ).setScenario( Scenario.SCENARIOTPCH ).setTpchWorkerMessage( workerMessage ).build();
    }


    /**
     * @param startTime timestamp when the query started
     * @param responseTime how long the transaction took to execute
     * @param transactionType which transaction was executed
     * @param queryID internal ID of the query
     * @param aborted if the transaction was aborted.
     */
    public static MUSQLEResultTuple MusqleResultTuple( long startTime, float responseTime, MusqleTransactionType transactionType, int queryID, boolean aborted, String query ) {
        return MUSQLEResultTuple.newBuilder().setAborted( aborted ).setQueryID( queryID ).setTransactionType( transactionType ).setResponseTime( responseTime ).setStartTimestamp( startTime ).setQuery( query ).build();
    }


    /**
     * @param dbHost Where the DB is located
     * @param port port for the DB
     * @param system Which system is to be benchmarked
     * @param accessMethod accessmethod to be used
     * @param streams how many streams should be executed
     */
    public static LaunchWorkerMessage MusqleWorkerMessage( String dbHost, int port,
            String database, String username, String password, DBMSSystem system, AccessMethod accessMethod, double SCALE_FACTOR, int streams ) {
        DBInfo dbInfo = DBInfo.newBuilder().setDbHost( dbHost ).setDbPort( port ).setDatabase( database ).setUsername( username ).setPassword( password ).setSystem( system ).setAccessMethod( accessMethod ).build();
        MUSQLEWorkerMessage workerMessage = MUSQLEWorkerMessage.newBuilder().setSCALEFACTOR( SCALE_FACTOR ).setStreams( streams ).build();
        return LaunchWorkerMessage.newBuilder().setDbInfo( dbInfo ).setScenario( Scenario.SCENARIOMUSQLE ).setMusqleWorkerMessage( workerMessage ).build();
    }


    /**
     * @param url where netdata is running. We expect netdata on the default port.
     * @param options List of things you want Netdata to Measure
     */
    public static MonitorWorkerMessage createMonitorWorkerMessage( String url,
            List<String> options ) {
        return MonitorWorkerMessage.newBuilder().setUrl( url ).addAllOptions( options ).build();
    }


    public static LaunchWorkerMessage createYCSBWorkerMessage( String host, int port, String database, String username, String password, DBMSSystem system,
            AccessMethod accessMethod, Map<String, String> properties ) {
        DBInfo dbInfo = DBInfo.newBuilder().setDbHost( host ).setDbPort( port ).setDatabase( database ).setUsername( username ).setPassword( password ).setSystem( system ).setAccessMethod( accessMethod ).build();

        YCSBWorkerMessage workerMessage = YCSBWorkerMessage.newBuilder().putAllProperties( properties ).build();
        return LaunchWorkerMessage.newBuilder().setDbInfo( dbInfo ).setScenario( Scenario.SCENARIOYCSB ).setYcsbWorkerMessage( workerMessage ).build();
    }


    /**
     * @param lower inclusive lower bound
     * @param upper exclusive upper bound
     */
    public static Pair Pair( int lower, int upper ) {
        return Pair.newBuilder().setLower( lower ).setUpper( upper ).build();
    }


    /**
     * @param start lower bound for result inclusion
     * @param stop upper bound for result inclusion
     */
    public static FetchResultsMessage createFetchTPCCMessage( long start, long stop ) {
        return FetchResultsMessage.newBuilder().setStartTime( start ).setStopTime( stop ).setScenario( Scenario.SCENARIOTPCC ).setFetchTpccMessage( FetchTPCCResultsMessage.newBuilder().build() ).build();
    }


    public static FetchResultsMessage createFetchTPCHMessage( long start, long stop ) {
        return FetchResultsMessage.newBuilder().setStartTime( start ).setStopTime( stop ).setScenario( Scenario.SCENARIOTPCH ).setFetchTpchMessage( FetchTPCHResultsMessage.newBuilder().build() ).build();
    }


    public static FetchResultsMessage createFetchMusqleMessage( long start, long stop ) {
        return FetchResultsMessage.newBuilder().setStartTime( start ).setStopTime( stop ).setScenario( Scenario.SCENARIOMUSQLE ).setFetchMusqleMessage( FetchMUSQLEResultsMessage.newBuilder().build() ).build();
    }


    public static FetchResultsMessage FetchYCSBResultsMessage( long start, long stop ) {
        return FetchResultsMessage.newBuilder().setStartTime( start ).setStopTime( stop ).setScenario( Scenario.SCENARIOYCSB ).setFetchYcsbMessage( FetchYCSBResultsMessage.newBuilder().build() ).build();
    }


    public static FetchMonitorWorkerMessage FetchMonitorWorkerMessage( String host, long start, long stop ) {
        return FetchMonitorWorkerMessage.newBuilder().setUrl( host ).setStartTime( start ).setStopTime( stop ).build();
    }


    public static StopWorkerMessage StopWorkerMessage( Scenario scenario ) {
        return StopWorkerMessage.newBuilder().setScenario( scenario ).build();
    }


    public static AckMessage okACK() {
        return AckMessage.newBuilder().setCode( Code.OK ).build();
    }


    public static ProgressMessage ProgressMessage( boolean isFinished, int executedQueries ) {
        return ProgressMessage.newBuilder().setIsFinished( isFinished ).setExecutedQueries( executedQueries ).build();
    }
}
