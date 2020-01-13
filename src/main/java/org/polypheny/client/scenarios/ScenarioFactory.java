package org.polypheny.client.scenarios;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.chronos.ChronosExecutor;
import org.polypheny.client.grpc.PolyClientGRPC;
import org.polypheny.client.job.PolyphenyJob;
import org.polypheny.client.job.PolyphenyJobCdl;
import org.polypheny.client.job.evaluation.PolyphenyCdlOptions;
import org.polypheny.client.scenarios.musqle.MusqleScenario;
import org.polypheny.client.scenarios.tpcc.TPCCScenario;
import org.polypheny.client.scenarios.tpch.TPCHScenario;
import org.polypheny.client.scenarios.ycsb.YCSBScenario;


/**
 * Generates a Scenario from a chronos-xml or a {@link PolyphenyJob}
 *
 * @author silvan on 18.03.17.
 */
public class ScenarioFactory {

    private static Logger logger = LogManager.getLogger();


    /**
     * Simply uses the PolyphenyJob from the executor to create a scenario.
     *
     * @param chronosExecutor The Executor for this Chronos-Job
     * @return created Scenario which has been given the relevant Parameters
     */
    public static Scenario createScenario( ChronosExecutor chronosExecutor ) {
        PolyphenyJob polyphenyJob = new PolyphenyJob( chronosExecutor );
        logger.debug( "Creating scenario from chronos" );
        logger.debug( polyphenyJob.getCdl().toString() );
        return createScenario( polyphenyJob );
    }


    /**
     * Creates a {@link Scenario} from the {@link PolyphenyCdlOptions#scenario} stored in the job.
     */
    @SuppressWarnings("JavadocReference")
    private static Scenario createScenario( PolyphenyJob polyphenyJob ) {
        PolyClientGRPC.Scenario scenario = polyphenyJob.getCdl().getEvaluation().getOptions().getScenario();
        switch ( scenario ) {
            case SCENARIOTPCC:
                return new TPCCScenario( polyphenyJob.getCdl(), polyphenyJob.getInputDir(), polyphenyJob.getOutputDir(), polyphenyJob.getExecutor().getProgressListener() );
            case SCENARIOTPCH:
                return new TPCHScenario( polyphenyJob.getCdl(), polyphenyJob.getInputDir(), polyphenyJob.getOutputDir(), polyphenyJob.getExecutor().getProgressListener() );
            case SCENARIOYCSB:
                return new YCSBScenario( polyphenyJob.getCdl(), polyphenyJob.getInputDir(), polyphenyJob.getOutputDir() );
            case SCENARIOMUSQLE:
                return new MusqleScenario( polyphenyJob.getCdl(), polyphenyJob.getInputDir(), polyphenyJob.getOutputDir(), polyphenyJob.getExecutor().getProgressListener() );
            default:
                throw new IllegalArgumentException( "Scenario : " + scenario + " not supported" );
        }
    }


    /**
     * Creates a {@link Scenario} from the scenario-flag stored in the Cdl.
     */
    public static Scenario createScenario( PolyphenyJobCdl polyphenyJobCdl ) {
        PolyClientGRPC.Scenario scenario = polyphenyJobCdl.getEvaluation().getOptions().getScenario();
        switch ( scenario ) {
            case SCENARIOTPCC:
                return new TPCCScenario( polyphenyJobCdl );
            case SCENARIOTPCH:
                return new TPCHScenario( polyphenyJobCdl );
            case SCENARIOYCSB:
                return new YCSBScenario( polyphenyJobCdl );
            default:
                throw new IllegalArgumentException( "Scenario : " + scenario + " not supported" );
        }
    }
}
