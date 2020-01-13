package org.polypheny.client.scenarios;


import java.util.Properties;


/**
 * Abstraction level for all Scenarios. This is very high-level given that we want to feature an extensible architecture.
 *
 * @author silvan on 18.03.17.
 */
public interface Scenario {

    /**
     * This method gets called once at the start of your scenario. You are expected to have received all input parameters before execute() is called.
     *
     * @return a {@link Properties} object which has relevant information about the results of your benchmark.
     */
    Properties execute();

    /**
     * Gets called when the benchmark should be aborted. You are expected to cancel benchmark execution on all your workers.
     */
    void abort();
}
