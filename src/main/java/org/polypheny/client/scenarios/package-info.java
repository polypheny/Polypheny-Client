/**
 * This package contains the core-logic of the 'scenarios' or 'benchmarks' which are the heart of our client. New {@link org.polypheny.client.scenarios.Scenario} should create their own package and add themselves to the protobuf-file and add a generator to the {@link
 * org.polypheny.client.scenarios.ScenarioFactory}. For a Reference Implementation see {@link org.polypheny.client.scenarios.tpcc.TPCCScenario}. The master should implement the {@link org.polypheny.client.scenarios.Scenario} interface, while the
 * worker should implement the {@link org.polypheny.client.scenarios.Worker} interface.
 *
 * @author Silvan Heller
 */

package org.polypheny.client.scenarios;