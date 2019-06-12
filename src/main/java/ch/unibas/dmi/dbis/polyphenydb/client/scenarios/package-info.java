/**
 * This package contains the core-logic of the 'scenarios' or 'benchmarks' which are the heart of our client. New {@link ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Scenario} should create their own package and add themselves to the protobuf-file and add a generator to the {@link
 * ch.unibas.dmi.dbis.polyphenydb.client.scenarios.ScenarioFactory}. For a Reference Implementation see {@link ch.unibas.dmi.dbis.polyphenydb.client.scenarios.tpcc.TPCCScenario}. The master should implement the {@link ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Scenario} interface, while the
 * worker should implement the {@link ch.unibas.dmi.dbis.polyphenydb.client.scenarios.Worker} interface.
 *
 * @author Silvan Heller
 */

package ch.unibas.dmi.dbis.polyphenydb.client.scenarios;