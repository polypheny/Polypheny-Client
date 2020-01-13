/**
 * This package is responsible for our internal abstraction of a {@link ch.unibas.dmi.dbis.chronos.agent.ChronosJob}. It contains all relevant benchmark-parameters. Benchmarks, scenarios and everyone should get their params, options and information from the {@link
 * org.polypheny.client.job.PolyphenyJobCdl}. If you are on the worker-side, do not access anything in this package. You should receive your stuff from the Master via GRPC.
 *
 * @author Silvan Heller
 */

package org.polypheny.client.job;