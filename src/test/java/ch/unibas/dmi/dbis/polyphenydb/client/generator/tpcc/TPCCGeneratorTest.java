package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc;


import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.client.generator.RandomGenerator;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


/**
 * Tests the TPC-C Generator
 *
 * Created by silvan on 11.04.17.
 */
public class TPCCGeneratorTest {

    private final Logger logger = LogManager.getLogger();


    @Test
    public void generateCTwice() {
        try {
            TPCCGenerator.generateC();
            TPCCGenerator.generateC();
            fail();
        } catch ( Throwable ignored ) {
            //Desired behavior
        }
    }


    @Test
    public void testCGeneration() {
        for ( int i = 0; i < 1000; i++ ) {
            //Ensure C_DELTA is calculated correctly
            if ( Math.abs( TPCCGenerator.getcLoad() - TPCCGenerator.getcRun() ) != TPCCGenerator
                    .getcDelta() ) {
                logger.error( Math.abs( TPCCGenerator.getcLoad() - TPCCGenerator.getcRun() ) );
                dumpTPCCGeneratorState();
                fail();
            }

            //Ensure Validity of C_Delta
            if ( TPCCGenerator.getcDelta() == 96 || TPCCGenerator.getcDelta() == 112
                    || TPCCGenerator.getcDelta() < 65 || TPCCGenerator.getcDelta() > 119 ) {
                dumpTPCCGeneratorState();
                fail();
            }
        }
    }


    @Test
    public void NURandTest() {
        Random rand = new Random();
        for ( int i = 0; i < 1000; i++ ) {
            int one = (int) Math.abs( rand.nextDouble() * 10000 );
            int two = (int) Math.abs( rand.nextDouble() * 10000 );
            int lb = Math.min( one, two );
            int ub = Math.min( one, two );
            int A = RandomGenerator.generateUniform( lb, ub );
            int C = RandomGenerator.generateUniform( 0, A );
            int generatedNURand = TPCCGenerator.NURand( A, lb, ub, C );
            if ( generatedNURand < lb || generatedNURand > ub ) {
                logger.error( "For lb {}, ub {}, A {}, C{}, the generated number was {}", lb, ub, A, C,
                        generatedNURand );
                fail();
            }
        }
    }


    private void dumpTPCCGeneratorState() {
        logger.info( "C_DELTA is {} with C_RUN {} and C_LOAD {}", TPCCGenerator.getcDelta(),
                TPCCGenerator.getcRun(), TPCCGenerator.getcLoad() );
    }

}
