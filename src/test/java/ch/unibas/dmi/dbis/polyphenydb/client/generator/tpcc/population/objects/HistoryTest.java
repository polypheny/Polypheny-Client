package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.objects;


import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.checkStringLength;
import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.generateCustomer;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCPopulationGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Customer;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.History;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 *
 * Created by silvan on 23.04.17.
 */
public class HistoryTest {

    private static final Logger logger = LogManager.getLogger();


    @Test
    public void generateHistoryForCustomer() {
        Timestamp start = Timestamp.from( Instant.now() );
        Customer customer = generateCustomer();
        History[] histories = TPCCPopulationGenerator.generateHistoryForCustomer( customer );
        Timestamp stop = Timestamp.from( Instant.now() );
        for ( History history : histories ) {
            if ( history.getH_C_ID() != customer.getC_ID() ) {
                fail();
            }
            if ( history.getH_C_D_ID() != customer.getC_D_ID() ) {
                fail();
            }
            if ( history.getH_C_W_ID() != customer.getC_W_ID() ) {
                fail();
            }
            if ( history.getH_DATE().before( start ) || history.getH_DATE().after( stop ) ) {
                fail();
            }
            if ( history.getH_AMOUNT() != 10 ) {
                fail();
            }
            checkStringLength( history.getH_DATA(), 12, 24 );
        }
    }
}
