package org.polypheny.client.generator.tpcc.population.objects;


import static org.junit.Assert.fail;
import static org.polypheny.client.generator.tpcc.population.TPCCPopulationGeneratorTest.generateDistrict;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.polypheny.client.generator.tpcc.TPCCPopulationGenerator;
import org.polypheny.client.generator.tpcc.objects.District;
import org.polypheny.client.generator.tpcc.objects.Order;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 *
 * Created by silvan on 23.04.17.
 */
public class OrderTest {

    private static final Logger logger = LogManager.getLogger();


    @Test
    public void generateOrdersForDistrict() {
        Timestamp start = Timestamp.from( Instant.now() );
        District district = generateDistrict();
        Order[] orders = TPCCPopulationGenerator.generateOrdersForDistrict( district );
        Timestamp stop = Timestamp.from( Instant.now() );
        HashSet<Integer> orderids = new HashSet<>();
        HashSet<Integer> customerids = new HashSet<>();
        for ( Order order : orders ) {
            orderids.add( order.getO_ID() );
            customerids.add( order.getO_C_ID() );
            if ( order.getO_D_ID() != district.getD_ID() ) {
                fail();
            }
            if ( order.getO_W_ID() != district.getD_W_ID() ) {
                fail();
            }
            if ( order.getO_ENTRY_D().before( start ) || order.getO_ENTRY_D().after( stop ) ) {
                fail();
            }
            if ( order.getO_ID() >= 2101 && order.getO_CARRIER_ID() != null ) {
                logger.error( "O_CARRIER_ID WAS {} for O_ID {}", order.getO_CARRIER_ID(), order.getO_ID() );
                fail();
            }
            if ( order.getO_ID() < 2101 && (order.getO_CARRIER_ID() == null || order.getO_CARRIER_ID() < 1
                    || order.getO_CARRIER_ID() > 10) ) {
                fail();
            }
            if ( order.getO_OL_CNT() < 5 || order.getO_OL_CNT() > 15 ) {
                logger.error( "O_OL_CNT was {}", order.getO_OL_CNT() );
                fail();
            }
            if ( order.getO_ALL_LOCAL() != 1 ) {
                fail();
            }
        }
        if ( orderids.size() != 3000 ) {
            logger.error( "There were {} unique orderids", orderids.size() );
            fail();
        }
        if ( customerids.size() != 3000 ) {
            logger.error( "there were {} unique customerids", customerids.size() );
            fail();
        }
    }

}
