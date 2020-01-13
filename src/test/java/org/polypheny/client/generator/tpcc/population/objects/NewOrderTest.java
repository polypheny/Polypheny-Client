package org.polypheny.client.generator.tpcc.population.objects;


import static junit.framework.TestCase.fail;

import java.util.HashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.polypheny.client.generator.tpcc.TPCCPopulationGenerator;
import org.polypheny.client.generator.tpcc.objects.District;
import org.polypheny.client.generator.tpcc.objects.NewOrder;
import org.polypheny.client.generator.tpcc.population.TPCCPopulationGeneratorTest;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 *
 * Created by silvan on 23.04.17.
 */
public class NewOrderTest {

    private static final Logger logger = LogManager.getLogger();


    @Test
    public void generateNewOrdersForDistrictTest() {
        District district = TPCCPopulationGeneratorTest.generateDistrict();
        NewOrder[] newOrders = TPCCPopulationGenerator
                .generateNewOrdersForDistrict( district );
        HashSet<Integer> ids = new HashSet<>();
        for ( NewOrder newOrder : newOrders ) {
            ids.add( newOrder.getNO_O_ID() );
            verifyNewOrder( newOrder, district );
        }
        if ( ids.size() != 900 ) {
            logger.error( "{} unique ids were generated", ids.size() );
            fail();
        }
    }


    @Test
    public void generateNewOrderTest() {
        int NO_O_ID = 1;
        int NO_D_ID = 1;
        int NO_W_ID = 1;
        NewOrder newOrder = TPCCPopulationGenerator.generateNewOrder( NO_O_ID, NO_D_ID, NO_W_ID );
        if ( newOrder.getNO_O_ID() != NO_O_ID ) {
            fail();
        }
        if ( newOrder.getNO_D_ID() != NO_D_ID ) {
            fail();
        }
        if ( newOrder.getNO_W_ID() != NO_W_ID ) {
            fail();
        }
    }


    /**
     * Checks if the given {@link NewOrder} is valid for the associated {@link District}.
     *
     * @param newOrder {@link NewOrder} to be verified
     * @param district {@link District} to be verified.
     */
    private void verifyNewOrder( NewOrder newOrder, District district ) {
        if ( newOrder.getNO_O_ID() < 2101 || newOrder.getNO_O_ID() > 3000 ) {
            fail();
        }
        if ( newOrder.getNO_D_ID() != district.getD_ID() ) {
            fail();
        }
        if ( newOrder.getNO_W_ID() != district.getD_W_ID() ) {
            fail();
        }
    }
}
