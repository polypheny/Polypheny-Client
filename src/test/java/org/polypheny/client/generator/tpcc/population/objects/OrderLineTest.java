package org.polypheny.client.generator.tpcc.population.objects;


import static org.junit.Assert.fail;

import java.util.HashSet;
import org.junit.Test;
import org.polypheny.client.generator.tpcc.TPCCPopulationGenerator;
import org.polypheny.client.generator.tpcc.objects.Order;
import org.polypheny.client.generator.tpcc.objects.OrderLine;
import org.polypheny.client.generator.tpcc.population.TPCCPopulationGeneratorTest;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 *
 * Created by silvan on 24.04.17.
 */
public class OrderLineTest {

    @Test
    public void generateOrderLineForOrder() {
        Order order = TPCCPopulationGeneratorTest.generateOrder();
        OrderLine[] orderLines = TPCCPopulationGenerator.generateOrderLineForOrder( order );
        if ( orderLines.length != order.getO_OL_CNT() ) {
            fail();
        }
        HashSet<Integer> ids = new HashSet<>();
        for ( OrderLine orderLine : orderLines ) {
            ids.add( orderLine.getOL_NUMBER() );
            verifyOrderLine( order, orderLine, orderLines.length );
        }
        if ( ids.size() != orderLines.length ) {
            fail();
        }
    }


    /**
     * Verifies a given {@link OrderLine}.
     *
     * @param order {@link Order} the {@link OrderLine} should belong to
     * @param orderLine {@link OrderLine} to be verified.
     * @param MAX_OL_NUMBER Maximum allowed {@link OrderLine#getOL_NUMBER()}
     */
    private void verifyOrderLine( Order order, OrderLine orderLine, int MAX_OL_NUMBER ) {
        if ( orderLine.getOL_O_ID() != order.getO_ID() ) {
            fail();
        }
        if ( orderLine.getOL_D_ID() != order.getO_D_ID() ) {
            fail();
        }
        if ( orderLine.getOL_W_ID() != order.getO_W_ID() ) {
            fail();
        }
        if ( orderLine.getOL_I_ID() < 1 || orderLine.getOL_I_ID() > 100000 ) {
            fail();
        }
        if ( orderLine.getOL_NUMBER() > MAX_OL_NUMBER ) {
            fail();
        }
        if ( orderLine.getOL_SUPPLY_W_ID() != order.getO_W_ID() ) {
            fail();
        }
        if ( orderLine.getOL_O_ID() < 2101 ) {
            if ( !orderLine.getOL_DELIVERY_D().equals( order.getO_ENTRY_D() ) ) {
                fail();
            }
        }
        if ( orderLine.getOL_O_ID() >= 2101 ) {
            if ( orderLine.getOL_DELIVERY_D() != null ) {
                fail();
            }
        }
        if ( orderLine.getOL_QUANTITY() != 5 ) {
            fail();
        }
        if ( orderLine.getOL_O_ID() < 2101 ) {
            if ( orderLine.getOL_AMOUNT() != 0 ) {
                fail();
            }
        }
        if ( orderLine.getOL_O_ID() >= 2101 ) {
            if ( orderLine.getOL_AMOUNT() < 0.01 || orderLine.getOL_AMOUNT() < 9999.99 ) {
                fail();
            }
        }
        TPCCPopulationGeneratorTest.checkStringLength( orderLine.getOL_DIST_INFO(), 24, 24 );
    }
}
