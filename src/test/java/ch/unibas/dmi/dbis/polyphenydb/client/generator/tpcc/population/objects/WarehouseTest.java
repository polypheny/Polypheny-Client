package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.objects;


import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.checkStringLength;
import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.verifyZip;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCPopulationGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Warehouse;
import java.util.HashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 *
 * Created by silvan on 23.04.17.
 */
public class WarehouseTest {

    private static final Logger logger = LogManager.getLogger();


    @Test
    public void generateWarehouses() {
        int NUMBER_OF_CONFIGURED_WAREHOUSES = 10_000;
        HashSet<Integer> ids = new HashSet<>();
        Warehouse[] warehouses = TPCCPopulationGenerator
                .generateWarehouses( NUMBER_OF_CONFIGURED_WAREHOUSES );
        for ( Warehouse warehouse : warehouses ) {
            ids.add( warehouse.getW_ID() );
            verifyWarehouse( warehouse );
        }
        if ( ids.size() != NUMBER_OF_CONFIGURED_WAREHOUSES ) {
            logger.error( "{} unique ids were generated instead of {}", ids.size(),
                    NUMBER_OF_CONFIGURED_WAREHOUSES );
            fail();
        }
    }


    /**
     * Verifies a given {@link Warehouse} according to 4.3.3.1
     */
    private void verifyWarehouse( Warehouse warehouse ) {
        checkStringLength( warehouse.getW_NAME(), 6, 10 );
        checkStringLength( warehouse.getW_STREET_1(), 10, 20 );
        checkStringLength( warehouse.getW_STREET_2(), 10, 20 );
        checkStringLength( warehouse.getW_CITY(), 10, 20 );
        checkStringLength( warehouse.getW_STATE(), 2, 2 );
        verifyZip( warehouse.getW_ZIP() );
        if ( warehouse.getW_TAX() < 0 || warehouse.getW_TAX() > 0.20001 ) {//Lulululu rounding errors jesus christ
            logger.error( warehouse.getW_TAX() - 0.2 );
            logger.error( "Generated W_TAX was {}", warehouse.getW_TAX() );
            fail();
        }
        if ( warehouse.getW_YTD() != 300000 ) {
            logger.error( "Generated W_YTD was {}", warehouse.getW_YTD() );
            fail();
        }
    }

}
