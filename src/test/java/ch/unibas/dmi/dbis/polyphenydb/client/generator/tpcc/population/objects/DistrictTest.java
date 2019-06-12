package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.objects;


import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.checkStringLength;
import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.generateWarehouse;
import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.verifyZip;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCPopulationGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.District;
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
public class DistrictTest {

    private static final Logger logger = LogManager.getLogger();


    @Test
    public void generateDistrictsForWarehouse() throws Exception {
        Warehouse warehouse = generateWarehouse();
        District[] districts = TPCCPopulationGenerator.generateDistrictsForWarehouse( warehouse );
        HashSet<Integer> ids = new HashSet<>();
        for ( District district : districts ) {
            ids.add( district.getD_ID() );
            verifyDistrict( district, warehouse );
        }
        if ( ids.size() != 10 ) {
            logger.error( "{} unique ids were generated", ids.size() );
            fail();
        }
    }


    /**
     * Verifies if a given {@link District} is valid and belongs to the given {@link Warehouse}
     */
    private void verifyDistrict( District district, Warehouse warehouse ) {
        if ( district.getD_W_ID() != warehouse.getW_ID() ) {
            logger.error( "D_W_ID was {}", district.getD_W_ID() );
            fail();
        }
        checkStringLength( district.getD_NAME(), 6, 10 );
        checkStringLength( district.getD_STREET_1(), 10, 20 );
        checkStringLength( district.getD_STREET_2(), 10, 20 );
        checkStringLength( district.getD_CITY(), 10, 20 );
        checkStringLength( district.getD_STATE(), 2, 2 );
        verifyZip( district.getD_ZIP() );
        if ( district.getD_TAX() < 0 || district.getD_TAX() > 0.2 ) {
            logger.error( "D_TAX was {}", district.getD_TAX() );
            fail();
        }
        if ( district.getD_YTD() != 30000 ) {
            logger.error( "D_YTD was {}", district.getD_YTD() );
            fail();
        }
        if ( district.getD_NEXT_O_ID() != 3001 ) {
            logger.error( "D_NEXT_O_ID was {}", district.getD_NEXT_O_ID() );
            fail();
        }
    }
}
