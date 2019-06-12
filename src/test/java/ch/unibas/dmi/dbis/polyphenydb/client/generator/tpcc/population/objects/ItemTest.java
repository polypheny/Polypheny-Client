package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.objects;


import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.checkStringLength;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCPopulationGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Item;
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
public class ItemTest {

    private static final Logger logger = LogManager.getLogger();


    @Test
    public void generateItems() {
        int containsOriginal = 0;
        HashSet<Integer> IDs = new HashSet<>();
        Item[] items = TPCCPopulationGenerator.generateItems();
        for ( Item item : items ) {
            IDs.add( item.getI_ID() );
            verifyItem( item );
            if ( item.getI_DATA().contains( "ORIGINAL" ) ) {
                containsOriginal++;
            }
        }
        if ( IDs.size() != 100000 ) {
            logger.error( "{} unique IDs were generated", IDs.size() );
            fail();
        }
        if ( containsOriginal == items.length / 10 ) {
            logger.warn(
                    "ORIGINAL was in precisely 10% of the generated strings. It was in {} of {} strings",
                    containsOriginal, items.length );
        }
    /*
      According to the TPC-C Specifications at the bottom of page 68 / 4.3.3.1, 5% Variation of target cardinality is allowed.
     */
        if ( Math.abs( containsOriginal - items.length / 10 ) > items.length / 100 * 5 ) {
            logger
                    .error( "original was in {} strings, item size was {}", containsOriginal, items.length );
            fail();
        }
    }


    /**
     * Verifies a given {@link Item} according to clause 4.3.3.1
     */
    private void verifyItem( Item item ) {
        if ( item.getI_IM_ID() < 1 || item.getI_IM_ID() > 10000 ) {
            logger.error( "generated I_IM_ID was {}", item.getI_IM_ID() );
            fail();
        }
        checkStringLength( item.getI_NAME(), 14, 24 );
        if ( item.getI_PRICE() < 1 || item.getI_PRICE() > 100 ) {
            logger.error( "generated I_PRICE was {}", item.getI_PRICE() );
            fail();
        }
        checkStringLength( item.getI_DATA(), 26, 50 );
    }
}
